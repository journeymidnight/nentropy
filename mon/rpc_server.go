package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/consistent"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/bits"
	"strconv"
	"strings"
)

var (
	monServer *grpc.Server
)

type monitorRpcServer struct {
}

func (s *monitorRpcServer) GetMonMap(ctx context.Context, in *protos.MonConfigRequest) (*protos.MonConfigReply, error) {
	peerAddr := clus.node.peersAddr
	monMap := protos.MonMap{Epoch: 0, MonMap: make(map[uint64]*protos.Mon)}
	leaderID := clus.node.LeaderID()
	res := protos.MonConfigReply{Map: &monMap, LeadId: leaderID}
	for _, v := range clus.node._confState.Nodes {
		if addr, ok := peerAddr[v]; ok {
			res.Map.MonMap[v] = &protos.Mon{Id: v, Addr: addr}
		}
	}
	return &res, nil
}

func (s *monitorRpcServer) GetLayout(ctx context.Context, in *protos.LayoutRequest) (*protos.LayoutReply, error) {
	poolId, err := GetPoolIdByName(in.PoolName)
	if err != nil {
		return &protos.LayoutReply{}, err
	}
	pgNumbers := clus.poolMap.Pools[poolId].PgNumbers
	hash := protos.Nentropy_str_hash(in.ObjectName)
	mask := protos.Calc_pg_masks(int(pgNumbers))
	hashPgId := protos.Nentropy_stable_mod(int(hash), int(pgNumbers), mask)
	pgName := fmt.Sprintf("%d.%d", poolId, hashPgId)
	if v, ok := clus.PgStatusMap[pgName]; ok {
		clus.pgMaps.Pgmaps[poolId].Pgmap[int32(hashPgId)].PrimaryId = v.LeaderNodeId
	} else {
		helper.Println(5, "No primary osd for pg ", pgName)
	}

	osds := make([]*protos.Osd, 0)
	nonPrimayOsds := make([]*protos.Osd, 0)
	for _, v := range clus.pgMaps.Pgmaps[poolId].Pgmap[int32(hashPgId)].Replicas {
		helper.Println(5, "osd to be returned:", *clus.osdMap.MemberList[v.OsdId])
		if clus.pgMaps.Pgmaps[poolId].Pgmap[int32(hashPgId)].PrimaryId == v.OsdId {
			osds = append(osds, clus.osdMap.MemberList[v.OsdId])
		} else {
			nonPrimayOsds = append(nonPrimayOsds, clus.osdMap.MemberList[v.OsdId])
		}
	}
	if len(osds) == 0 {
		helper.Println(5, "No primary id in replicas for pg ", pgName)
	}

	osds = append(osds, nonPrimayOsds...)

	return &protos.LayoutReply{0, pgName, osds}, nil
}

func (s *monitorRpcServer) GetPgStatus(ctx context.Context, in *protos.GetPgStatusRequest) (*protos.GetPgStatusReply, error) {
	status, ok := clus.PgStatusMap[in.PgId]
	if ok {
		return &protos.GetPgStatusReply{&status}, nil
	} else {
		return &protos.GetPgStatusReply{}, errors.New("can not find specified pg")
	}
}

func (s *monitorRpcServer) OsdConfig(ctx context.Context, in *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	var err error
	var reply *protos.OsdConfigReply
	switch in.OpType {
	case protos.OsdConfigRequest_ADD:
		reply, err = HandleOsdAdd(in)
	case protos.OsdConfigRequest_DEL:
		reply, err = HandleOsdDel(in)
	case protos.OsdConfigRequest_IN:
		reply, err = HandleOsdIn(in)
	case protos.OsdConfigRequest_OUT:
		reply, err = HandleOsdOut(in)
	case protos.OsdConfigRequest_UP:
		reply, err = HandleOsdUp(in)
	case protos.OsdConfigRequest_DOWN:
		reply, err = HandleOsdDown(in)
	case protos.OsdConfigRequest_LIST:
		reply, err = HandleOsdList(in)

	default:
		return nil, errors.New("osd operation type error")

	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (s *monitorRpcServer) PoolConfig(ctx context.Context, in *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	var err error
	var reply *protos.PoolConfigReply
	switch in.OpType {
	case protos.PoolConfigRequest_ADD:
		reply, err = HandlePoolCreate(in)
	case protos.PoolConfigRequest_DEL:
		reply, err = HandlePoolDelete(in)
	case protos.PoolConfigRequest_SET_PGS:
		reply, err = HandlePoolSetPgs(in)
	case protos.PoolConfigRequest_SET_SIZE:
		reply, err = HandlePoolSetSize(in)
	case protos.PoolConfigRequest_LIST:
		reply, err = HandlePoolList(in)

	default:
		return nil, errors.New("pool operation type error")

	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (s *monitorRpcServer) PgConfig(ctx context.Context, in *protos.PgConfigRequest) (*protos.PgConfigReply, error) {
	var err error
	var reply *protos.PgConfigReply
	switch in.OpType {
	case protos.PgConfigRequest_LIST:
		reply, err = HandlePgList(in)
	default:
		return nil, errors.New("pg operation type error")
	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func newServer() *monitorRpcServer {
	s := new(monitorRpcServer)
	return s
}

func HandleOsdAdd(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; ok {
		return &protos.OsdConfigReply{}, errors.New(fmt.Sprintf("osd %v already existd in osdmap", req.Osd.Id))
	}

	newOsdMap := protos.OsdMap{}
	data, err := clus.osdMap.Marshal()
	if err != nil {
		helper.Println(5, "Eorror marshal osdmap!")
		return &protos.OsdConfigReply{}, err
	}
	err = newOsdMap.Unmarshal(data)
	if err != nil {
		helper.Println(5, "Eorror unmarshal osdmap!")
		return &protos.OsdConfigReply{}, err
	}

	newOsdMap.MemberList = make(map[int32]*protos.Osd)
	for k, v := range clus.osdMap.MemberList {
		newOsdMap.MemberList[k] = v
	}
	helper.Println(5, "osd to be added:", *req.Osd)
	newOsdMap.MemberList[req.Osd.Id] = req.Osd
	trans := protos.Transaction{}
	err = PrepareOsdMap(&trans, &newOsdMap)
	if err != nil {
		helper.Print(5, "prepare osd map failed", err)
		return &protos.OsdConfigReply{}, err
	}

	newPgmaps, err := updatePgmaps(&clus.poolMap, &clus.pgMaps, &newOsdMap)
	if err != nil {
		helper.Print(5, "update pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgmaps)
	if err != nil {
		helper.Print(5, "prepare pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Print(5, "propose data failed", err)
		return &protos.OsdConfigReply{}, err
	}
	return &protos.OsdConfigReply{}, err
}

func HandleOsdDel(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return &protos.OsdConfigReply{}, errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}

	newOsdMap := clus.osdMap
	newOsdMap.MemberList = make(map[int32]*protos.Osd)
	for k, v := range clus.osdMap.MemberList {
		newOsdMap.MemberList[k] = v
	}
	delete(newOsdMap.MemberList, req.Osd.Id)
	trans := protos.Transaction{}
	err := PrepareOsdMap(&trans, &newOsdMap)
	if err != nil {
		helper.Print(5, "prepare osd map failed", err)
		return &protos.OsdConfigReply{}, err
	}
	newPgmaps, err := updatePgmaps(&clus.poolMap, &clus.pgMaps, &newOsdMap)
	if err != nil {
		helper.Print(5, "update pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgmaps)
	if err != nil {
		helper.Print(5, "prepare pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Print(5, "propose data failed", err)
		return &protos.OsdConfigReply{}, err
	}
	return &protos.OsdConfigReply{}, err
}

func HandleOsdIn(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return &protos.OsdConfigReply{}, errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}
	return &protos.OsdConfigReply{}, nil
}

func HandleOsdOut(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return &protos.OsdConfigReply{}, errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}
	return &protos.OsdConfigReply{}, nil
}

func HandleOsdUp(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	return &protos.OsdConfigReply{}, nil
}

func HandleOsdDown(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	return &protos.OsdConfigReply{}, nil
}

func HandleOsdList(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	osdmap, _ := GetCurrOsdMap()
	return &protos.OsdConfigReply{0, &osdmap}, nil
}

func HandlePoolCreate(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	// add more parameters check here
	if req.Size_ < 1 || req.Size_ > 3 {
		return &protos.PoolConfigReply{}, errors.New(fmt.Sprintf("pool size range error, should be 1-3"))
	}
	var maxIndex int32 = 0
	for k, pool := range clus.poolMap.Pools {
		if pool.Name == req.Name {
			return &protos.PoolConfigReply{}, errors.New(fmt.Sprintf("pool %v already existd in poolmap", req.Name))
		}
		if k > maxIndex {
			maxIndex = k
		}
	}
	newPoolMap := clus.poolMap
	newPoolMap.Pools = make(map[int32]*protos.Pool)
	for k, v := range clus.poolMap.Pools {
		newPoolMap.Pools[k] = v
	}
	newId := maxIndex + 1
	rounded := numberRoundToPowerOfTwo(uint32(req.PgNumbers))
	newPoolMap.Pools[newId] = &protos.Pool{newId, req.Name, req.Size_, int32(rounded), req.Policy}
	newPoolMap.Epoch++
	trans := protos.Transaction{}
	err := PreparePoolMap(&trans, &newPoolMap)
	if err != nil {
		helper.Print(5, "prepare pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}

	newPgMaps, err := allocateNewPgs(&newPoolMap, &clus.pgMaps, &clus.osdMap, newId, int32(rounded))
	if err != nil {
		helper.Print(5, "prepare allocate new pgs failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgMaps)
	if err != nil {
		helper.Print(5, "prepare pg maps failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Print(5, "propose data failed", err)
		return &protos.PoolConfigReply{}, err
	}

	return &protos.PoolConfigReply{}, nil
}

func HandlePoolDelete(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	poolId, err := GetPoolIdByName(req.Name)
	if err != nil {
		return &protos.PoolConfigReply{}, err
	}
	newPoolMap := clus.poolMap
	newPoolMap.Pools = make(map[int32]*protos.Pool)
	for k, v := range clus.poolMap.Pools {
		newPoolMap.Pools[k] = v
	}
	delete(newPoolMap.Pools, poolId)
	newPoolMap.Epoch++

	newPgMaps := clus.pgMaps
	newPgMaps.Pgmaps = make(map[int32]*protos.PgMap)
	for k, v := range clus.pgMaps.Pgmaps {
		newPgMaps.Pgmaps[k] = v
	}
	delete(newPgMaps.Pgmaps, poolId)

	trans := protos.Transaction{}
	err = PreparePoolMap(&trans, &newPoolMap)
	if err != nil {
		helper.Print(5, "prepare pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = PreparePgMap(&trans, &newPgMaps)
	if err != nil {
		helper.Print(5, "prepare pg maps failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Print(5, "propose data failed", err)
		return &protos.PoolConfigReply{}, err
	}

	return &protos.PoolConfigReply{}, nil
}

//1 1
//2 2
//3 4
//4 4
//5 8
func numberRoundToPowerOfTwo(n uint32) int {
	count := bits.Len32(n)
	if bits.OnesCount32(n) == 1 {
		return int(n)
	} else {
		return 1 << uint(count)
	}
}

func HandlePoolSetPgs(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	//clus.mapLock.Lock()
	//defer clus.mapLock.Unlock()
	poolId, err := GetPoolIdByName(req.Name)
	if err != nil {
		return &protos.PoolConfigReply{}, err
	}
	currentPgs := clus.poolMap.Pools[poolId].PgNumbers
	if req.PgNumbers <= currentPgs {
		return &protos.PoolConfigReply{}, errors.New(fmt.Sprintf("specify pg numbers must bigger than current value :%d", currentPgs))
	}
	rounded := numberRoundToPowerOfTwo(uint32(req.PgNumbers))
	helper.Println(5, "got rounded pg nubers:", req.PgNumbers, rounded)
	newPoolMap := protos.PoolMap{}
	data, err := clus.poolMap.Marshal()
	if err != nil {
		helper.Println(5, "Eorror marshal PoolMap!")
		return &protos.PoolConfigReply{}, err
	}
	err = newPoolMap.Unmarshal(data)
	if err != nil {
		helper.Println(5, "Eorror unmarshal PoolMap!")
		return &protos.PoolConfigReply{}, err
	}

	newPoolMap.Pools[poolId].PgNumbers = int32(rounded)
	newPoolMap.Epoch++
	trans := protos.Transaction{}
	err = PreparePoolMap(&trans, &newPoolMap)
	if err != nil {
		helper.Print(5, "prepare pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}

	newPgMaps, err := allocateNewPgs(&newPoolMap, &clus.pgMaps, &clus.osdMap, poolId, int32(rounded))
	if err != nil {
		helper.Print(5, "prepare allocate new pgs failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgMaps)
	if err != nil {
		helper.Print(5, "prepare pg maps failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Print(5, "propose data failed", err)
		return &protos.PoolConfigReply{}, err
	}
	return &protos.PoolConfigReply{}, nil
}

func HandlePoolSetSize(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	return &protos.PoolConfigReply{}, nil
}

func HandlePoolList(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	return &protos.PoolConfigReply{0, &clus.poolMap}, nil
}

func GetPoolIdByName(name string) (int32, error) {
	for k, v := range clus.poolMap.Pools {
		if v.Name == name {
			return k, nil
		}
	}
	return 0, errors.New("specified pool name not exist")
}

func HandlePgList(req *protos.PgConfigRequest) (*protos.PgConfigReply, error) {
	pgmaps, _ := GetCurrPgMaps()
	statusMap := make(map[int32]protos.PgStatus)
	if req.Pool != "" {
		poolId, err := GetPoolIdByName(req.Pool)
		if err != nil {
			return &protos.PgConfigReply{}, err
		}
		for pgName, status := range clus.PgStatusMap {
			res := strings.Split(pgName, ".")
			pool_id, err := strconv.Atoi(res[0])
			if err != nil {
				continue
			}
			pg_id, err := strconv.Atoi(res[1])
			if err != nil {
				continue
			}
			if int32(pool_id) == poolId {
				statusMap[int32(pg_id)] = status
			}
		}
		return &protos.PgConfigReply{0, pgmaps.Epoch, pgmaps.Pgmaps[poolId], nil, statusMap}, nil
	} else {
		return &protos.PgConfigReply{0, pgmaps.Epoch, nil, &pgmaps, nil}, nil
	}
}

//func copyPg(src *protos.Pg) protos.Pg {
//	newPg := *src
//	copy(newPg.OsdIds, src.OsdIds)
//	return newPg
//}
//
//func copyPgMap(src *protos.PgMap) protos.PgMap {
//	newPgmap := *src
//	newPgmap.Pgmap = make(map[int32]*protos.Pg)
//	for k, v := range src.Pgmap {
//		newPg := copyPg(v)
//		newPgmap.Pgmap[k] = &newPg
//	}
//	return newPgmap
//}

func updatePgmaps(poolMap *protos.PoolMap, pgMaps *protos.PgMaps, osdMap *protos.OsdMap) (*protos.PgMaps, error) {
	var err error
	newPgMaps := protos.PgMaps{}
	data, err := pgMaps.Marshal()
	if err != nil {
		helper.Println(5, "Eorror marshal PgMaps!")
		return &newPgMaps, err
	}
	err = newPgMaps.Unmarshal(data)
	if err != nil {
		helper.Println(5, "Eorror unmarshal PgMaps!")
		return &newPgMaps, err
	}

	for k := range newPgMaps.Pgmaps {
		hashRing := consistent.New(osdMap, poolMap.Pools[k].Policy)
		err = updatePgMap(newPgMaps.Pgmaps[k], poolMap, hashRing)
		if err != nil {
			helper.Print(5, "update pg map failed ", err)
			return nil, err
		}
	}
	return &newPgMaps, nil
}

func setParentAndChildren(pgMap *protos.PgMap, currentPgs, newPgs int32) {
	if currentPgs == 0 { //mean these new pgs are created when creating pool, so their parent are themselves, and children are left empty
		for pgId, pg := range pgMap.Pgmap {
			pg.ParentId = pgId
		}
	} else {
		chilerenCnt := newPgs/currentPgs - 1
		for pIndex := int32(0); pIndex < currentPgs; pIndex++ {
			pgMap.Pgmap[pIndex].Clildren = pgMap.Pgmap[pIndex].Clildren[:0] //clear relationship of pgs when last time to enlarge pgs
			for i := int32(1); i <= chilerenCnt; i++ {
				cIndex := pIndex + currentPgs*i
				pgMap.Pgmap[pIndex].Clildren = append(pgMap.Pgmap[pIndex].Clildren, cIndex)
				pgMap.Pgmap[cIndex].ParentId = pIndex
			}
		}
	}
}

func allocateNewPgs(poolMap *protos.PoolMap, pgMaps *protos.PgMaps, osdMap *protos.OsdMap, poolId int32, n int32) (*protos.PgMaps, error) {
	newPgMaps := protos.PgMaps{}
	data, err := pgMaps.Marshal()
	if err != nil {
		helper.Println(5, "Eorror marshal PgMaps!")
		return &newPgMaps, err
	}
	err = newPgMaps.Unmarshal(data)
	if err != nil {
		helper.Println(5, "Eorror unmarshal PgMaps!")
		return &newPgMaps, err
	}

	if newPgMaps.Pgmaps == nil {
		helper.Println(5, "enter allocateNewPgs and make new pgmaps ****************")
		newPgMaps.Pgmaps = make(map[int32]*protos.PgMap)
	}

	helper.Println(5, "debug allocateNewPgs")
	helper.Println(5, "debug allocateNewPgs", newPgMaps.Pgmaps)
	helper.Println(5, "debug allocateNewPgs", poolId)

	if _, ok := newPgMaps.Pgmaps[poolId]; !ok {
		newPgMaps.Pgmaps[poolId] = &protos.PgMap{}
		newPgMaps.Pgmaps[poolId].PoolId = poolId
		newPgMaps.Pgmaps[poolId].Pgmap = make(map[int32]*protos.Pg)
	}
	targetMap := newPgMaps.Pgmaps[poolId]
	startIndex := len(targetMap.Pgmap)
	for i := 0; i < int(n)-startIndex; i++ {
		id := int32(startIndex + i)
		targetMap.Pgmap[id] = &protos.Pg{id, 0, 0, make([]protos.PgReplica, 0), 1, 0, make([]int32, 0)}
	}
	hashRing := consistent.New(osdMap, poolMap.Pools[poolId].Policy)
	err = updatePgMap(targetMap, poolMap, hashRing)
	if err != nil {
		helper.Print(5, "update pg map failed ", err)
		return nil, err
	}
	setParentAndChildren(targetMap, int32(startIndex), n)
	newPgMaps.Epoch = newPgMaps.Epoch + 1
	return &newPgMaps, nil
}

func updatePgMap(m *protos.PgMap, poolMap *protos.PoolMap, ring *consistent.Consistent) error {
	poolId := m.PoolId
	for k, pg := range m.Pgmap {
		helper.Println(5, "start getn: ", fmt.Sprintf("%d.%d", poolId, pg.Id))
		osds, err := ring.GetN(fmt.Sprintf("%d.%d", poolId, pg.Id), int(poolMap.Pools[poolId].Size_))
		helper.Println(5, "end getn:", osds)
		if err != nil {
			return err
		}
		//		helper.Print(5, "osds******************", osds)
		oldReplicas := make([]protos.PgReplica, 0)
		copy(oldReplicas, m.Pgmap[k].Replicas)
		m.Pgmap[k].Replicas = m.Pgmap[k].Replicas[:0]
		m.Pgmap[k].ExpectedPrimaryId = osds[0].Id
		//		helper.Print(5, "osdids******************", m.Pgmap[k].OsdIds)

		for _, value := range osds {
			foundExistOsd := false
			for _, oldReplica := range oldReplicas {
				if value.Id == oldReplica.OsdId {
					m.Pgmap[k].Replicas = append(m.Pgmap[k].Replicas, oldReplica)
					foundExistOsd = true
					break
				}
			}
			if foundExistOsd == true {
				continue
			}
			m.Pgmap[k].Replicas = append(m.Pgmap[k].Replicas, protos.PgReplica{value.Id, m.Pgmap[k].NextReplicaId})
			m.Pgmap[k].NextReplicaId = m.Pgmap[k].NextReplicaId + 1
		}
	}
	return nil
}

func (s *monitorRpcServer) OsdStatusReport(ctx context.Context, in *protos.OsdStatusReportRequest) (*protos.OsdStatusReportReply, error) {
	if clus.isPrimaryMon != true {
		return &protos.OsdStatusReportReply{}, errors.New("not primary monitor, please check!")
	}
	pgsStatuses := in.GetLeaderPgsStatus()
	clus.internalMapLock.Lock()
	for k, v := range pgsStatuses {
		clus.PgStatusMap[k] = v
	}
	clus.internalMapLock.Unlock()
	return &protos.OsdStatusReportReply{}, nil
}

func runServer(grpc *grpc.Server) {
	protos.RegisterMonitorServer(grpc, newServer())
}
