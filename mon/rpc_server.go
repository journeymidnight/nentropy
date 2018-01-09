package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/consistent"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"strconv"
	"strings"
)

var (
	monServer *grpc.Server
)

type monitorRpcServer struct {
}

func (s *monitorRpcServer) GetLayout(ctx context.Context, in *protos.LayoutRequest) (*protos.LayoutReply, error) {
	poolId, err := GetPoolIdByName(in.PoolName)
	if err != nil {
		return &protos.LayoutReply{}, err
	}
	pgNumbers := clus.poolMap.Pools[poolId].PgNumbers
	hashPgId := helper.HashKey(in.ObjectName) % uint32(pgNumbers)
	pgName := fmt.Sprintf("%d.%d", poolId, hashPgId)
	osds := make([]*protos.Osd, 0)
	for _, v := range clus.pgMaps.Pgmaps[poolId].Pgmap[int32(hashPgId)].Replicas {
		helper.Logger.Println(5, "osd to be returned:", *clus.osdMap.MemberList[v.OsdId])
		osds = append(osds, clus.osdMap.MemberList[v.OsdId])
	}
	return &protos.LayoutReply{0, pgName, osds}, nil
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
	case protos.PoolConfigRequest_EDT:
		reply, err = HandlePoolEdit(in)
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
		helper.Logger.Println(5, "Eorror marshal osdmap!")
		return &protos.OsdConfigReply{}, err
	}
	err = newOsdMap.Unmarshal(data)
	if err != nil {
		helper.Logger.Println(5, "Eorror unmarshal osdmap!")
		return &protos.OsdConfigReply{}, err
	}

	newOsdMap.MemberList = make(map[int32]*protos.Osd)
	for k, v := range clus.osdMap.MemberList {
		newOsdMap.MemberList[k] = v
	}
	helper.Logger.Println(5, "osd to be added:", *req.Osd)
	newOsdMap.MemberList[req.Osd.Id] = req.Osd
	trans := protos.Transaction{}
	err = PrepareOsdMap(&trans, &newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "prepare osd map failed", err)
		return &protos.OsdConfigReply{}, err
	}

	newPgmaps, err := updatePgmaps(&clus.poolMap, &clus.pgMaps, &newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "update pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgmaps)
	if err != nil {
		helper.Logger.Print(5, "prepare pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Logger.Print(5, "propose data failed", err)
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
		helper.Logger.Print(5, "prepare osd map failed", err)
		return &protos.OsdConfigReply{}, err
	}
	newPgmaps, err := updatePgmaps(&clus.poolMap, &clus.pgMaps, &newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "update pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgmaps)
	if err != nil {
		helper.Logger.Print(5, "prepare pg maps failed", err)
		return &protos.OsdConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Logger.Print(5, "propose data failed", err)
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
	newPoolMap.Pools[newId] = &protos.Pool{newId, req.Name, req.Size_, req.PgNumbers, req.Policy}
	trans := protos.Transaction{}
	err := PreparePoolMap(&trans, &newPoolMap)
	if err != nil {
		helper.Logger.Print(5, "prepare pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}

	newPgMaps, err := allocateNewPgs(&newPoolMap, &clus.pgMaps, &clus.osdMap, newId, req.PgNumbers)
	if err != nil {
		helper.Logger.Print(5, "prepare allocate new pgs failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = PreparePgMap(&trans, newPgMaps)
	if err != nil {
		helper.Logger.Print(5, "prepare pg maps failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Logger.Print(5, "propose data failed", err)
		return &protos.PoolConfigReply{}, err
	}

	return &protos.PoolConfigReply{}, nil
}

func HandlePoolDelete(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	found := false
	key := int32(0)
	for index, pool := range clus.poolMap.Pools {
		if pool.Name == req.Name {
			found = true
			key = index
		}
	}
	if found == false {
		return &protos.PoolConfigReply{}, errors.New(fmt.Sprintf("pool %v not exist", req.Name))
	}
	newPoolMap := clus.poolMap
	newPoolMap.Pools = make(map[int32]*protos.Pool)
	for k, v := range clus.poolMap.Pools {
		newPoolMap.Pools[k] = v
	}
	delete(newPoolMap.Pools, key)

	newPgMaps := clus.pgMaps
	newPgMaps.Pgmaps = make(map[int32]*protos.PgMap)
	for k, v := range clus.pgMaps.Pgmaps {
		newPgMaps.Pgmaps[k] = v
	}
	delete(newPgMaps.Pgmaps, key)

	trans := protos.Transaction{}
	err := PreparePoolMap(&trans, &newPoolMap)
	if err != nil {
		helper.Logger.Print(5, "prepare pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = PreparePgMap(&trans, &newPgMaps)
	if err != nil {
		helper.Logger.Print(5, "prepare pg maps failed", err)
		return &protos.PoolConfigReply{}, err
	}

	err = proposeData(&trans)
	if err != nil {
		helper.Logger.Print(5, "propose data failed", err)
		return &protos.PoolConfigReply{}, err
	}

	return &protos.PoolConfigReply{}, nil
}

func HandlePoolEdit(req *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
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
	poolId, err := GetPoolIdByName(req.Pool)
	if err != nil {
		return &protos.PgConfigReply{}, err
	}
	leaderMap := make(map[int32]int32)
	for pgName, nodeId := range clus.leaderPgLocationMap {
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
			leaderMap[int32(pg_id)] = nodeId
		}

	}
	return &protos.PgConfigReply{0, pgmaps.Epoch, pgmaps.Pgmaps[poolId], leaderMap}, nil
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
		helper.Logger.Println(5, "Eorror marshal PgMaps!")
		return &newPgMaps, err
	}
	err = newPgMaps.Unmarshal(data)
	if err != nil {
		helper.Logger.Println(5, "Eorror unmarshal PgMaps!")
		return &newPgMaps, err
	}

	for k := range newPgMaps.Pgmaps {
		hashRing := consistent.New(osdMap, poolMap.Pools[k].Policy)
		err = updatePgMap(newPgMaps.Pgmaps[k], poolMap, hashRing)
		if err != nil {
			helper.Logger.Print(5, "update pg map failed ", err)
			return nil, err
		}
	}
	return &newPgMaps, nil
}

func allocateNewPgs(poolMap *protos.PoolMap, pgMaps *protos.PgMaps, osdMap *protos.OsdMap, poolId int32, n int32) (*protos.PgMaps, error) {
	newPgMaps := protos.PgMaps{}
	data, err := pgMaps.Marshal()
	if err != nil {
		helper.Logger.Println(5, "Eorror marshal PgMaps!")
		return &newPgMaps, err
	}
	err = newPgMaps.Unmarshal(data)
	if err != nil {
		helper.Logger.Println(5, "Eorror unmarshal PgMaps!")
		return &newPgMaps, err
	}

	if _, ok := newPgMaps.Pgmaps[poolId]; !ok {
		newPgMaps.Pgmaps[poolId] = &protos.PgMap{}
		newPgMaps.Pgmaps[poolId].PoolId = poolId
		newPgMaps.Pgmaps[poolId].Pgmap = make(map[int32]*protos.Pg)
	}
	targetMap := newPgMaps.Pgmaps[poolId]
	startIndex := len(targetMap.Pgmap)
	for i := 0; i < int(n); i++ {
		id := int32(startIndex + i)
		targetMap.Pgmap[id] = &protos.Pg{id, 0, make([]protos.PgReplica, 0), 1}
	}
	hashRing := consistent.New(osdMap, poolMap.Pools[poolId].Policy)
	err = updatePgMap(targetMap, poolMap, hashRing)
	if err != nil {
		helper.Logger.Print(5, "update pg map failed ", err)
		return nil, err
	}
	newPgMaps.Epoch = newPgMaps.Epoch + 1
	return &newPgMaps, nil
}

func updatePgMap(m *protos.PgMap, poolMap *protos.PoolMap, ring *consistent.Consistent) error {
	poolId := m.PoolId
	for k, pg := range m.Pgmap {
		helper.Logger.Println(5, "start getn: ", fmt.Sprintf("%d.%d", poolId, pg.Id))
		osds, err := ring.GetN(fmt.Sprintf("%d.%d", poolId, pg.Id), int(poolMap.Pools[poolId].Size_))
		helper.Logger.Println(5, "end getn:", osds)
		if err != nil {
			return err
		}
		//		helper.Logger.Print(5, "osds******************", osds)
		oldReplicas := make([]protos.PgReplica, 0)
		copy(oldReplicas, m.Pgmap[k].Replicas)
		m.Pgmap[k].Replicas = m.Pgmap[k].Replicas[:0]
		//		helper.Logger.Print(5, "osdids******************", m.Pgmap[k].OsdIds)
		for _, value := range osds {
			for _, oldReplica := range oldReplicas {
				if value.Id == oldReplica.OsdId {
					m.Pgmap[k].Replicas = append(m.Pgmap[k].Replicas, oldReplica)
					continue
				}
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
	nodeId := in.GetNodeId()
	pgNames := in.GetOwnPrimaryPgs()
	clus.internalMapLock.Lock()
	for _, v := range pgNames {
		clus.leaderPgLocationMap[v] = nodeId
	}
	clus.internalMapLock.Unlock()
	return &protos.OsdStatusReportReply{}, nil
}

func runServer(grpc *grpc.Server) {
	protos.RegisterMonitorServer(grpc, newServer())
}
