package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/consistent"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

var (
	monServer *grpc.Server
)

type monitorRpcServer struct {
}

func (s *monitorRpcServer) GetLayout(ctx context.Context, in *protos.LayoutRequest) (*protos.LayoutReply, error) {
	return &protos.LayoutReply{}, nil
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
	newOsdMap := clus.osdMap
	newOsdMap.Epoch++
	newOsdMap.MemberList = make(map[int32]*protos.Osd)
	for k, v := range clus.osdMap.MemberList {
		newOsdMap.MemberList[k] = v
	}
	newOsdMap.MemberList[req.Osd.Id] = req.Osd
	err := ProposeOsdMap(&newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "failed add osd:", req.Osd, err)
		return &protos.OsdConfigReply{}, err
	}
	err = UpdatePgmaps()
	return &protos.OsdConfigReply{}, err
}

func HandleOsdDel(req *protos.OsdConfigRequest) (*protos.OsdConfigReply, error) {
	clus.mapLock.Lock()
	defer clus.mapLock.Unlock()
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return &protos.OsdConfigReply{}, errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}

	newOsdMap := clus.osdMap
	newOsdMap.Epoch++
	newOsdMap.MemberList = make(map[int32]*protos.Osd)
	for k, v := range clus.osdMap.MemberList {
		newOsdMap.MemberList[k] = v
	}
	delete(newOsdMap.MemberList, req.Osd.Id)
	err := ProposeOsdMap(&newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "failed delete osd:", req.Osd, err)
	}
	err = UpdatePgmaps()
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
	newPoolMap.Epoch++
	newPoolMap.Pools = make(map[int32]*protos.Pool)
	for k, v := range clus.poolMap.Pools {
		newPoolMap.Pools[k] = v
	}
	newId := maxIndex + 1
	newPoolMap.Pools[newId] = &protos.Pool{newId, req.Name, req.Size_, req.PgNumbers, req.Policy}
	err := ProposePoolMap(&newPoolMap)
	if err != nil {
		helper.Logger.Print(5, "propose pool map failed", err)
		return &protos.PoolConfigReply{}, err
	}
	//seams to be a problem here, what if pool map uodated but allocate pg failed?
	err = AllocatePgsTomap(newId, req.PgNumbers)
	if err != nil {
		helper.Logger.Print(5, "allocate new pgs failed", err)
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
	newPoolMap.Epoch++
	newPoolMap.Pools = make(map[int32]*protos.Pool)
	for k, v := range clus.poolMap.Pools {
		newPoolMap.Pools[k] = v
	}
	delete(newPoolMap.Pools, key)
	err := ProposePoolMap(&newPoolMap)
	if err != nil {
		helper.Logger.Print(5, "propose pool map failed", err)
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
	return &protos.PgConfigReply{0, pgmaps.Pgmaps[poolId]}, nil
}

func copyPg(src *protos.Pg) protos.Pg {
	newPg := *src
	copy(newPg.OsdIds, src.OsdIds)
	return newPg
}

func copyPgMap(src *protos.PgMap) protos.PgMap {
	newPgmap := *src
	newPgmap.Pgmap = make(map[int32]*protos.Pg)
	for k, v := range src.Pgmap {
		newPg := copyPg(v)
		newPgmap.Pgmap[k] = &newPg
	}
	return newPgmap
}

func UpdatePgmaps() error {
	var err error
	newPgMaps := clus.pgMaps
	newPgMaps.Pgmaps = make(map[int32]*protos.PgMap)
	for k, v := range clus.pgMaps.Pgmaps {
		newPgmap := copyPgMap(v)
		newPgMaps.Pgmaps[k] = &newPgmap
		hashRing := consistent.New(&clus.osdMap, clus.poolMap.Pools[k].Policy)
		err = UpdatePgMap(newPgMaps.Pgmaps[k], hashRing)
		if err != nil {
			helper.Logger.Print(5, "update pg map failed ", err)
			return err
		}
	}
	err = ProposePgMaps(&newPgMaps)
	if err != nil {
		helper.Logger.Print(5, "propose pg maps failed ", err)
	}
	return err
}

func AllocatePgsTomap(poolId int32, n int32) error {
	newPgMaps := clus.pgMaps
	newPgMaps.Pgmaps = make(map[int32]*protos.PgMap)
	for k, v := range clus.pgMaps.Pgmaps {
		newPgmap := copyPgMap(v)
		newPgMaps.Pgmaps[k] = &newPgmap
	}
	if _, ok := newPgMaps.Pgmaps[poolId]; !ok {
		helper.Logger.Println(5, "here1")
		newPgMaps.Pgmaps[poolId] = &protos.PgMap{}
		helper.Logger.Println(5, "here2", newPgMaps.Pgmaps[poolId].Pgmap)
		newPgMaps.Pgmaps[poolId].PoolId = poolId
		newPgMaps.Pgmaps[poolId].Pgmap = make(map[int32]*protos.Pg)
	}
	targetMap := newPgMaps.Pgmaps[poolId]
	helper.Logger.Println(5, "map:", len(targetMap.Pgmap))
	startIndex := len(targetMap.Pgmap)
	for i := 0; i < int(n); i++ {
		id := int32(startIndex + i)
		targetMap.Pgmap[id] = &protos.Pg{id, make([]int32, 0)}
	}
	helper.Logger.Println(5, "get new ring")
	hashRing := consistent.New(&clus.osdMap, clus.poolMap.Pools[poolId].Policy)
	helper.Logger.Println(5, "end new ring")
	err := UpdatePgMap(targetMap, hashRing)
	if err != nil {
		helper.Logger.Print(5, "update pg map failed ", err)
		return err
	}
	helper.Logger.Println(5, "end UpdatePgMap")
	err = ProposePgMaps(&newPgMaps)
	if err != nil {
		helper.Logger.Print(5, "propose pg map failed ", err)
	}
	return err
}

func UpdatePgMap(m *protos.PgMap, ring *consistent.Consistent) error {
	m.Epoch++
	poolId := m.PoolId
	for k, pg := range m.Pgmap {
		helper.Logger.Println(5, "start getn: ", fmt.Sprintf("%d.%d", poolId, pg.Id))
		osds, err := ring.GetN(fmt.Sprintf("%d.%d", poolId, pg.Id), int(clus.poolMap.Pools[poolId].Size_))
		helper.Logger.Println(5, "end getn:", osds)
		if err != nil {
			return err
		}
		//		helper.Logger.Print(5, "osds******************", osds)

		m.Pgmap[k].OsdIds = m.Pgmap[k].OsdIds[:0]
		//		helper.Logger.Print(5, "osdids******************", m.Pgmap[k].OsdIds)
		for _, value := range osds {
			m.Pgmap[k].OsdIds = append(m.Pgmap[k].OsdIds, value.Id)
		}
	}
	return nil
}

func runServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 0))
	if err != nil {
		logger.Fatalf(5, "failed to listen: %v", err)
	}
	helper.Logger.Println(5, "Using monitor rpc port:", lis.Addr().(*net.TCPAddr).Port)
	var opts []grpc.ServerOption
	monServer = grpc.NewServer(opts...)
	protos.RegisterMonitorServer(monServer, newServer())
	go monServer.Serve(lis)
}
