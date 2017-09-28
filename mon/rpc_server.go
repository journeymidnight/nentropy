package main

import (
	"errors"
	"fmt"
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
	switch in.OpType {
	case protos.OsdConfigRequest_ADD:
		err = HandleOsdAdd(in)
	case protos.OsdConfigRequest_DEL:
		err = HandleOsdDel(in)
	case protos.OsdConfigRequest_IN:
		err = HandleOsdIn(in)
	case protos.OsdConfigRequest_OUT:
		err = HandleOsdOut(in)
	case protos.OsdConfigRequest_UP:
		err = HandleOsdUp(in)
	case protos.OsdConfigRequest_DOWN:
		err = HandleOsdDown(in)

	default:
		return nil, errors.New("osd operation type error")

	}
	if err != nil {
		return nil, err
	}
	return &protos.OsdConfigReply{}, nil
}

func (s *monitorRpcServer) PoolConfig(ctx context.Context, in *protos.PoolConfigRequest) (*protos.PoolConfigReply, error) {
	var err error
	switch in.OpType {
	case protos.PoolConfigRequest_ADD:
		err = HandlePoolCreate(in)
	case protos.PoolConfigRequest_DEL:
		err = HandlePoolDelete(in)
	case protos.PoolConfigRequest_EDT:
		err = HandlePoolEdit(in)

	default:
		return nil, errors.New("pool operation type error")

	}
	if err != nil {
		return nil, err
	}
	return &protos.PoolConfigReply{}, nil
}

func newServer() *monitorRpcServer {
	s := new(monitorRpcServer)
	return s
}

func HandleOsdAdd(req *protos.OsdConfigRequest) error {
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; ok {
		return errors.New(fmt.Sprintf("osd %v already existd in osdmap", req.Osd.Id))
	}
	newOsdMap := clus.osdMap
	newOsdMap.Epoch++
	newOsdMap.MemberList[req.Osd.Id] = req.Osd
	err := ProposeDataNodeMap(&newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "failed add osd:", req.Osd, err)
	}
	return err
}

func HandleOsdDel(req *protos.OsdConfigRequest) error {
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}

	newOsdMap := clus.osdMap
	newOsdMap.Epoch++
	delete(newOsdMap.MemberList, req.Osd.Id)
	err := ProposeDataNodeMap(&newOsdMap)
	if err != nil {
		helper.Logger.Print(5, "failed delete osd:", req.Osd, err)
	}
	return err
}

func HandleOsdIn(req *protos.OsdConfigRequest) error {
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}
	return nil
}

func HandleOsdOut(req *protos.OsdConfigRequest) error {
	if _, ok := clus.osdMap.MemberList[req.Osd.Id]; !ok {
		return errors.New(fmt.Sprintf("osd %v not existd in osdmap", req.Osd.Id))
	}
	return nil
}

func HandleOsdUp(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdDown(req *protos.OsdConfigRequest) error {
	return nil
}

func HandlePoolCreate(req *protos.PoolConfigRequest) error {
	// add more parameters check here
	if req.Size_ < 1 || req.Size_ > 3 {
		return errors.New(fmt.Sprintf("pool size range error, should be 1-3"))
	}

	var maxIndex int32 = 0
	for k, pool := range clus.poolMap.Pools {
		if pool.Name == req.Name {
			return errors.New(fmt.Sprintf("pool %v already existd in poolmap", req.Name))
		}
		if k > maxIndex {
			maxIndex = k
		}
	}
	newPoolMap := clus.poolMap
	newPoolMap.Epoch++
	newPoolMap.Pools[maxIndex] = &protos.Pool{maxIndex, req.Name, req.Size_, req.PgNumbers, req.Policy}
	return nil
}

func HandlePoolDelete(req *protos.PoolConfigRequest) error {
	found := false
	key := int32(0)
	for index, pool := range clus.poolMap.Pools {
		if pool.Name == req.Name {
			found = true
			key = index
		}
	}
	if found == false {
		return errors.New(fmt.Sprintf("pool %v not exist", req.Name))
	}
	newPoolMap := clus.poolMap
	newPoolMap.Epoch++
	delete(newPoolMap.Pools, key)
	return nil
}

func HandlePoolEdit(req *protos.PoolConfigRequest) error {
	return nil
}

func UpdatePgMap() (*protos.PgMaps, error) {
//	newPgMaps := protos.PgMaps{}
//
	return nil, nil
}

func runServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 9999))
	if err != nil {
		logger.Fatalf(5, "failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	monServer = grpc.NewServer(opts...)
	protos.RegisterMonitorServer(monServer, newServer())
	go monServer.Serve(lis)
}
