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
	osdMap  map[int][]*protos.Osd
	poolMap map[int][]*protos.Pool
	pgMaps  map[int]map[int][]*protos.Pg
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

	case protos.OsdConfigRequest_IN:

	case protos.OsdConfigRequest_OUT:

	case protos.OsdConfigRequest_UP:

	case protos.OsdConfigRequest_DOWN:

	default:
		return nil, errors.New("osd operation type error")

	}
	if err != nil {
		return nil, err
	}
	return &protos.OsdConfigReply{}, nil
}

func loadData(s *monitorRpcServer) error {
	return nil
}
func newServer() *monitorRpcServer {
	s := new(monitorRpcServer)
	err := loadData(s)
	if err != nil {
		helper.Logger.Panic(5, "load data from raft failed")
	}
	return s
}

func HandleOsdAdd(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdDel(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdIn(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdOut(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdUp(req *protos.OsdConfigRequest) error {
	return nil
}

func HandleOsdDown(req *protos.OsdConfigRequest) error {
	return nil
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
