package main

import (
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

func (s *monitorRpcServer) OsdOperation(ctx context.Context, in *protos.OsdRequest) (*protos.OsdRequest, error) {
	return &protos.OsdRequest{}, nil
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
