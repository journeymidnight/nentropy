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
	osdServer *grpc.Server
)

type osdRpcServer struct {
}

func (s *osdRpcServer) CreatePg(ctx context.Context, in *protos.CreatePgRequest) (*protos.CreatePgReply, error) {
	return &protos.CreatePgReply{}, nil
}

func (s *osdRpcServer) DeletePg(ctx context.Context, in *protos.DeletePgRequest) (*protos.DeletePgReply, error) {
	return &protos.DeletePgReply{}, nil
}

func newServer() *osdRpcServer {
	s := new(osdRpcServer)
	return s
}

func runServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 0))
	if err != nil {
		logger.Fatalf(5, "failed to listen: %v", err)
	}
	helper.Logger.Println(5, "Using osd rpc port:", lis.Addr().(*net.TCPAddr).Port)
	var opts []grpc.ServerOption
	osdServer = grpc.NewServer(opts...)
	protos.RegisterOsdRpcServer(osdServer, newServer())
	go osdServer.Serve(lis)
}
