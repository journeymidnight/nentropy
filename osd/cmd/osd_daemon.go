package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/nentropy/osd"
	pb "github.com/journeymidnight/nentropy/osd/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// osd_daemon runs as a daemon to server client requests

const (
	port = ":50052"
)

func runRpcServer(done <-chan os.Signal) {
	logger := log.New(os.Stdout, "osd::rpcserver: ", log.Ltime)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	osdserver := osd.NewServer()
	pb.RegisterStoreServer(s, osdserver)
	reflection.Register(s)

	syncdone := make(chan struct{})
	go osd.StartSyncThread(syncdone)

	go func() {
		select {
		case <-done:
			logger.Println("server going to stop now")
			syncdone <- struct{}{}
			go s.GracefulStop()
			go lis.Close()
			osdserver.Close()
		}
	}()

	s.Serve(lis)
}

func main() {
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	//store.LoadCollections()

	osd.DefaultStripeSize = 8
	runRpcServer(ch)
}
