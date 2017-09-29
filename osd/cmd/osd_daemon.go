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
	port = ":50051"
)

func runRpcServer(done <-chan os.Signal) {
	logger := log.New(os.Stdout, "osd::rpcserver: ", log.Ltime)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &osd.Server{})
	reflection.Register(s)

	go func() {
		select {
		case <-done:
			logger.Println("server going to stop now")
			go s.GracefulStop()
			go lis.Close()
		}
	}()

	s.Serve(lis)
}

func main() {
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	//store.LoadCollections()

	runRpcServer(ch)
}
