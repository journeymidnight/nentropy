package main

import (
	"context"
	"fmt"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

func main() {
	// Set up a connection to the server.
	conn, _ := grpc.Dial(address, grpc.WithInsecure())
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	l := uint64(len([]byte("hello")))
	req := &pb.WriteRequest{
		PGID:   []byte("1.0"),
		Oid:    []byte("hello"),
		Value:  []byte("world"),
		Length: l,
		Offset: 0,
	}

	c.Write(context.Background(), req)

	readreq := &pb.ReadRequest{
		PGID:   []byte("1.0"),
		Oid:    []byte("hello"),
		Length: l,
		Offset: 0,
	}
	readret, _ := c.Read(context.Background(), readreq)
	fmt.Println(readret.RetCode)
	fmt.Println(string(readret.ReadBuf))
}
