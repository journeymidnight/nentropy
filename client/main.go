package main

import (
	"flag"
	"log"

	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
)

func main() {
	var opts []grpc.DialOption

	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = "1"
	val := multiraftbase.Value{RawBytes: []byte("value1")}
	putReq := multiraftbase.NewPut([]byte("key1"), val)
	dumy.Requests = make([]multiraftbase.RequestUnion, 1)
	dumy.Requests[0].MustSetInner(putReq)

	ctx := context.Background()
	res, err := client.Batch(ctx, &dumy)
	if err != nil {
		log.Printf("Error send rpc request!")
		return
	}

	log.Printf("Finished! The response is %s!", res)
}
