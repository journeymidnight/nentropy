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
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = "1"
	val := multiraftbase.Value{RawBytes: []byte("value2")}
	putReq := multiraftbase.NewPut([]byte("key2"), val)
	dumy.Request.MustSetInner(putReq)

	ctx := context.Background()
	res, err := client.Batch(ctx, &dumy)
	if err != nil {
		log.Printf("Error send rpc request!")
		return
	}

	log.Printf("Finished! The response is %s!", res)
}
