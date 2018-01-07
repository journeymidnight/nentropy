package main

import (
	"flag"
	"log"

	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	addr   = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
	key    = flag.String("key", "", "key")
	val    = flag.String("val", "", "val")
	method = flag.String("method", "", "get or put")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = "1"
	if *method == "get" {
		getReq := multiraftbase.NewGet([]byte(*key))
		dumy.Request.MustSetInner(getReq)

		ctx := context.Background()
		res, err := client.Batch(ctx, &dumy)
		if err != nil {
			log.Printf("Error send rpc request!")
			return
		}

		getRes := res.Responses.GetValue().(*multiraftbase.GetResponse)
		log.Printf("Finished! res=%s", string(getRes.Value.RawBytes))

	} else if *method == "put" {
		data := multiraftbase.Value{RawBytes: []byte(*val)}
		putReq := multiraftbase.NewPut([]byte(*key), data)
		dumy.Request.MustSetInner(putReq)

		ctx := context.Background()
		res, err := client.Batch(ctx, &dumy)
		if err != nil {
			log.Printf("Error send rpc request!")
			return
		}

		log.Printf("Finished! res=%s", res)

	} else {
		log.Panicln("unknow method.")
	}
}
