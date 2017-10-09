package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

var action = flag.String("action", "", "action to osd, support: createpg, removepg, writeobj, readobj, removeobj")
var pgid = flag.String("pgid", "", "id of the pg")
var oid = flag.String("oid", "", "oid of an object")
var value = flag.String("value", "", "value of the object")

func main() {
	flag.Parse()
	if *action == "" {
		fmt.Println("please provide action name")
		os.Exit(-1)
	}

	if *pgid == "" {
		fmt.Println("please provide the pgid")
		os.Exit(-1)
	}

	// Set up a connection to the server.
	conn, _ := grpc.Dial(address, grpc.WithInsecure())
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	switch *action {
	case "createpg":
		creq := &pb.CreatePgRequest{
			PGID: []byte(*pgid),
		}

		_, err := c.CreatePG(context.Background(), creq)
		if err != nil {
			fmt.Printf("failed to create pg, err is %s\r\n", err.Error())
			return
		}
		fmt.Printf("pg %s created successfully\r\n", *pgid)

	case "removepg":
		req := &pb.RemovePgRequest{
			PGID: []byte(*pgid),
		}

		_, err := c.RemovePG(context.Background(), req)
		if err != nil {
			fmt.Printf("failed to remove pg, err is %s\r\n", err.Error())
			return
		}
		fmt.Printf("pg %s removed successfully\r\n", *pgid)
	case "writeobj":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		req := &pb.WriteRequest{
			PGID:   []byte(*pgid),
			Oid:    []byte(*oid),
			Value:  []byte(*value),
			Length: 0,
			Offset: 0,
		}

		_, err := c.Write(context.Background(), req)
		if err != nil {
			fmt.Printf("objct write failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Println("object write successfully")
		}
	case "readobj":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		readreq := &pb.ReadRequest{
			PGID:   []byte(*pgid),
			Oid:    []byte(*oid),
			Length: 0,
			Offset: 0,
		}
		readret, err := c.Read(context.Background(), readreq)
		if err != nil {
			fmt.Printf("objct write failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Printf("objct write successfully, value is  %s\r\n", string(readret.ReadBuf))
		}
	case "removeobj":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		removereq := &pb.RemoveRequest{
			PGID: []byte(*pgid),
			Oid:  []byte(*oid),
		}
		_, err := c.Remove(context.Background(), removereq)
		if err != nil {
			fmt.Printf("objct remove failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Println("objct remove successfully")
		}
	default:
		fmt.Println("action not supported")
	}
}
