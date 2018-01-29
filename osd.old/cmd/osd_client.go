package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"google.golang.org/grpc"
)

const (
	address   = "localhost:50052"
	chunksize = uint64(8)
)

var action = flag.String("action", "", "action to osd, support: createpg, removepg, writeobj, readobj, removeobj, putfile, getfile, getobjsize")
var pgid = flag.String("pgid", "", "id of the pg")
var oid = flag.String("oid", "", "oid of an object")
var offset = flag.Uint64("offset", 0, "specify offset for read/write")
var length = flag.Uint64("length", 0, "specify length for read/write")
var value = flag.String("value", "", "value of the object")
var filename = flag.String("filename", "", "file name")
var outputfile = flag.String("outputfile", "", "file name of output file")

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
		buf := []byte(*value)
		buflen := uint64(len(buf))

		if *length == 0 {
			*length = buflen
		}

		if buflen > *length {
			//shortten the buffer
			buf = buf[:*length]
		} else {
			//use length as actual buflen
			*length = buflen
		}
		req := &pb.WriteRequest{
			PGID:   []byte(*pgid),
			Oid:    []byte(*oid),
			Value:  buf,
			Length: *length,
			Offset: *offset,
		}

		_, err := c.Write(context.Background(), req)
		if err != nil {
			fmt.Printf("objct write failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Println("object write successfully")
		}
	case "getobjsize":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		statreq := &pb.ObjectStatRequest{
			PGID: []byte(*pgid),
			Oid:  []byte(*oid),
		}
		statret, err := c.ObjectStat(context.Background(), statreq)
		if err != nil {
			fmt.Printf("get object size failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Printf("get object size successfully, size is: %d", statret.GetSize())
		}
	case "readobj":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		readreq := &pb.ReadRequest{
			PGID:   []byte(*pgid),
			Oid:    []byte(*oid),
			Length: *length,
			Offset: *offset,
		}
		readret, err := c.Read(context.Background(), readreq)
		if err != nil {
			fmt.Printf("objct write failed, error is  %s\r\n", err.Error())
		} else {
			fmt.Printf("objct write successfully, value is: %X(%s)", readret.ReadBuf, readret.ReadBuf)
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
	case "putfile":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}
		if *filename == "" {
			fmt.Println("please provide a filename")
			os.Exit(-1)
		}

		if _, err := os.Stat(*filename); err != nil {
			if os.IsNotExist(err) {
				fmt.Println("file not exist")
				os.Exit(-1)
			}
		}
		file, err := os.Open(*filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		buf := make([]byte, chunksize) // define your buffer size here.

		offset := uint64(0)
		for {
			n, err := file.Read(buf)

			if n > 0 {
				req := &pb.WriteRequest{
					PGID:   []byte(*pgid),
					Oid:    []byte(*oid),
					Value:  buf[:n],
					Length: uint64(n),
					Offset: offset,
				}

				_, err := c.Write(context.Background(), req)
				if err != nil {
					fmt.Printf("put file at offset %d failed, error is  %s\r\n", offset, err.Error())
					os.Exit(-1)
				} else {
					fmt.Printf("put file at offset %d successed \r\n", offset)
				}
				offset += uint64(n)
			}

			if err == io.EOF {
				fmt.Printf("put file successed \r\n")
				break
			}
			if err != nil {
				log.Printf("read %d bytes: %v", n, err)
				break
			}
		}
	case "getfile":
		if *oid == "" {
			fmt.Println("please provide the oid")
			os.Exit(-1)
		}

		if *outputfile == "" {
			fmt.Println("please provide a output filename")
			os.Exit(-1)
		}

		if _, err := os.Stat(*filename); err == nil {
			fmt.Println("file already exists")
			os.Exit(-1)
		}
		file, err := os.Create(*outputfile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		statreq := &pb.ObjectStatRequest{
			PGID: []byte(*pgid),
			Oid:  []byte(*oid),
		}
		statret, err := c.ObjectStat(context.Background(), statreq)
		if err != nil {
			fmt.Printf("get object size failed, error is  %s\r\n", err.Error())
			os.Exit(-1)
		}
		objsize := statret.GetSize()

		offset := uint64(0)
		for objsize > 0 {
			// try read in chunks
			readreq := &pb.ReadRequest{
				PGID:   []byte(*pgid),
				Oid:    []byte(*oid),
				Length: chunksize,
				Offset: offset,
			}
			readret, err := c.Read(context.Background(), readreq)
			if err != nil {
				os.Exit(-1)
			}

			readlength := uint64(len(readret.ReadBuf))
			if objsize/chunksize > 1 && readlength != chunksize {
				fmt.Println("read length is illegal")
				os.Exit(-1)
			}

			file.WriteAt(readret.ReadBuf, int64(offset))
			objsize -= readlength
			offset += readlength
		}

	default:
		fmt.Println("action not supported")
	}
}