/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package store

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/journeymidnight/nentropy/store/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
	defaultKey  = "hello"
	port        = ":50051"
)

func runServer(t *testing.T, done <-chan struct{}) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &server{})
	reflection.Register(s)

	go func() {
		select {
		case <-done:
			go s.GracefulStop()
			go lis.Close()
		}
	}()

	s.Serve(lis)
}

func TestWriteThenReadKey(t *testing.T) {
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
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

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   []byte("1.0"),
		Oid:    []byte("hello"),
		Length: l,
		Offset: 0,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, []byte("world"))
	done <- struct{}{}
}

func TestReadNonExist(t *testing.T) {
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	readreq := &pb.ReadRequest{
		PGID:   []byte("1.0"),
		Oid:    []byte("maynotexist"),
		Length: 0,
		Offset: 0,
	}
	_, err = c.Read(context.Background(), readreq)
	require.NotEqual(t, err, nil)

	//rpc error contains more string info than "no value for this key"
	require.Contains(t, err.Error(), "no value for this key")
	done <- struct{}{}
}

func TestWriteRemoveRead(t *testing.T) {
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
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

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	removereq := &pb.RemoveRequest{
		PGID: []byte("1.0"),
		Oid:  []byte("hello"),
	}

	removeret, err := c.Remove(context.Background(), removereq)
	if err != nil {
		t.Fatalf("could not remove : %v\n", err)
	} else {
		require.Equal(t, removeret.RetCode, int32(0))
	}

	readreq := &pb.ReadRequest{
		PGID:   []byte("1.0"),
		Oid:    []byte("hello"),
		Length: l,
		Offset: 0,
	}

	_, err = c.Read(context.Background(), readreq)
	require.NotEqual(t, err, nil)

	//rpc error contains more string info than "no value for this key"
	require.Contains(t, err.Error(), "no value for this key")
	done <- struct{}{}
}
