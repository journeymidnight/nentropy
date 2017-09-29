package osd

import (
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/journeymidnight/nentropy/osd/protos"
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
	pb.RegisterStoreServer(s, &Server{})
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
	pgid := []byte("1.0")
	req := &pb.WriteRequest{
		PGID:   pgid,
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
		PGID:   pgid,
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
	os.RemoveAll(string(pgid))
}

func TestReadNonExistPG(t *testing.T) {
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
	pgid := []byte("1.0")

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l,
		Offset: 0,
	}
	_, err = c.Read(context.Background(), readreq)
	require.Contains(t, err.Error(), ErrNoSuchPG.Error())
	done <- struct{}{}
	os.RemoveAll(string(pgid))
}

func TestReadNonExistKey(t *testing.T) {
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
	pgid := []byte("1.0")
	req := &pb.WriteRequest{
		PGID:   pgid,
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
		PGID:   pgid,
		Oid:    []byte("maynotexist"),
		Length: 0,
		Offset: 0,
	}
	_, err = c.Read(context.Background(), readreq)
	require.NotEqual(t, err, nil)

	//rpc error contains more string info than "no value for this key"
	require.Contains(t, err.Error(), "no value for this key")
	done <- struct{}{}
	os.RemoveAll(string(pgid))
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
	pgid := []byte("1.0")
	req := &pb.WriteRequest{
		PGID:   pgid,
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
		PGID: pgid,
		Oid:  []byte("hello"),
	}

	removeret, err := c.Remove(context.Background(), removereq)
	if err != nil {
		t.Fatalf("could not remove : %v\n", err)
	} else {
		require.Equal(t, removeret.RetCode, int32(0))
	}

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l,
		Offset: 0,
	}

	_, err = c.Read(context.Background(), readreq)
	require.NotEqual(t, err, nil)

	//rpc error contains more string info than "no value for this key"
	require.Contains(t, err.Error(), "no value for this key")
	done <- struct{}{}
	os.RemoveAll(string(pgid))
}
