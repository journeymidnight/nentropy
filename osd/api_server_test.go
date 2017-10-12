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
	pb.RegisterStoreServer(s, NewServer())
	reflection.Register(s)

	syncdone := make(chan struct{})
	go syncThread(syncdone)

	go func() {
		select {
		case <-done:
			syncdone <- struct{}{}
			go s.GracefulStop()
			go lis.Close()
		}
	}()

	s.Serve(lis)
}
func TestCreateNonExistPG(t *testing.T) {
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	pgid := []byte("asdf")

	//remove if exists
	os.RemoveAll(string(pgid))
	req := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), req)
	require.Equal(t, err, nil)

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)

	done <- struct{}{}
}

func TestCreateExistingPG(t *testing.T) {
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	pgid := []byte("asdf")
	os.Mkdir(string(pgid), 0755)
	req := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), req)
	require.Contains(t, err.Error(), ErrPGAlreadyExists.Error())

	done <- struct{}{}
	os.RemoveAll(string(pgid))
}

func TestAlignedWriteAndRead_fullstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("hellohah")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
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
	require.Equal(t, readret.ReadBuf, value)

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestAlignedWriteAndRead_notfullstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
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
	require.Equal(t, readret.ReadBuf, value)

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestAlignedWriteAndRead_readatlonglength(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
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
		Length: l + 100,
		Offset: 0,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestAlignedWriteAndRead_alignto16(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 16, // read at 16 to get what we want
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
		Offset: 16,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value)

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}
func TestAlignedWriteAndRead_alignto64_comparewholevalue(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 64,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: 64 + l, //read the whole thing, so length is 64 + l
		Offset: 0,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))

	var buf []byte
	for i := 0; i < 64; i++ {
		buf = append(buf, 0)
	}
	buf = append(buf, value...)

	require.Equal(t, readret.ReadBuf, buf)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

//rewrite the original stripe
func TestAlignedWriteAndRead_rewritewhole(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("abcdefg")
	value2 := []byte("ABCDEFG")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 0,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 0,
	}

	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))
	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l2,
		Offset: 0,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value2)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestAlignedWriteAndRead_rewritepart(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	value2 := []byte("world")
	value3 := []byte("worldha")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 0,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 0,
	}

	r, err = c.Write(context.Background(), req)
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
	require.Equal(t, readret.ReadBuf, value3)

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestAlignedWriteAndRead_crossstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("hellohahasdfasdfasdfasdfasdf")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
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
	require.Equal(t, readret.ReadBuf, value)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}
func TestUnAlignedWriteAndRead_sameoffset(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("helloha")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1, // use 1 as offset
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
		Offset: 1, //read at offset 1
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_differentoffset(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("asdfasd")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1, // use 3 as offset
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: 1, // read single byte from store
		Offset: 3, //read at offset 1
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, []byte("d"))
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_crossstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf")
	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 100,
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
		Offset: 100,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_rewritewhole_notcrossstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("asdfas")
	value2 := []byte("sdfasd")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 1,
	}

	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l2,
		Offset: 1,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value2)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_rewritewhole_crossstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("abcdefghijklmnopqrstuvwxyz")
	value2 := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")

	//remove if exists
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 1,
	}
	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l2,
		Offset: 1,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value2)
	done <- struct{}{}
}
func TestUnAlignedWriteAndRead_rewritepart_crossstripe(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("abcdefghijklmnopqrstuvwxyz")
	value2 := []byte("BCDEFGHIJKLMNOPQRSTUVWXY")
	value3 := []byte("BCDEFGHIJKLMNOPQRSTUVWXYyz")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")

	//remove if exists
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 1,
	}
	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l, //read the whole thing, which is same length of value1
		Offset: 1,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value3)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_rewritepart_crossstripe_differentoffset(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("abcdefghijklmnopqrstuvwxyz")
	value2 := []byte("BCDEFGHIJKLMNOPQRSTUVWXY")
	value3 := []byte("aBCDEFGHIJKLMNOPQRSTUVWXYz")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")

	//remove if exists
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 2,
	}
	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l, //read the whole thing, which is same length of value1
		Offset: 1,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value3)
	done <- struct{}{}
}

func TestUnAlignedWriteAndRead_rewritepart_crossstripe_differentoffset_badstripesize(t *testing.T) {
	DefaultStripeSize = 7 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	value := []byte("abcdefghijklmnopqrstuvwxyz")
	value2 := []byte("BCDEFGHIJKLMNOPQRSTUVWXY")
	value3 := []byte("aBCDEFGHIJKLMNOPQRSTUVWXYz")
	l := uint64(len(value))
	l2 := uint64(len(value2))
	pgid := []byte("1.0")

	//remove if exists
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 1,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	req = &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value2,
		Length: l2,
		Offset: 2,
	}
	r, err = c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	readreq := &pb.ReadRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Length: l, //read the whole thing, which is same length of value1
		Offset: 1,
	}
	readret, err := c.Read(context.Background(), readreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}

	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	require.Equal(t, readret.RetCode, int32(0))
	require.Equal(t, readret.ReadBuf, value3)
	done <- struct{}{}
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
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

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
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
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

	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

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
	require.Contains(t, err.Error(), ErrNoValueForKey.Error())
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
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
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)
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
	require.Contains(t, err.Error(), ErrNoValueForKey.Error())

	pgremovereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), pgremovereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestWriteAndGetObjectStat_zerooffset(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	var value []byte
	for i := 0; i < 1024; i++ {
		value = append(value, 'p')
	}

	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: 0,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	statreq := &pb.ObjectStatRequest{
		PGID: pgid,
		Oid:  []byte("hello"),
	}
	statret, err := c.ObjectStat(context.Background(), statreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, statret.Size, l)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}

func TestWriteAndGetObjectStat_haveoffset(t *testing.T) {
	DefaultStripeSize = 8 // smaller stripe size for simple test
	done := make(chan struct{})
	go runServer(t, done)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewStoreClient(conn)

	var value []byte
	offset := uint64(110)
	for i := 0; i < 1024; i++ {
		value = append(value, 'p')
	}

	l := uint64(len(value))
	pgid := []byte("1.0")
	os.RemoveAll(string(pgid))
	creq := &pb.CreatePgRequest{
		PGID: pgid,
	}

	_, err = c.CreatePG(context.Background(), creq)
	require.Equal(t, err, nil)

	req := &pb.WriteRequest{
		PGID:   pgid,
		Oid:    []byte("hello"),
		Value:  value,
		Length: l,
		Offset: offset,
	}

	r, err := c.Write(context.Background(), req)
	if err != nil {
		t.Fatalf("could not write: %v\n", err)
	}
	require.Equal(t, r.RetCode, int32(0))

	statreq := &pb.ObjectStatRequest{
		PGID: pgid,
		Oid:  []byte("hello"),
	}
	statret, err := c.ObjectStat(context.Background(), statreq)
	if err != nil {
		t.Fatalf("could not read: %v", err)
	}
	require.Equal(t, statret.Size, l+offset)
	removereq := &pb.RemovePgRequest{
		PGID: pgid,
	}
	_, err = c.RemovePG(context.Background(), removereq)
	require.Equal(t, err, nil)
	done <- struct{}{}
}
