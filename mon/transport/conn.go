package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/protos"

	"google.golang.org/grpc"
)

var (
	errNoConnection = fmt.Errorf("No connection exists")
)

const (
	GrpcMaxSize = 256 << 20
)

// "pool" is used to manage the grpc client connection(s) for communicating with other
// mon instances.  Right now it just holds one of them.
type pool struct {
	// A "pool" now consists of one connection.  gRPC uses HTTP2 transport to combine
	// messages in the same TCP stream.
	conn *grpc.ClientConn

	Addr string
	// Requires a lock on poolsi.
	refcount int64
}

type poolsi struct {
	sync.RWMutex
	all map[string]*pool
}

var pi *poolsi

func init() {
	pi = new(poolsi)
	pi.all = make(map[string]*pool)
}

func pools() *poolsi {
	return pi
}

func (p *poolsi) get(addr string) (*pool, error) {
	p.RLock()
	defer p.RUnlock()
	pool, ok := p.all[addr]
	if !ok {
		return nil, errNoConnection
	}
	pool.AddOwner()
	return pool, nil
}

// One of these must be called for each call to get(...).
func (p *poolsi) release(pl *pool) {
	// We close the conn after unlocking p.
	newRefcount := atomic.AddInt64(&pl.refcount, -1)
	if newRefcount == 0 {
		p.Lock()
		delete(p.all, pl.Addr)
		p.Unlock()

		destroyPool(pl)
	}
}

func destroyPool(pl *pool) {
	err := pl.conn.Close()
	if err != nil {
		helper.Printf(5, "Error closing cluster connection: %v\n", err.Error())
	}
}

// Returns a pool that you should call put() on.
func (p *poolsi) connect(addr string) (*pool, bool) {
	p.RLock()
	existingPool, has := p.all[addr]
	if has {
		p.RUnlock()
		existingPool.AddOwner()
		return existingPool, true
	}
	p.RUnlock()

	pool, err := newPool(addr)
	// TODO: Rename newPool to newConn, rename pool.
	// TODO: This can get triggered with totally bogus config.
	if err != nil {
		helper.Fatalf("Unable to connect to host %s", addr)
	}

	p.Lock()
	existingPool, has = p.all[addr]
	if has {
		p.Unlock()
		destroyPool(pool)
		existingPool.refcount++
		return existingPool, true
	}
	p.all[addr] = pool
	pool.AddOwner() // matches p.put() run by caller
	p.Unlock()

	// No need to block this thread just to print some messages.
	pool.AddOwner() // matches p.put() in goroutine
	go func() {
		defer p.release(pool)
		err = testConnection(pool)
		if err != nil {
			helper.Printf(0, "Connection to %q fails, got error: %v\n", addr, err)
			// Don't return -- let's still put the empty pool in the map.  Its users
			// have to handle errors later anyway.
		} else {
			helper.Printf(0, "Connection with %q healthy.\n", addr)
		}
	}()

	return pool, true
}

// testConnection tests if we can run an Echo query on a connection.
func testConnection(p *pool) error {
	conn := p.Get()

	query := new(protos.Payload)
	query.Data = make([]byte, 10)
	_, err := rand.Read(query.Data)
	helper.Check(err)

	c := protos.NewRaftNodeClient(conn)
	resp, err := c.Echo(context.Background(), query)
	if err != nil {
		return err
	}
	// If a server is sending bad echos, do we have to freak out and die?
	helper.AssertTruef(bytes.Equal(resp.Data, query.Data),
		"non-matching Echo response value from %v", p.Addr)
	return nil
}

// NewPool creates a new "pool" with one gRPC connection, refcount 0.
func newPool(addr string) (*pool, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GrpcMaxSize),
			grpc.MaxCallSendMsgSize(GrpcMaxSize)),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	// The pool hasn't been added to poolsi yet, so it gets no refcount.
	return &pool{conn: conn, Addr: addr, refcount: 0}, nil
}

// Get returns the connection to use from the pool of connections.
func (p *pool) Get() *grpc.ClientConn {
	return p.conn
}

// AddOwner adds 1 to the refcount for the pool (atomically).
func (p *pool) AddOwner() {
	atomic.AddInt64(&p.refcount, 1)
}
