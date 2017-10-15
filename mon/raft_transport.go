package main

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"golang.org/x/net/context"

	"fmt"
	"net"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/protos"
	"google.golang.org/grpc"
)

const (
	messageBatchSoftLimit = 10000000
)

// Raft is used to call functions in draft
type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

// Transport is network interface for raft
type Transport interface {
	Start()
	Send(m raftpb.Message)
	Disconnect()
	AddPeer(id uint64, rc protos.RaftContext)
	RemovePeer(id uint64)
}

type sendmsg struct {
	to   uint64
	data []byte
}

// grpcRaftNode struct implements the gRPC server interface.
type grpcRaftNode struct {
	sync.Mutex
	messages  chan sendmsg
	id        uint64
	peersAddr []string // raft peer URLs
	peers     peerPool
	raft      Raft
}

var (
	raftRPCNode   grpcRaftNode
	raftRPCServer *grpc.Server
)

// peerPool stores the peers' addresses and our connections to them.  It has exactly one
// entry for every peer other than ourselves.  Some of these peers might be unreachable or
// have bogus (but never empty) addresses.
type peerPool struct {
	sync.RWMutex
	peers map[uint64]peerPoolEntry
}

var (
	errNoPeerPoolEntry = fmt.Errorf("no peerPool entry")
	errNoPeerPool      = fmt.Errorf("no peerPool pool, could not connect")
)

// getPool returns the non-nil pool for a peer.  This might error even if get(id)
// succeeds, if the pool is nil.  This happens if the peer was configured so badly (it had
// a totally bogus addr) we can't make a pool.  (A reasonable refactoring would have us
// make a pool, one that has a nil gRPC connection.)
//
// You must call pools().release on the pool.
func (p *peerPool) getPool(id uint64) (*pool, error) {
	p.RLock()
	defer p.RUnlock()
	ent, ok := p.peers[id]
	if !ok {
		return nil, errNoPeerPoolEntry
	}
	if ent.poolOrNil == nil {
		return nil, errNoPeerPool
	}
	ent.poolOrNil.AddOwner()
	return ent.poolOrNil, nil
}

func (p *peerPool) get(id uint64) (string, bool) {
	p.RLock()
	defer p.RUnlock()
	ret, ok := p.peers[id]
	return ret.addr, ok
}

func (p *peerPool) set(id uint64, addr string, pl *pool) {
	p.Lock()
	defer p.Unlock()
	if old, ok := p.peers[id]; ok {
		if old.poolOrNil != nil {
			pools().release(old.poolOrNil)
		}
	}
	p.peers[id] = peerPoolEntry{addr, pl}
}

type trans grpcRaftNode

func (w *trans) Send(m raftpb.Message) {
	helper.AssertTruef(w.id != m.To, "Seding message to itself")
	data, err := m.Marshal()
	helper.Check(err)
	if m.Type != raftpb.MsgHeartbeat && m.Type != raftpb.MsgHeartbeatResp {
		helper.Logger.Printf(5, "\t\tSENDING: %v %v-->%v\n", m.Type, m.From, m.To)
	}
	select {
	case w.messages <- sendmsg{to: m.To, data: data}:
		// pass
	default:
		// TODO: It's bad to fail like this.
		helper.Logger.Fatalf(0, "Unable to push messages to channel in send")
	}
}

func (w *trans) Disconnect() {

}

func (w *trans) Start() {
	laddr := "0.0.0.0"
	var err error
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, Config.MonPort))
	if err != nil {
		helper.Logger.Fatalf(0, "While running server: %v", err)
		return
	}
	helper.Logger.Printf(0, "Worker listening at address: %v", ln.Addr())
	protos.RegisterRaftNodeServer(raftRPCServer, &raftRPCNode)
	go raftRPCServer.Serve(ln)
}

func (w *trans) AddPeer(id uint64, rc protos.RaftContext) {
	w.peersAddr = append(w.peersAddr, rc.Addr)
}

func (w *trans) RemovePeer(id uint64) {

}

func (rn *grpcRaftNode) applyMessage(ctx context.Context, msg raftpb.Message) error {
	var rc protos.RaftContext
	helper.Check(rc.Unmarshal(msg.Context))
	rn.connect(msg.From, rc.Addr)

	c := make(chan error, 1)
	go func() { c <- rn.raft.Process(ctx, msg) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c:
		return err
	}
}

func (rn *grpcRaftNode) batchAndSendMessages() {
	batches := make(map[uint64]*bytes.Buffer)
	for {
		totalSize := 0
		sm := <-rn.messages
	slurp_loop:
		for {
			var buf *bytes.Buffer
			if b, ok := batches[sm.to]; !ok {
				buf = new(bytes.Buffer)
				batches[sm.to] = buf
			} else {
				buf = b
			}
			totalSize += 4 + len(sm.data)
			helper.Check(binary.Write(buf, binary.LittleEndian, uint32(len(sm.data))))
			helper.Check2(buf.Write(sm.data))

			if totalSize > messageBatchSoftLimit {
				// We limit the batch size, but we aren't pushing back on
				// n.messages, because the loop below spawns a goroutine
				// to do its dirty work.  This is good because right now
				// (*node).send fails(!) if the channel is full.
				break
			}

			select {
			case sm = <-rn.messages:
			default:
				break slurp_loop
			}
		}

		for to, buf := range batches {
			if buf.Len() == 0 {
				continue
			}
			data := make([]byte, buf.Len())
			copy(data, buf.Bytes())
			go rn.doSendMessage(to, data)
			buf.Reset()
		}
	}
}

// You must call release on the pool.  Can error for some pid's for which GetPeer
// succeeds.
func (rn *grpcRaftNode) getPeerPool(pid uint64) (*pool, error) {
	return rn.peers.getPool(pid)
}

// Never returns ("", true)
func (rn *grpcRaftNode) GetPeer(pid uint64) (string, bool) {
	return rn.peers.get(pid)
}

// addr must not be empty.
func (rn *grpcRaftNode) SetPeer(pid uint64, addr string, poolOrNil *pool) {
	helper.AssertTruef(addr != "", "SetPeer for peer %d has empty addr.", pid)
	rn.peers.set(pid, addr, poolOrNil)
}

// Connects the node and makes its peerPool refer to the constructed pool and address
// (possibly updating ourselves from the old address.)  (Unless pid is ourselves, in which
// case this does nothing.)
func (rn *grpcRaftNode) connect(pid uint64, addr string) {
	if pid == rn.id {
		return
	}
	if paddr, ok := rn.GetPeer(pid); ok && paddr == addr {
		// Already connected.
		return
	}
	// Here's what we do.  Right now peerPool maps peer node id's to addr values.  If
	// a *pool can be created, good, but if not, we still create a peerPoolEntry with
	// a nil *pool.
	p, ok := pools().connect(addr)
	if !ok {
		// TODO: Note this fact in more general peer health info somehow.
		helper.Logger.Printf(10, "Peer %d claims same host as me\n", pid)
	}
	rn.SetPeer(pid, addr, p)
}

func (rn *grpcRaftNode) doSendMessage(to uint64, data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	addr := rn.peersAddr[to-1]
	rn.connect(to, addr)
	pool, err := rn.getPeerPool(to)
	if err != nil {
		// No such peer exists or we got handed a bogus config (bad addr), so we
		// can't send messages to this peer.
		return
	}
	defer pools().release(pool)
	conn := pool.Get()

	c := protos.NewRaftNodeClient(conn)
	p := &protos.Payload{Data: data}

	ch := make(chan error, 1)
	go func() {
		_, err = c.RaftMessage(ctx, p)
		ch <- err
	}()

	select {
	case <-ctx.Done():
		return
	case <-ch:
		// We don't need to do anything if we receive any error while sending message.
		// RAFT would automatically retry.
		return
	}
}

func (rn *grpcRaftNode) RaftMessage(ctx context.Context, query *protos.Payload) (*protos.Payload, error) {
	if ctx.Err() != nil {
		return &protos.Payload{}, ctx.Err()
	}

	for idx := 0; idx < len(query.Data); {
		helper.AssertTruef(len(query.Data[idx:]) >= 4,
			"Slice left of size: %v. Expected at least 4.", len(query.Data[idx:]))

		sz := int(binary.LittleEndian.Uint32(query.Data[idx : idx+4]))
		idx += 4
		msg := raftpb.Message{}
		if idx+sz > len(query.Data) {
			return &protos.Payload{}, helper.Errorf(
				"Invalid query. Specified size %v overflows slice [%v,%v)\n",
				sz, idx, len(query.Data))
		}
		if err := msg.Unmarshal(query.Data[idx : idx+sz]); err != nil {
			helper.Check(err)
		}
		if msg.Type != raftpb.MsgHeartbeat && msg.Type != raftpb.MsgHeartbeatResp {
			helper.Logger.Printf(10, "RECEIVED: %v %v-->%v\n", msg.Type, msg.From, msg.To)
		}
		if err := rn.applyMessage(ctx, msg); err != nil {
			return &protos.Payload{}, err
		}
		idx += sz
	}
	// fmt.Printf("Got %d messages\n", count)
	return &protos.Payload{}, nil
}

// Hello rpc call is used to check connection with other workers after mon
// tcp server for this instance starts.
func (rn *grpcRaftNode) Echo(ctx context.Context, in *protos.Payload) (*protos.Payload, error) {
	return &protos.Payload{Data: in.Data}, nil
}

// StopServer stop the server
func StopServer() {
	if raftRPCServer != nil { // possible if Config.InMemoryComm == true
		raftRPCServer.GracefulStop() // blocking stop server
	}
}

// GetTransport return Transport interface
func GetTransport() Transport {
	return (*trans)(&raftRPCNode)
}

// InitRaftTransport init global variable
func InitRaftTransport(id uint64, raft Raft, addr []string) {
	raftRPCServer = grpc.NewServer()
	peers := peerPool{
		peers: make(map[uint64]peerPoolEntry),
	}
	raftRPCNode = grpcRaftNode{id: id,
		messages:  make(chan sendmsg, 1000),
		peers:     peers,
		raft:      raft,
		peersAddr: addr,
	}
	go raftRPCNode.batchAndSendMessages()
}
