/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"encoding/binary"
	"sync"

	"golang.org/x/net/context"

	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger"
	"github.com/journeymidnight/nentropy/consistent"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/raftwal"
	"google.golang.org/grpc"
	"net"
	"strings"
	"time"
)

type cluster struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wal       *raftwal.Wal
	node      *node
	myAddr    string
	peersAddr []string // raft peer URLs
	osdMap    protos.OsdMap
	poolMap   protos.PoolMap
	pgMaps    protos.PgMaps
	hashRing  *consistent.Consistent
}

// grpcRaftNode struct implements the gRPC server interface.
type grpcRaftNode struct {
	sync.Mutex
}

var (
	raftRpcServer *grpc.Server
)

var clus *cluster

func getCluster() *cluster {
	return clus
}

func (c *cluster) Node() *node {
	if c.node != nil {
		return c.node
	}
	return nil
}

// Peer returns node(raft) id of the peer of given nodeid of given group
func (c *cluster) Peer(nodeId uint64) (uint64, bool) {

	for idx := range c.node.peersAddr {
		if uint64(idx+1) != nodeId {
			return uint64(idx + 1), true
		}
	}
	return 0, false
}

var (
	errNoNode = fmt.Errorf("No node has been set up yet")
)

func handleCommittedMsg(userData []byte) error {
	// We derive the schema here if it's not present
	// Since raft committed logs are serialized, we can derive
	// schema here without any locking
	proposal := &protos.Proposal{}
	if err := proposal.Unmarshal(userData); err != nil {
		helper.Logger.Fatalf(0, "Unable to unmarshal proposal: %v %q\n", err, userData)
	}

	data := string(proposal.Data)
	if &data != nil {
		if proposal.Type == protos.Proposal_DATA_TYPE_DATA_NODE_MAP {
			var osdMap protos.OsdMap
			if err := osdMap.Unmarshal(proposal.Data); err != nil {
				helper.Check(err)
			}
			getCluster().osdMap = osdMap
		}

	}
	return nil
}

func applyMessage(ctx context.Context, msg raftpb.Message) error {
	var rc protos.RaftContext
	helper.Check(rc.Unmarshal(msg.Context))
	node := clus.Node()
	if node == nil {
		// Maybe we went down, went back up, reconnected, and got an RPC
		// message before we set up Raft?
		return errNoNode
	}
	node.Connect(msg.From, rc.Addr)

	c := make(chan error, 1)
	go func() { c <- node.Step(ctx, msg) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c:
		return err
	}
}

func (w *grpcRaftNode) RaftMessage(ctx context.Context, query *protos.Payload) (*protos.Payload, error) {
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
		if err := applyMessage(ctx, msg); err != nil {
			return &protos.Payload{}, err
		}
		idx += sz
	}
	// fmt.Printf("Got %d messages\n", count)
	return &protos.Payload{}, nil
}

func (w *grpcRaftNode) JoinCluster(ctx context.Context, rc *protos.RaftContext) (*protos.Payload, error) {
	if ctx.Err() != nil {
		return &protos.Payload{}, ctx.Err()
	}

	// TODO:
	// Check the node if it is exist

	node := clus.Node()
	if node == nil {
		return &protos.Payload{}, nil
	}
	helper.Logger.Println(10, "JoinCluster: id:", rc.Id, "Addr:", rc.Addr)
	node.Connect(rc.Id, rc.Addr)
	helper.Logger.Println(10, "after connection")

	c := make(chan error, 1)
	go func() { c <- node.AddToCluster(ctx, rc.Id) }()

	select {
	case <-ctx.Done():
		return &protos.Payload{}, ctx.Err()
	case err := <-c:
		return &protos.Payload{}, err
	}
}

// Hello rpc call is used to check connection with other workers after mon
// tcp server for this instance starts.
func (w *grpcRaftNode) Echo(ctx context.Context, in *protos.Payload) (*protos.Payload, error) {
	return &protos.Payload{Data: in.Data}, nil
}

// StartRaftNodes will read the WAL dir, create the RAFT cluster,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(walStore *badger.KV) {
	clus = new(cluster)
	clus.ctx, clus.cancel = context.WithCancel(context.Background())

	clus.wal = raftwal.Init(walStore, Config.RaftId)

	var wg sync.WaitGroup

	mons := strings.Split(Config.Monitors, ",")
	for _, mon := range mons {
		if strings.Contains(mon, ":") {
			clus.peersAddr = append(clus.peersAddr, mon)
		} else {
			clus.peersAddr = append(clus.peersAddr, fmt.Sprintf("%s:%d", mon, Config.MonPort))
		}
	}
	for i, v := range mons {
		if uint64(i+1) == Config.RaftId {
			clus.myAddr = v
		}
	}
	node := newNode(Config.RaftId, clus.myAddr)
	if clus.node != nil {
		helper.AssertTruef(false, "Didn't expect a node in RAFT group mapping: %v", 0)
	}
	clus.node = node

	node.peersAddr = mons
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.InitAndStartNode(clus.wal)
	}()

	wg.Wait()
}

// RunServer initializes a tcp server on port which listens to requests from
// other workers for internal communication.
func RunServer() {
	laddr := "0.0.0.0"
	var err error
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, Config.MonPort))
	if err != nil {
		helper.Logger.Fatalf(0, "While running server: %v", err)
		return
	}
	helper.Logger.Printf(0, "Worker listening at address: %v", ln.Addr())
	raftRpcServer = grpc.NewServer()
	protos.RegisterRaftNodeServer(raftRpcServer, &grpcRaftNode{})
	go raftRpcServer.Serve(ln)
}

// BlockingStop stops all the nodes, server between other workers and syncs all marks.
func BlockingStop() {
	clus.Node().Stop()        // blocking stop all nodes
	if raftRpcServer != nil { // possible if Config.InMemoryComm == true
		raftRpcServer.GracefulStop() // blocking stop server
	}
	// blocking sync all marks
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
}

func ProposeOsdMap(osdMap *protos.OsdMap) error {
	data, err := osdMap.Marshal()
	helper.Check(err)
	proposal := protos.Proposal{Data: data}
	clus.Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func GetCurrOsdMap() (protos.OsdMap, error) {
	return clus.osdMap, nil
}

func ProposePoolMap(poolMap *protos.PoolMap) error {
	data, err := poolMap.Marshal()
	helper.Check(err)
	proposal := protos.Proposal{Data: data}
	clus.Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func GetCurrPoolMap() (protos.PoolMap, error) {
	return clus.poolMap, nil
}

func ProposePgMaps(pgMaps *protos.PgMaps) error {
	data, err := pgMaps.Marshal()
	helper.Check(err)
	proposal := protos.Proposal{Data: data}
	clus.Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func GetCurrPgMaps() (protos.PgMaps, error) {
	return clus.pgMaps, nil
}

func NotifyMemberEvent(eventType memberlist.MemberEventType, member memberlist.Member) error {
	return nil
}
