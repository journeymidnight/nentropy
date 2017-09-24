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

package mon

import (
	"sync"

	"golang.org/x/net/context"

	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/journeymidnight/nentropy/helper"
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
	currDNM   protos.DataNodeMap

	// kvstore store key and values
	kvStore *kvstore
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

// StartRaftNodes will read the WAL dir, create the RAFT cluster,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(walStore *badger.KV, bindall bool) {
	clus = new(cluster)
	clus.ctx, clus.cancel = context.WithCancel(context.Background())

	clus.wal = raftwal.Init(walStore, Config.RaftId)

	var wg sync.WaitGroup

	peers := strings.Split(Config.PeerAddr, ",")
	clus.peersAddr = peers
	for i, v := range peers {
		if uint64(i+1) == Config.RaftId {
			clus.myAddr = v
		}
	}
	node := clus.newNode(Config.RaftId, clus.myAddr)

	node.peersAddr = peers
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.InitAndStartNode(clus.wal)
	}()

	wg.Wait()
	clus.kvStore = newKVStore()
}

func (c *cluster) getKvStore() *kvstore {
	return c.kvStore
}

func (c *cluster) Node() *node {
	if c.node != nil {
		return c.node
	}
	return nil
}

func (c *cluster) newNode(nodeId uint64, myAddr string) *node {

	node := newNode(nodeId, myAddr)
	if c.node != nil {
		helper.AssertTruef(false, "Didn't expect a node in RAFT group mapping: %v", 0)
	}
	c.node = node
	return node
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

// snapshotAll takes snapshot of all nodes of the mon group
func snapshotAll() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(n *node) {
		defer wg.Done()
		n.snapshot(0)
	}(clus.Node())
	wg.Wait()
}

// StopAllNodes stops all the nodes of the mon group.
func stopNode() {
	clus.Node().Stop()
}

// Hello rpc call is used to check connection with other workers after mon
// tcp server for this instance starts.
func (w *grpcRaftNode) Echo(ctx context.Context, in *protos.Payload) (*protos.Payload, error) {
	return &protos.Payload{Data: in.Data}, nil
}

// RunServer initializes a tcp server on port which listens to requests from
// other workers for internal communication.
func RunServer(bindall bool) {
	laddr := "localhost"
	if bindall {
		laddr = "0.0.0.0"
	}
	var err error
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, Config.WorkerPort))
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
	stopNode()                // blocking stop all nodes
	if raftRpcServer != nil { // possible if Config.InMemoryComm == true
		raftRpcServer.GracefulStop() // blocking stop server
	}
	// blocking sync all marks
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	snapshotAll()
}

func ProposeMessage(key string, val string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{key, val}); err != nil {
		helper.Logger.Fatal(0, err)
	}
	proposal := protos.Proposal{Data: []byte(buf.String())}
	getCluster().Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func Lookup(key string) (string, bool) {
	val, _ := getCluster().kvStore.Lookup(key)
	helper.Logger.Println(0, "lookup() key:", key, "val:", val)
	return val, true
}

func ProposeDataNodeMap(dnm *protos.DataNodeMap) error {
	proposal := protos.Proposal{Dnm: dnm}
	getCluster().Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func GetCurrentDataNodeMap() (protos.DataNodeMap, error) {
	return getCluster().currDNM, nil
}
