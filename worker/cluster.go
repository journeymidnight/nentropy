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

package worker

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/dgraph-io/badger"
	"github.com/journeymidnight/nentropy/raftwal"
	"github.com/journeymidnight/nentropy/x"
	"strings"
)

type clus struct {
	x.SafeMutex
	ctx       context.Context
	cancel    context.CancelFunc
	wal       *raftwal.Wal
	node      *node
	myAddr    string
	peersAddr []string // raft peer URLs
}

var gr *clus

func cluster() *clus {
	return gr
}

// StartRaftNodes will read the WAL dir, create the RAFT cluster,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrace to the RAFT
// world from main.go.
func StartRaftNodes(walStore *badger.KV, bindall bool) {
	gr = new(clus)
	gr.ctx, gr.cancel = context.WithCancel(context.Background())

	gr.wal = raftwal.Init(walStore, Config.RaftId)

	var wg sync.WaitGroup

	peers := strings.Split(Config.PeerAddr, ",")
	gr.peersAddr = peers
	for i, v := range peers {
		if uint64(i + 1) == Config.RaftId {
			gr.myAddr = v
		}
	}
	node := gr.newNode(Config.RaftId, gr.myAddr)

	node.peersAddr = peers
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.InitAndStartNode(gr.wal)
	}()

	wg.Wait()
}

func (g *clus) Node() *node {
	g.RLock()
	defer g.RUnlock()
	if g.node != nil {
		return g.node
	}
	return nil
}

func (g *clus) newNode(nodeId uint64, myAddr string) *node {
	g.Lock()
	defer g.Unlock()

	node := newNode(nodeId, myAddr)
	if g.node != nil {
		x.AssertTruef(false, "Didn't expect a node in RAFT group mapping: %v", 0)
	}
	g.node = node
	return node
}

func (g *clus) Server(id uint64) (rs string, found bool) {
	g.RLock()
	defer g.RUnlock()

	return "", false
}

// Peer returns node(raft) id of the peer of given nodeid of given group
func (g *clus) Peer(nodeId uint64) (uint64, bool) {
	g.RLock()
	defer g.RUnlock()

	for idx, _ := range g.node.peersAddr {
		if uint64(idx + 1) != nodeId {
			return uint64(idx + 1), true
		}
	}
	return 0, false
}

// Leader will try to return the leader of a given group, based on membership information.
// There is currently no guarantee that the returned server is the leader of the group.
func (g *clus) Leader() (uint64, string) {
	g.RLock()
	defer g.RUnlock()

	id := g.node._raft.Status().Lead
	addr := g.node.peersAddr[id - 1]

	return id, addr
}

func (g *clus) nodes() (nodes []*node) {
	g.RLock()
	defer g.RUnlock()
	nodes = append(nodes, g.node)
	return
}

// snapshotAll takes snapshot of all nodes of the worker group
func snapshotAll() {
	var wg sync.WaitGroup
	for _, n := range cluster().nodes() {
		wg.Add(1)
		go func(n *node) {
			defer wg.Done()
			n.snapshot(0)
		}(n)
	}
	wg.Wait()
}

// StopAllNodes stops all the nodes of the worker group.
func stopAllNodes() {
	for _, n := range cluster().nodes() {
		n.Stop()
	}
}
