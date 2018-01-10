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
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/mon/raftwal"
	"github.com/journeymidnight/nentropy/mon/transport"
	"github.com/journeymidnight/nentropy/protos"
	"google.golang.org/grpc"
)

const (
	errorNodeIDExists = "Error Node ID already exists in the cluster"
)

type HandleCommittedMsg func(data []byte) error
type HandleConfChange func(data []byte) error

type proposalCtx struct {
	ch  chan error
	ctx context.Context
}

type proposals struct {
	sync.RWMutex
	ids map[uint32]*proposalCtx
}

func (p *proposals) Store(pid uint32, pctx *proposalCtx) bool {
	p.Lock()
	defer p.Unlock()
	if _, has := p.ids[pid]; has {
		return false
	}
	p.ids[pid] = pctx
	return true
}

func (p *proposals) Ctx(pid uint32) (context.Context, bool) {
	p.RLock()
	defer p.RUnlock()
	if pd, has := p.ids[pid]; has {
		return pd.ctx, true
	}
	return nil, false
}

func (p *proposals) Done(pid uint32, err error) {
	p.Lock()
	pd, has := p.ids[pid]
	if has {
		delete(p.ids, pid)
	}
	p.Unlock()
	if !has {
		return
	}
	pd.ch <- err
}

func (p *proposals) Has(pid uint32) bool {
	p.RLock()
	defer p.RUnlock()
	_, has := p.ids[pid]
	return has
}

type node struct {
	helper.SafeMutex

	// SafeMutex is for fields which can be changed after init.
	_confState *raftpb.ConfState
	_raft      raft.Node

	// Fields which are never changed after init.
	cfg         *raft.Config
	applyCh     chan raftpb.Entry
	ctx         context.Context
	stop        chan struct{} // to send the stop signal to Run
	done        chan struct{} // to check whether node is running or not
	gid         uint32
	id          uint64
	peersAddr   map[uint64]string
	props       proposals
	raftContext *protos.RaftContext
	store       *raft.MemoryStorage
	wal         *raftwal.Wal
	transport   transport.Transport
	leader      bool

	canCampaign bool

	handleCommittedMsg HandleCommittedMsg
	HandleConfChange   HandleConfChange
}

// SetRaft would set the provided raft.Node to this node.
// It would check fail if the node is already set.
func (n *node) SetRaft(r raft.Node) {
	n.Lock()
	defer n.Unlock()
	helper.AssertTrue(n._raft == nil)
	n._raft = r
}

// Raft would return back the raft.Node stored in the node.
func (n *node) Raft() raft.Node {
	n.RLock()
	defer n.RUnlock()
	return n._raft
}

// SetConfState would store the latest ConfState generated by ApplyConfChange.
func (n *node) SetConfState(cs *raftpb.ConfState) {
	n.Lock()
	defer n.Unlock()
	n._confState = cs
}

// ConfState would return the latest ConfState stored in node.
func (n *node) ConfState() *raftpb.ConfState {
	n.RLock()
	defer n.RUnlock()
	return n._confState
}

func newNode(id uint64, myAddr string) *node {
	helper.Logger.Printf(10, "Node with ID: %v\n", id)

	props := proposals{
		ids: make(map[uint32]*proposalCtx),
	}

	store := raft.NewMemoryStorage()
	rc := &protos.RaftContext{
		Addr: myAddr,
		Id:   id,
	}

	n := &node{
		ctx:   context.Background(),
		id:    id,
		store: store,
		cfg: &raft.Config{
			ID:              id,
			ElectionTick:    10, // 200 ms if we call Tick() every 20 ms.
			HeartbeatTick:   1,  // 20 ms if we call Tick() every 20 ms.
			Storage:         store,
			MaxSizePerMsg:   256 << 10,
			MaxInflightMsgs: 256,
			Logger:          &raft.DefaultLogger{Logger: helper.Logger.Logger},
		},
		applyCh:     make(chan raftpb.Entry, numPendingMutations),
		props:       props,
		raftContext: rc,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	return n
}

// Never returns ("", true)
func (n *node) SetCommittedMsgHandler(callback HandleCommittedMsg) {
	n.handleCommittedMsg = callback
}

/*
func (n *node) AddToCluster(ctx context.Context, pid uint64) error {
	addr, ok := n.GetPeer(pid)
	helper.AssertTruef(ok, "Unable to find conn pool for peer: %d", pid)
	rc := &protos.RaftContext{
		Addr: addr,
		Id:   pid,
	}
	rcBytes, err := rc.Marshal()
	helper.Check(err)
	return n.Raft().ProposeConfChange(ctx, raftpb.ConfChange{
		ID:      pid,
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  pid,
		Context: rcBytes,
	})
}
*/

func (n *node) ProposeAndWait(ctx context.Context, proposal *protos.Proposal) error {
	if n.Raft() == nil {
		return errors.Errorf("RAFT isn't initialized yet")
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	che := make(chan error, 1)
	pctx := &proposalCtx{
		ch:  che,
		ctx: ctx,
	}
	for {
		id := rand.Uint32()
		if n.props.Store(id, pctx) {
			proposal.Id = id
			break
		}
	}

	sz := proposal.Size()
	slice := make([]byte, sz)

	upto, err := proposal.MarshalTo(slice)
	if err != nil {
		return err
	}

	//	we don't timeout on a mutation which has already been proposed.
	if err = n.Raft().Propose(ctx, slice[:upto]); err != nil {
		return helper.Wrapf(err, "While proposing")
	}

	err = <-che
	if err != nil {
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf(err.Error())
		}
	}
	return err
}

const numPendingMutations = 10000

func (n *node) processApplyCh() {
	for e := range n.applyCh {
		if len(e.Data) == 0 {
			continue
		}

		if e.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			cc.Unmarshal(e.Data)

			var rc protos.RaftContext
			if len(cc.Context) > 0 {
				helper.Check(rc.Unmarshal(cc.Context))
				//n.Connect(rc.Id, rc.Addr)
				//n.peersAddr = append(n.peersAddr, rc.Addr)
				helper.Logger.Println(10, "ConfChange rc.ID:", rc.Id, "rc.Addr", rc.Addr)
			}
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					n.transport.AddPeer(rc.Id, rc)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(n.id) {
					n.transport.RemovePeer(rc.Id)
				}
			}

			cs := n.Raft().ApplyConfChange(cc)
			helper.Logger.Println(10, "ConfChange cc.ID:", cc.ID, "cc.NodeID", cc.NodeID)
			n.SetConfState(cs)
			continue
		}

		helper.AssertTrue(e.Type == raftpb.EntryNormal)
		helper.Logger.Println(5, "Process EntryNormal for raft!")

		proposal := &protos.Proposal{}
		if err := proposal.Unmarshal(e.Data); err != nil {
			helper.Logger.Fatalf(0, "Unable to unmarshal proposal: %v %q\n", err, e.Data)
		}
		var err error
		if handleCommittedMsg != nil {
			err = handleCommittedMsg(proposal.Data)
		}
		n.props.Done(proposal.Id, err)
	}
}

func (n *node) saveToStorage(s raftpb.Snapshot, h raftpb.HardState,
	es []raftpb.Entry) {
	if !raft.IsEmptySnap(s) {
		le, err := n.store.LastIndex()
		if err != nil {
			log.Fatalf("While retrieving last index: %v\n", err)
		}
		if s.Metadata.Index <= le {
			return
		}

		if err := n.store.ApplySnapshot(s); err != nil {
			log.Fatalf("Applying snapshot: %v", err)
		}
	}

	if !raft.IsEmptyHardState(h) {
		n.store.SetHardState(h)
	}
	n.store.Append(es)
}

func (n *node) retrieveSnapshot(peerID uint64) {
	return
}

func (n *node) Run() {
	firstRun := true
	// See also our configuration of HeartbeatTick and ElectionTick.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	rcBytes, err := n.raftContext.Marshal()
	helper.Check(err)
	for {
		select {
		case <-ticker.C:
			n.Raft().Tick()

		case rd := <-n.Raft().Ready():
			if rd.SoftState != nil {
				helper.Logger.Println(5, "enter raft ready change SoftState:", rd.RaftState, n.leader)
				if rd.RaftState == raft.StateFollower && n.leader {
					// stepped down as leader do a sync membership immediately
					//cluster().syncMemberships()
					clus.internalMapLock.Lock()
					clus.isPrimaryMon = false
					clus.leaderPgLocationMap = nil
					memberlist.SetMonFollower()
					clus.internalMapLock.Unlock()

				} else if rd.RaftState == raft.StateLeader && !n.leader {
					//leaseMgr().resetLease(n.gid)
					//cluster().syncMemberships()
					clus.internalMapLock.Lock()
					clus.isPrimaryMon = true
					clus.leaderPgLocationMap = make(map[string]int32)
					memberlist.SetMonLeader()
					clus.internalMapLock.Unlock()
				}
				n.leader = rd.RaftState == raft.StateLeader
			}
			helper.Check(n.wal.StoreSnapshot(n.gid, rd.Snapshot))
			helper.Check(n.wal.Store(n.gid, rd.HardState, rd.Entries))

			n.saveToStorage(rd.Snapshot, rd.HardState, rd.Entries)

			for _, msg := range rd.Messages {
				// NOTE: We can do some optimizations here to drop messages.
				msg.Context = rcBytes
				//n.send(msg)
				helper.Logger.Println(5, "msg, from:", msg.From, " to:", msg.To, " type:", msg.Type)
				n.transport.Send(msg)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				// We don't send snapshots to other nodes. But, if we get one, that means
				// either the leader is trying to bring us up to state; or this is the
				// snapshot that I created. Only the former case should be handled.
				var rc protos.RaftContext
				helper.Check(rc.Unmarshal(rd.Snapshot.Data))
				if rc.Id != n.id {
					helper.Logger.Printf(10, "-------> SNAPSHOT [%d] from %d\n", n.gid, rc.Id)
					n.retrieveSnapshot(rc.Id)
					helper.Logger.Printf(10, "-------> SNAPSHOT [%d]. DONE.\n", n.gid)
				} else {
					helper.Logger.Printf(10, "-------> SNAPSHOT [%d] from %d [SELF]. Ignoring.\n", n.gid, rc.Id)
				}
			}
			if len(rd.CommittedEntries) > 0 {
				helper.Logger.Println(10, "Ready(): message count: ", len(rd.CommittedEntries))
				if tr, ok := trace.FromContext(n.ctx); ok {
					tr.LazyPrintf("Found %d committed entries", len(rd.CommittedEntries))
				}
			}

			for _, entry := range rd.CommittedEntries {
				// Just queue up to be processed. Don't wait on them.
				n.applyCh <- entry
			}

			n.Raft().Advance()
			if firstRun && n.canCampaign {
				go n.Raft().Campaign(n.ctx)
				firstRun = false
			}

		case <-n.stop:
			if peerId, has := getCluster().Peer(config.RaftId); has && n.AmLeader() {
				n.Raft().TransferLeadership(n.ctx, config.RaftId, peerId)
				go func() {
					select {
					case <-n.ctx.Done(): // time out
						if tr, ok := trace.FromContext(n.ctx); ok {
							tr.LazyPrintf("context timed out while transfering leadership")
						}
					case <-time.After(1 * time.Second):
						if tr, ok := trace.FromContext(n.ctx); ok {
							tr.LazyPrintf("Timed out transfering leadership")
						}
					}
					n.Raft().Stop()
					close(n.done)
				}()
			} else {
				n.Raft().Stop()
				close(n.done)
			}
		case <-n.done:
			return
		}
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
	case <-n.done:
		// already stopped.
		return
	}
	<-n.done // wait for Run to respond.
}

func (n *node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.Raft().Step(ctx, msg)
}

func (n *node) snapshotPeriodically() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.snapshot(config.MaxPendingCount)

		case <-n.done:
			return
		}
	}
}

func (n *node) snapshot(skip uint64) {
	/*
		water := posting.SyncMarkFor(n.gid)
		le := water.DoneUntil()

		existing, err := n.store.Snapshot()
		helper.Checkf(err, "Unable to get existing snapshot")

		si := existing.Metadata.Index
		if le <= si+skip {
			return
		}
		snapshotIdx := le - skip
		if tr, ok := trace.FromContext(n.ctx); ok {
			tr.LazyPrintf("Taking snapshot for group: %d at watermark: %d\n", n.gid, snapshotIdx)
		}
		rc, err := n.raftContext.Marshal()
		helper.Check(err)

		s, err := n.store.CreateSnapshot(snapshotIdx, n.ConfState(), rc)
		helper.Checkf(err, "While creating snapshot")
		helper.Checkf(n.store.Compact(snapshotIdx), "While compacting snapshot")
		helper.Check(n.wal.StoreSnapshot(n.gid, s))
	*/
}

func (n *node) joinPeers() {
	/*
		var id int
		var addr string
		for id, addr = range n.peersAddr {
			if uint64(id+1) != n.id {
				break
			}
		}
		if id == len(n.peersAddr) {
			log.Fatal("Unable to get id to add the node")
		}
		id = id + 1
		n.Connect(uint64(id), addr)
		helper.Logger.Printf(10, "joinPeers connected with: %q with peer id: %d\n", addr, id)

		pool, err := pools().get(addr)
		if err != nil {
			log.Fatalf("Unable to get pool for addr: %q for peer: %d, error: %v\n", addr, id, err)
		}
		defer pools().release(pool)

		// Bring the instance up to speed first.
		// Raft would decide whether snapshot needs to fetched or not
		// so populateShard is not needed
		// _, err := populateShard(n.ctx, pool, n.gid)
		// helper.Checkf(err, "Error while populating shard")

		conn := pool.Get()

		c := protos.NewRaftNodeClient(conn)
		helper.Logger.Printf(10, "Calling JoinCluster")
		_, err = c.JoinCluster(n.ctx, n.raftContext)
		helper.Checkf(err, "Error while joining cluster")
		helper.Logger.Printf(10, "Done with JoinCluster call\n")
	*/
}

func (n *node) initFromWal(wal *raftwal.Wal) (restart bool, rerr error) {
	n.wal = wal

	var sp raftpb.Snapshot
	sp, rerr = wal.Snapshot(n.gid)
	if rerr != nil {
		return
	}
	var term, idx uint64
	if !raft.IsEmptySnap(sp) {
		helper.Logger.Printf(10, "Found Snapshot: %+v\n", sp)
		restart = true
		if rerr = n.store.ApplySnapshot(sp); rerr != nil {
			return
		}
		term = sp.Metadata.Term
		idx = sp.Metadata.Index
	}

	var hd raftpb.HardState
	hd, rerr = wal.HardState(n.gid)
	if rerr != nil {
		return
	}
	if !raft.IsEmptyHardState(hd) {
		helper.Logger.Printf(10, "Found hardstate: %+v\n", sp)
		restart = true
		if rerr = n.store.SetHardState(hd); rerr != nil {
			return
		}
	}

	var es []raftpb.Entry
	es, rerr = wal.Entries(n.gid, term, idx)
	if rerr != nil {
		return
	}
	helper.Logger.Printf(10, "Group %d found %d entries\n", n.gid, len(es))
	if len(es) > 0 {
		restart = true
	}
	rerr = n.store.Append(es)
	return
}

// InitAndStartNode gets called after having at least one membership sync with the cluster.
func (n *node) InitAndStartNode(wal *raftwal.Wal, grpcSrv *grpc.Server) {
	n.transport = transport.InitRaftTransport(n.id, n, n.peersAddr)
	n.transport.Start(grpcSrv)

	restart, err := n.initFromWal(wal)
	helper.Check(err)

	rpeers := make([]raft.Peer, len(n.peersAddr))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	if restart {
		helper.Logger.Printf(10, "Restarting node for cluster")
		n.SetRaft(raft.RestartNode(n.cfg))

	} else {
		helper.Logger.Printf(10, "New Node for cluster")
		if config.JoinMon {
			n.joinPeers()
			rpeers = nil
		}
		//peers := []raft.Peer{{ID: n.id}}
		n.SetRaft(raft.StartNode(n.cfg, rpeers))
		// Trigger election, so this node can become the leader of this single-node cluster.
		//n.canCampaign = true
	}
	go n.processApplyCh()
	go n.Run()
	// TODO: Find a better way to snapshot, so we don't lose the membership
	// state information, which isn't persisted.
	go n.snapshotPeriodically()
}

func (n *node) AmLeader() bool {
	if n.Raft() == nil {
		return false
	}
	r := n.Raft()
	return r.Status().Lead == r.Status().ID
}

func (n *node) Process(ctx context.Context, m raftpb.Message) error {
	return n._raft.Step(ctx, m)
}
func (n *node) IsIDRemoved(id uint64) bool                           { return false }
func (n *node) ReportUnreachable(id uint64)                          {}
func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
