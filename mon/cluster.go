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
	"sync"

	"golang.org/x/net/context"

	"fmt"
	"time"

	"bytes"
	"encoding/binary"
	"errors"
	"github.com/coreos/etcd/raft"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/mon/raftwal"
	"github.com/journeymidnight/nentropy/mon/transport"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/idutil"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/wait"
	"google.golang.org/grpc"
)

const (
	DATA_TYPE_OSD_MAP  uint32 = 1
	DATA_TYPE_POOL_MAP uint32 = 2
	DATA_TYPE_PG_MAPS  uint32 = 3
)

type cluster struct {
	ctx             context.Context
	cancel          context.CancelFunc
	wal             *raftwal.Wal
	node            *node
	myAddr          string
	isPrimaryMon    bool
	PgStatusMap     map[string]protos.PgStatus
	internalMapLock *sync.Mutex //for PgStatusMap
	osdMap          protos.OsdMap
	poolMap         protos.PoolMap
	pgMaps          protos.PgMaps
	monMap          protos.MonMap
	mapLock         *sync.Mutex

	eng engine.Engine

	reqIDGen  *idutil.Generator
	readwaitc chan struct{}
	// stopping is closed by run goroutine on shutdown.
	stopping chan struct{}

	readMu syncutil.RWMutex
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	readNotifier *helper.Notifier

	// a chan to send out readState
	readStateC chan raft.ReadState
	applyWait  wait.WaitTime
	// done is closed when all goroutines from start() complete.
	done chan struct{}
}

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

func putOp(t *protos.Transaction, prefix string, epoch uint64, data []byte) error {
	key := fmt.Sprintf("%s.%v", prefix, epoch)
	t.Ops = append(t.Ops, &protos.Op{Type: protos.Op_OP_PUT, Prefix: prefix, Key: key, Data: data})
	t.Keys++
	t.Bytes += uint64(len(prefix) + len(key) + len(data))
	return nil
}

func syncPgMapsToEachOsd(addr string) {
	pgmaps := clus.pgMaps
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		helper.Println(5, "fail to dial: %v", err)
	}
	defer conn.Close()
	client := protos.NewOsdRpcClient(conn)
	req := protos.SyncMapRequest{}
	req.MapType = protos.PGMAP
	req.UnionMap.Reset()
	setSuccess := req.UnionMap.SetValue(&pgmaps)
	if setSuccess != true {
		helper.Println(5, "Error send SyncMap rpc request, internal error")
		return
	}

	ctx := context.Background()
	res, err := client.SyncMap(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send SyncMap rpc request", err)
		return
	}
	helper.Println(5, "Finished! The syncPgMaps response is %s!", res)

}

func syncPgMaps() {
	osdmap := clus.osdMap
	for _, v := range osdmap.MemberList {
		if v.Up == false || v.In == false {
			continue
		}
		helper.Println(5, "call syncPgMaps to :", v.Addr)
		go syncPgMapsToEachOsd(v.Addr)
	}
}

func handleCommittedMsg(data []byte) error {
	if data == nil {
		return nil
	}
	trans := protos.Transaction{}
	if err := trans.Unmarshal(data); err != nil {
		helper.Check(err)
	}
	batch := clus.eng.NewBatch(true)
	defer batch.Close()
	for _, v := range trans.Ops {
		if v.Type == protos.Op_OP_PUT {
			if v.Prefix == "osdmap" {
				var osdMap protos.OsdMap
				if err := osdMap.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.osdMap = osdMap
				helper.Println(5, "New osdmap committed, member in osdmap :")
				if clus.osdMap.MemberList != nil {
					for id, _ := range clus.osdMap.MemberList {
						helper.Println(5, "OSD Member Id:", id)
					}
				}
				err := batch.Put([]byte(v.Key), v.Data)
				helper.Printf(5, "Put data %s\n", v.Key)
				if err != nil {
					helper.Check(err)
				}

			} else if v.Prefix == "poolmap" {
				var poolMap protos.PoolMap
				if err := poolMap.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.poolMap = poolMap
				err := batch.Put([]byte(v.Key), v.Data)
				helper.Printf(5, "Put data %s\n", v.Key)
				if err != nil {
					helper.Check(err)
				}

			} else if v.Prefix == "pgmap" {
				var pgMaps protos.PgMaps
				if err := pgMaps.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.pgMaps = pgMaps
				if clus.node.AmLeader() {
					go syncPgMaps()
				}
				err := batch.Put([]byte(v.Key), v.Data)
				helper.Printf(5, "Put data %s\n", v.Key)
				if err != nil {
					helper.Check(err)
				}
			} else {
				helper.Check(errors.New("Unknown data type!"))
			}
		} else {
			helper.Check(errors.New("Unknown data type!"))
		}
	}
	batch.Commit()

	return nil
}

// StartRaftNodes will read the WAL dir, create the RAFT cluster,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(eng engine.Engine, grpcSrv *grpc.Server, peers []string, myAddr string) {
	clus = new(cluster)
	clus.eng = eng
	clus.ctx, clus.cancel = context.WithCancel(context.Background())

	clus.wal = raftwal.Init(eng, config.RaftId)
	clus.mapLock = &sync.Mutex{}
	clus.internalMapLock = &sync.Mutex{}
	clus.PgStatusMap = make(map[string]protos.PgStatus)
	var wg sync.WaitGroup

	clus.myAddr = myAddr
	node := newNode(config.RaftId, myAddr)
	if clus.node != nil {
		helper.AssertTruef(false, "Didn't expect a node in RAFT group mapping: %v", 0)
	}
	node.SetCommittedMsgHandler(handleCommittedMsg)
	clus.node = node

	node.peersAddr = make(map[uint64]string)
	for k, v := range peers {
		node.peersAddr[uint64(k+1)] = v
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.InitAndStartNode(clus.wal, grpcSrv)
	}()

	wg.Wait()

	clus.reqIDGen = idutil.NewGenerator(0, time.Now())
	clus.applyWait = wait.NewTimeList()
	clus.readStateC = make(chan raft.ReadState, 1)
	clus.readNotifier = helper.NewNotifier()
	clus.readwaitc = make(chan struct{}, 1)

	//go clus.linearizableReadLoop()
}

// BlockingStop stops all the nodes, server between other workers and syncs all marks.
func BlockingStop() {
	clus.Node().Stop() // blocking stop all nodes
	transport.StopServer()
	// blocking sync all marks
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
}

func readData(key []byte) ([]byte, error) {
	return clus.eng.Get(key)
}

func proposeData(t *protos.Transaction) error {
	data, err := t.Marshal()
	helper.Check(err)
	proposal := protos.Proposal{Data: data}
	clus.Node().ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func ProposeOsdMap(osdMap *protos.OsdMap) error {
	data, err := osdMap.Marshal()
	helper.Check(err)
	trans := protos.Transaction{}
	err = putOp(&trans, "osdmap", osdMap.Epoch, data)
	if err != nil {
		return err
	}
	proposeData(&trans)
	return nil
}

func GetCurrOsdMap() (protos.OsdMap, error) {
	return clus.osdMap, nil
}

func PreparePoolMap(trans *protos.Transaction, poolMap *protos.PoolMap) error {
	poolMap.Epoch++
	data, err := poolMap.Marshal()
	helper.Check(err)
	err = putOp(trans, "poolmap", poolMap.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func PreparePgMap(trans *protos.Transaction, pgMaps *protos.PgMaps) error {
	pgMaps.Epoch++
	data, err := pgMaps.Marshal()
	helper.Check(err)
	err = putOp(trans, "pgmap", pgMaps.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func PrepareOsdMap(trans *protos.Transaction, osdMap *protos.OsdMap) error {
	osdMap.Epoch++
	data, err := osdMap.Marshal()
	helper.Check(err)
	err = putOp(trans, "osdmap", osdMap.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func GetCurrPoolMap() (protos.PoolMap, error) {
	return clus.poolMap, nil
}

func GetCurrPgMaps(epoch uint64) (*protos.PgMaps, error) {
	if epoch == 0 {
		return &clus.pgMaps, nil
	}

	key := fmt.Sprintf("pgmap.%d", epoch)
	data, err := readData([]byte(key))
	if err != nil {
		return nil, err
	}
	pgmaps := protos.PgMaps{}
	err = pgmaps.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &pgmaps, nil
}

func NotifyMemberEvent(eventType memberlist.MemberEventType, member memberlist.Member) error {
	helper.Println(5, "Call NotifyMemberEvent()")
	if !clus.node.AmLeader() {
		return nil
	}
	if member.IsMon {
		return nil
	}

	if _, ok := clus.osdMap.MemberList[int32(member.ID)]; !ok {
		return nil
	}

	if clus.osdMap.MemberList == nil {
		clus.osdMap.MemberList = make(map[int32]*protos.Osd)
	}
	helper.Println(5, "Before update osdmap, member in osdmap :")
	if clus.osdMap.MemberList != nil {
		for k, _ := range clus.osdMap.MemberList {
			helper.Println(5, "OSD Member Id:", k)
		}
	}

	if eventType == memberlist.MemberJoin {
		osdMap := protos.OsdMap{}
		data, err := clus.osdMap.Marshal()
		if err != nil {
			helper.Println(5, "Eorror marshal osdmap!")
			return err
		}
		err = osdMap.Unmarshal(data)
		if err != nil {
			helper.Println(5, "Eorror unmarshal osdmap!")
			return err
		}
		osdMap.Epoch++
		osdMap.MemberList[int32(member.ID)].Up = true
		osdMap.MemberList[int32(member.ID)].Addr = member.Addr
		helper.Println(5, "Osd info updated! id:", member.ID)
		ProposeOsdMap(&osdMap)

	} else if eventType == memberlist.MemberLeave {
		osdMap := protos.OsdMap{}
		data, err := clus.osdMap.Marshal()
		if err != nil {
			helper.Println(5, "Eorror marshal osdmap!")
			return err
		}
		err = osdMap.Unmarshal(data)
		if err != nil {
			helper.Println(5, "Eorror unmarshal osdmap!")
			return err
		}
		osdMap.Epoch++
		osdMap.MemberList[int32(member.ID)].Up = false
		helper.Println(0, "Osd info updated! id:", member.ID)
		ProposeOsdMap(&osdMap)

	} else {

	}
	return nil
}

func (c *cluster) linearizableReadLoop(ctx context.Context) {
	var rs raft.ReadState

	for {
		rctx := make([]byte, 8)
		binary.BigEndian.PutUint64(rctx, c.reqIDGen.Next())

		select {
		case <-c.readwaitc:
		case <-c.stopping:
			return
		}
		nextnr := helper.NewNotifier()

		c.readMu.Lock()
		nr := c.readNotifier
		c.readNotifier = nextnr
		c.readMu.Unlock()

		c.node._raft.ReadIndex(ctx, rctx)

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {
			select {
			case rs = <-c.readStateC:
				done = bytes.Equal(rs.RequestCtx, rctx)
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					helper.Printf(5, "ignored out-of-date read index response (want %v, got %v)", rs.RequestCtx, ctx)
				}
			case <-time.After(5 * time.Second):
				helper.Printf(5, "timed out waiting for read index response")
				nr.Notify(errors.New("netropy: request timed out"))
				timeout = true
			case <-c.stopping:
				return
			}
		}
		if !done {
			continue
		}
		/*
			if ai := c.mu.state.RaftAppliedIndex; ai < rs.Index {
				select {
				case <-c.applyWait.Wait(rs.Index):
				case <-c.stopping:
					return
				}
			}
		*/
		// unblock all l-reads requested at indices before rs.Index
		nr.Notify(nil)
	}
}

func (c *cluster) linearizableReadNotify(ctx context.Context) error {
	c.readMu.RLock()
	nc := c.readNotifier
	c.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case c.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.GetChan():
		return nc.GetErr()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("netropy: server stopped")
	}
}
