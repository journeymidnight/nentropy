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
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/mon/raftwal"
	"github.com/journeymidnight/nentropy/protos"
	"google.golang.org/grpc"
)

const (
	DATA_TYPE_OSD_MAP  uint32 = 1
	DATA_TYPE_POOL_MAP uint32 = 2
	DATA_TYPE_PG_MAPS  uint32 = 3
)

type cluster struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	wal                  *raftwal.Wal
	node                 *node
	myAddr               string
	isPrimaryMon         bool
	primaryPgLocationMap map[string]int32
	internalMapLock      *sync.Mutex
	osdMap               protos.OsdMap
	poolMap              protos.PoolMap
	pgMaps               protos.PgMaps
	monMap               protos.MonMap
	mapLock              *sync.Mutex
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
	key := fmt.Sprintf("%v", epoch)
	t.Ops = append(t.Ops, &protos.Op{Type: protos.Op_OP_PUT, Prefix: prefix, Key: key, Data: data})
	t.Keys++
	t.Bytes += uint64(len(prefix) + len(key) + len(data))
	return nil
}

func syncPgMapsToEachOsd(addr string) {
	pgmaps := clus.pgMaps
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		helper.Logger.Println(5, "fail to dial: %v", err)
	}
	defer conn.Close()
	client := protos.NewOsdRpcClient(conn)
	req := protos.SyncMapRequest{}
	req.MapType = protos.PGMAP
	req.UnionMap.SetValue(pgmaps)
	ctx := context.Background()
	res, err := client.SyncMap(ctx, &req)
	if err != nil {
		helper.Logger.Println(5, "Error send SyncMap rpc request", err)
		return
	}
	helper.Logger.Println(5, "Finished! The syncPgMaps response is %s!", res)

}

func syncPgMaps() {
	osdmap := clus.osdMap
	for _, v := range osdmap.MemberList {
		if v.Up == false || v.In == false {
			continue
		}
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
	for _, v := range trans.Ops {
		if v.Type == protos.Op_OP_PUT {
			if v.Prefix == "osdmap" {
				var osdMap protos.OsdMap
				if err := osdMap.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.osdMap = osdMap
				helper.Logger.Println(5, "New osdmap committed, member in osdmap :")
				if clus.osdMap.MemberList != nil {
					for id, _ := range clus.osdMap.MemberList {
						helper.Logger.Println(5, "OSD Member Id:", id)
					}
				}

			} else if v.Prefix == "poolmap" {
				var poolMap protos.PoolMap
				if err := poolMap.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.poolMap = poolMap

			} else if v.Prefix == "pgmap" {
				var pgMaps protos.PgMaps
				if err := pgMaps.Unmarshal(v.Data); err != nil {
					helper.Check(err)
				}
				clus.pgMaps = pgMaps
				go syncPgMaps()

			} else {
				helper.Errorf("Unknown data type!")
			}
		} else {
			helper.Errorf("UnSupport data type!")
		}
	}

	return nil
}

// StartRaftNodes will read the WAL dir, create the RAFT cluster,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(walStore *badger.DB) {
	clus = new(cluster)
	clus.ctx, clus.cancel = context.WithCancel(context.Background())

	clus.wal = raftwal.Init(walStore, Config.RaftId)
	clus.mapLock = &sync.Mutex{}
	var wg sync.WaitGroup

	mons := strings.Split(Config.Monitors, ",")
	for i, v := range mons {
		if uint64(i+1) == Config.RaftId {
			clus.myAddr = v
		}
	}
	node := newNode(Config.RaftId, clus.myAddr)
	if clus.node != nil {
		helper.AssertTruef(false, "Didn't expect a node in RAFT group mapping: %v", 0)
	}
	node.SetCommittedMsgHandler(handleCommittedMsg)
	clus.node = node

	node.peersAddr = mons
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.InitAndStartNode(clus.wal)
	}()

	wg.Wait()
}

// BlockingStop stops all the nodes, server between other workers and syncs all marks.
func BlockingStop() {
	clus.Node().Stop() // blocking stop all nodes
	StopServer()
	// blocking sync all marks
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
}

type ProposedData struct {
	Type uint32
	Data []byte
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
	data, err := poolMap.Marshal()
	helper.Check(err)
	err = putOp(trans, "poolmap", poolMap.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func PreparePgMap(trans *protos.Transaction, pgMaps *protos.PgMaps) error {
	data, err := pgMaps.Marshal()
	helper.Check(err)
	err = putOp(trans, "pgmap", pgMaps.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func PrepareOsdMap(trans *protos.Transaction, osdMap *protos.OsdMap) error {
	data, err := osdMap.Marshal()
	helper.Check(err)
	err = putOp(trans, "osdmap", osdMap.Epoch, data)
	if err != nil {
		return err
	}
	return nil
}

func ProposePoolMap(poolMap *protos.PoolMap) error {
	data, err := poolMap.Marshal()
	helper.Check(err)
	trans := protos.Transaction{}
	err = putOp(&trans, "poolmap", poolMap.Epoch, data)
	if err != nil {
		return err
	}
	proposeData(&trans)
	return nil
}

func GetCurrPoolMap() (protos.PoolMap, error) {
	return clus.poolMap, nil
}

func ProposePgMaps(pgMaps *protos.PgMaps) error {
	data, err := pgMaps.Marshal()
	helper.Check(err)
	trans := protos.Transaction{}
	err = putOp(&trans, "pgmap", pgMaps.Epoch, data)
	if err != nil {
		return err
	}
	proposeData(&trans)
	return nil
}

func GetCurrPgMaps() (protos.PgMaps, error) {
	return clus.pgMaps, nil
}

func NotifyMemberEvent(eventType memberlist.MemberEventType, member memberlist.Member) error {
	helper.Logger.Println(5, "Call NotifyMemberEvent()")
	if !clus.node.AmLeader() {
		return nil
	}
	if member.IsMon {
		return nil
	}
	var exist bool
	if _, ok := clus.osdMap.MemberList[int32(member.ID)]; ok {
		exist = true
	}
	if clus.osdMap.MemberList == nil {
		clus.osdMap.MemberList = make(map[int32]*protos.Osd)
	}
	helper.Logger.Println(5, "Before update osdmap, member in osdmap :")
	if clus.osdMap.MemberList != nil {
		for k, _ := range clus.osdMap.MemberList {
			helper.Logger.Println(5, "OSD Member Id:", k)
		}
	}

	if eventType == memberlist.MemberJoin {
		if exist {
			return nil
		}
		osdMap := protos.OsdMap{}
		data, err := clus.osdMap.Marshal()
		if err != nil {
			helper.Logger.Println(5, "Eorror marshal osdmap!")
			return err
		}
		err = osdMap.Unmarshal(data)
		if err != nil {
			helper.Logger.Println(5, "Eorror unmarshal osdmap!")
			return err
		}
		osdMap.Epoch++
		if osdMap.MemberList == nil {
			osdMap.MemberList = make(map[int32]*protos.Osd)
		}
		osdMap.MemberList[int32(member.ID)] = &protos.Osd{Id: int32(member.ID)}
		helper.Logger.Println(5, "New member added! id:", member.ID)
		ProposeOsdMap(&osdMap)

	} else if eventType == memberlist.MemberLeave {
		if !exist {
			return nil
		}
		osdMap := protos.OsdMap{}
		data, err := clus.osdMap.Marshal()
		if err != nil {
			helper.Logger.Println(5, "Eorror marshal osdmap!")
			return err
		}
		err = osdMap.Unmarshal(data)
		if err != nil {
			helper.Logger.Println(5, "Eorror unmarshal osdmap!")
			return err
		}
		osdMap.Epoch++
		delete(osdMap.MemberList, int32(member.ID))
		helper.Logger.Println(0, "New member leave! id:", member.ID)
		ProposeOsdMap(&osdMap)

	} else {

	}
	return nil
}
