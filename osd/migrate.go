package main

import (
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
)

type statics struct {
	parent string
	count  int64
	marker []byte
	state  int32 //0 = started ,1 = quit with error, 2 = complete
}

type migrateCenter struct {
	tasks sync.Map //map<string, st statics>
}

func (m *migrateCenter) addTask(parent, child string) {
	m.tasks.Store(child, &statics{parent, 0, nil, 0})
	go m.processMigrateTask(child)
}

func (m *migrateCenter) getStatics(child string) statics {
	value, _ := m.tasks.Load(child)
	return *(value.(*statics))
}

func (m *migrateCenter) setStatics(child string, st statics) {
	m.tasks.Store(child, &st)
}

func (m *migrateCenter) processMigrateTask(child string) {
	st := m.getStatics(child)
	mon := memberlist.GetLeaderMon()
	if mon == nil {
		helper.Println(5, "can not get primary mon addr yet!")
		return
	}
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		helper.Println(5, "fail to dial: %v, when try connect to mon", err)
		return
	}
	defer conn.Close()
	helper.Println(5, "migrate start 1", child, st.parent)
	client_mon := protos.NewMonitorClient(conn)
	req := protos.GetPgStatusRequest{}
	req.PgId = st.parent
	ctx := context.Background()
	res, err := client_mon.GetPgStatus(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send rpc request!")
		return
	}
	helper.Println(5, "migrate start 2", child)
	nodeId := res.Status.LeaderNodeId
	member := memberlist.GetMemberByName(fmt.Sprintf("osd.%d", nodeId))
	if member == nil {
		helper.Println(5, "Exec GetMemberByName failed! nodeId:", nodeId)
		return
	}
	helper.Println(5, "migrate start 3", child)
	conn_osd, err := grpc.Dial(member.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		helper.Println(5, "fail to dial: %v, when try connect to mon", err)
		return
	}
	defer conn_osd.Close()
	helper.Println(5, "migrate start 4", child)
	client_osd := protos.NewOsdRpcClient(conn_osd)
	for {
		req := protos.MigrateGetRequest{}

		req.Marker = st.marker
		req.ParentPgId = st.parent
		req.ChildPgId = child
		req.FlagNext = true
		ctx := context.Background()
		helper.Println(5, "try migrated next object from old pg", req.Marker, req.ParentPgId, req.ChildPgId)
		res, err := client_osd.MigrateGet(ctx, &req)
		if err != nil {
			helper.Println(5, "Error send rpc request!", err)
			return
		}
		if len(res.Key) > 0 {
			//TODO:check if this key already exist
			b := &client.Batch{}
			b.Header.GroupID = multiraftbase.GroupID(child)
			b.AddRawRequest(&multiraftbase.PutRequest{
				Key:   multiraftbase.Key(res.Key),
				Value: multiraftbase.Value{Offset: 0, Len: uint64(len(res.Value)), RawBytes: res.Value},
			})
			if err := Server.store.Db.Run(context.Background(), b); err != nil {
				helper.Printf(5, "Error run batch! try put migrated kv failed")
				return
			}
			helper.Printf(5, "migrated object to pg:%s from pg:%s, marker=%s, key=%s", child, st.parent, st.marker, res.Key)
			helper.Println(5, "migrated object to pg", st.marker, res.Key)
			st.count++
			st.marker = res.Marker
			m.setStatics(child, st)
			UpdatePgStatusMap(child, int32(protos.PG_STATE_ACTIVE|protos.PG_STATE_MIGRATING), st.count)
		} else {
			//change pg state
			newState := protos.PG_STATE_ACTIVE | protos.PG_STATE_CLEAN
			err = Server.SetPgState(child, int32(newState))
			if err != nil {
				helper.Printf(5, "failed set pg state for pg:", child, err)
				return
			}
			UpdatePgStatusMap(child, int32(newState), st.count)
			helper.Println(5, "finish migrate task for pg!", child)
			break
		}
	}
	helper.Println(5, "migrate start 5", child)
}
