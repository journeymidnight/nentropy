package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"math"
	"strconv"
	"strings"
)

func (s *OsdServer) CreatePg(ctx context.Context, in *protos.CreatePgRequest) (*protos.CreatePgReply, error) {
	err := s.store.BootstrapGroup(nil, in.GroupDescriptor)
	return &protos.CreatePgReply{}, err
}

func (s *OsdServer) DeletePg(ctx context.Context, in *protos.DeletePgRequest) (*protos.DeletePgReply, error) {
	return &protos.DeletePgReply{}, nil
}

func (s *OsdServer) createOrRemoveReplica() {
	s.confChangeLock.Lock()
	defer s.confChangeLock.Unlock()
	if s.pgMaps.Epoch <= s.lastPgMapsEpoch {
		return
	}
	//TODO: gc/remove replicas

	//add new replicas
	for poolId, pgMap := range s.pgMaps.Pgmaps {
		for pgId, pg := range pgMap.Pgmap {
			for _, replica := range pg.Replicas {
				if replica.OsdId != int32(s.cfg.NodeID) {
					continue
				}
				_, err := s.store.GetReplica(multiraftbase.GroupID(fmt.Sprintf("%d.%d", poolId, pgId)))
				if err == nil {
					//replica already existed
					break
				}
				groupDes := multiraftbase.GroupDescriptor{}
				groupDes.PoolId = int64(poolId)
				groupDes.PgId = int64(pgId)
				groupDes.GroupID = multiraftbase.GroupID(fmt.Sprintf("%d.%d", poolId, pgId))
				for _, subReplica := range pg.Replicas {
					groupDes.Replicas = append(groupDes.Replicas, multiraftbase.ReplicaDescriptor{multiraftbase.NodeID(fmt.Sprintf("osd.%d", subReplica.OsdId)), 0, multiraftbase.ReplicaID(subReplica.ReplicaIndex)})
				}
				groupDes.NextReplicaID = multiraftbase.ReplicaID(pg.NextReplicaId)
				go s.store.BootstrapGroup(nil, &groupDes)
				helper.Println(5, fmt.Sprintf("try start a new replica :%d.%d", poolId, pgId))
				break
			}
		}

	}
	s.lastPgMapsEpoch = s.pgMaps.Epoch
	return

}

func (s *OsdServer) SyncMap(ctx context.Context, in *protos.SyncMapRequest) (*protos.SyncMapReply, error) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()
	switch in.MapType {
	case protos.PGMAP:
		newPgMaps := in.UnionMap.GetPgmap()
		helper.Println(5, "unionmap0", in.UnionMap)
		helper.Println(5, "unionmap1", newPgMaps)
		helper.Println(5, "wich one will panic 0", newPgMaps.Epoch)

		if newPgMaps.Epoch > s.pgMaps.Epoch {
			s.pgMaps = newPgMaps
			go s.createOrRemoveReplica()
			helper.Println(5, "recevie sync map request, epoch:", newPgMaps.Epoch)
		} else {
			helper.Println(5, "recevie expire sync map request, epoch:", newPgMaps.Epoch)
			return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie expire sync map request, new epoch: %v, current epoch: %v", newPgMaps.Epoch, s.pgMaps.Epoch))
		}

	case protos.OSDMAP:
		return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie unsupport sync map request which type is %v", protos.OSDMAP))
	case protos.POOLMAP:
		return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie unsupport sync map request which type is %v", protos.POOLMAP))
	}

	return &protos.SyncMapReply{}, nil
}

func (s *OsdServer) MigrateGet(ctx context.Context, in *protos.MigrateGetRequest) (*protos.MigrateGetReply, error) {
	engine := s.store.LoadGroupEngine(multiraftbase.GroupID(in.ParentPgId))
	it := engine.NewIterator()
	poolId, _ := strconv.Atoi(strings.Split(in.ChildPgId, ".")[0])
	childPgId, _ := strconv.Atoi(strings.Split(in.ChildPgId, ".")[1])
	pgNumbers := len(s.pgMaps.Pgmaps[int32(poolId)].Pgmap)
	mask := protos.Calc_pg_masks(int(pgNumbers))
	if in.Marker != nil {
		//TODO: CLEAR OBJECT WHICH HAS ALREADY BE MIGRATED
	}
	if in.FlagNext == false {
		value, err := StripeRead(engine, in.Marker, 0, math.MaxUint32)
		if err != nil {
			helper.Println(5, "print err when migrate key :", in.Marker, string(in.Marker), err)
			return &protos.MigrateGetReply{}, err
		} else {
			return &protos.MigrateGetReply{in.Marker, value, in.Marker}, nil
		}
	}
	for it.Seek(in.Marker); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		helper.Println(5, "print migrate iter key :", key, string(key))
		if bytes.Equal(key, in.Marker) {
			continue
		} else {
			// exclude all localPrefixByte
			if !bytes.HasPrefix(key, []byte{'\x02'}) {
				continue
			}
			helper.Println(5, "print useful migrate key :", key)
			oid := bytes.TrimPrefix(key, []byte{'\x02'})
			if string(oid) == "system_pg_state" {
				continue
			}
			hash := protos.Nentropy_str_hash(string(oid))
			hashPgId := protos.Nentropy_stable_mod(int(hash), int(pgNumbers), mask)
			if hashPgId == childPgId {
				//value, err := item.Value()
				//var onode multiraft.Onode
				//bson.Unmarshal(value, onode)
				value, err := StripeRead(engine, oid, 0, math.MaxUint32)
				if err != nil {
					helper.Println(5, "print err when migrate key :", oid, string(oid), err)
					return &protos.MigrateGetReply{}, err
				} else {
					return &protos.MigrateGetReply{oid, value, key}, nil
				}
			} else {
				continue
			}
		}
	}
	return &protos.MigrateGetReply{}, nil
}

//func newServer() *osdRpcServer {
//	s := new(osdRpcServer)
//	return s
//}
//
//func runServer() {
//	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 0))
//	if err != nil {
//		logger.Fatalf(5, "failed to listen: %v", err)
//	}
//	helper.Println(5, "Using osd rpc port:", lis.Addr().(*net.TCPAddr).Port)
//	var opts []grpc.ServerOption
//	osdServer = grpc.NewServer(opts...)
//	protos.RegisterOsdRpcServer(osdServer, newServer())
//	go osdServer.Serve(lis)
//}
