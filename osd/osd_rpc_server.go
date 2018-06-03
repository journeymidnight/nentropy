package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"strconv"
	"strings"
)

func (s *OsdServer) CreatePg(ctx context.Context, in *protos.CreatePgRequest) (*protos.CreatePgReply, error) {
	err := s.store.BootstrapGroup(false, in.GroupDescriptor)
	return &protos.CreatePgReply{}, err
}

func (s *OsdServer) DeletePg(ctx context.Context, in *protos.DeletePgRequest) (*protos.DeletePgReply, error) {
	return &protos.DeletePgReply{}, nil
}

func proposeConfChange(confType multiraftbase.ConfType, groupId multiraftbase.GroupID, reps []protos.PgReplica) error {
	for _, rep := range reps {
		b := &client.Batch{}
		b.Header.GroupID = groupId
		b.AddRawRequest(&multiraftbase.ChangeConfRequest{
			ConfType:  confType,
			OsdId:     rep.OsdId,
			ReplicaId: rep.ReplicaIndex})
		if err := Server.store.Db.Run(context.Background(), b); err != nil {
			helper.Println(5, "Error run batch! failed to propose ChangeConfRequest", err)
			return err
		}
		helper.Println(5, "proposeConfChange() groupId:", string(groupId), " add replica ", rep.ReplicaIndex, " osd ", rep.OsdId, " ConfType:", int32(confType))
	}
	return nil
}

func isExist(osdId int32, replicas []protos.PgReplica) bool {
	for _, replica := range replicas {
		if replica.OsdId == int32(osdId) {
			return true
		}
	}
	return false
}

func (s *OsdServer) createOrRemoveReplica(pgMaps *protos.PgMaps) {
	s.confChangeLock.Lock()
	defer s.confChangeLock.Unlock()
	if s.pgMaps.Epoch <= s.lastPgMapsEpoch {
		return
	}

	for poolId, pgMap := range s.pgMaps.Pgmaps {
		for pgId, pg := range pgMap.Pgmap {
			groupID := multiraftbase.GroupID(fmt.Sprintf("%d.%d", poolId, pgId))
			var err error
			var replicas []protos.PgReplica
			replicas, err = s.GetPgMember(string(groupID))
			if err != nil {
				helper.Check(err)
			}
			if replicas == nil {
				replicas = make([]protos.PgReplica, 0)
			}
			helper.Println(5, "Before merge:", " groupID:", groupID)
			for _, replica := range replicas {
				helper.Printf(5, " replica: %s  ", replica.String())
			}

			if !isExist(int32(s.cfg.NodeID), pg.Replicas) && !isExist(int32(s.cfg.NodeID), replicas) {
				continue
			}

			var newReplicas []protos.PgReplica
			for _, rep := range pg.Replicas {
				var exist bool
				for _, replica := range replicas {
					if replica.ReplicaIndex == rep.ReplicaIndex && replica.OsdId == rep.OsdId {
						exist = true
					}
				}
				if !exist {
					replicas = append(replicas, rep)
					newReplicas = append(newReplicas, rep)
				}
			}
			helper.Println(5, "After merge:", " groupID:", groupID)
			for _, replica := range replicas {
				helper.Printf(5, " replica: %s  ", replica.String())
			}
			s.saveReplicasLocally(replicas, string(groupID))

			rep, err := s.store.GetReplica(groupID)
			if err == nil {
				//replica already existed
				var exist bool
				for _, subReplica := range replicas {
					if subReplica.ReplicaIndex == int32(rep.mu.replicaID) {
						exist = true
						break
					}
				}

				if exist {
					if rep.amLeader() {
						err := proposeConfChange(multiraftbase.ConfType_ADD_REPLICA, groupID, newReplicas)
						if err != nil {
							helper.Check(err)
						}
					}
					continue
				} else {
					// remove old replica from the pg
					helper.Println(5, "Remove replica, group:", string(groupID), " replicaId:", rep.mu.replicaID)
					if rep.mu.destroyed != nil {
						s.store.DelReplica(groupID)
						s.store.DelDBEngine(groupID)
						s.store.removeReplicaWorkDir(groupID)
					}
				}
			}

			groupDes := multiraftbase.GroupDescriptor{}
			groupDes.PoolId = int64(poolId)
			groupDes.PgId = int64(pgId)
			groupDes.GroupID = groupID
			for _, subReplica := range replicas {
				nodeId := fmt.Sprintf("osd.%d", subReplica.OsdId)
				groupDes.Replicas = append(groupDes.Replicas, multiraftbase.ReplicaDescriptor{multiraftbase.NodeID(nodeId), 0, multiraftbase.ReplicaID(subReplica.ReplicaIndex)})
			}
			groupDes.NextReplicaID = multiraftbase.ReplicaID(pg.NextReplicaId)
			helper.Println(5, fmt.Sprintf("try start a new replica :%d.%d", poolId, pgId))
			go s.store.BootstrapGroup(false, &groupDes)
		}
	}

	s.lastPgMapsEpoch = s.pgMaps.Epoch
	return
}

func (s *OsdServer) SyncMap(ctx context.Context, in *protos.SyncMapRequest) (*protos.SyncMapReply, error) {
	switch in.MapType {
	case protos.PGMAP:
		newPgMaps := in.UnionMap.GetPgmap()
		helper.Println(5, "unionmap0", in.UnionMap)
		helper.Println(5, "unionmap1", newPgMaps)
		helper.Println(5, "wich one will panic 0", newPgMaps.Epoch)
		for pool, pgMap := range newPgMaps.Pgmaps {
			helper.Println(5, "pool:", pool, " poolId:", pgMap.PoolId)
			for pgId, pg := range pgMap.Pgmap {
				helper.Println(5, "pgId:", pgId, " pg:", pg)
			}
		}

		if newPgMaps.Epoch > s.pgMaps.Epoch {
			s.pgMapChan <- newPgMaps
			// signal create replica loop for current notify if it hasn't been already
			s.waitChan <- struct{}{}
			//go s.createOrRemoveReplica(newPgMaps)
			helper.Println(5, "recevie sync map request, epoch:", newPgMaps.Epoch)
		} else {
			helper.Println(5, "recevie expire sync map request, epoch:", newPgMaps.Epoch)
			return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie expire sync map request, new epoch: %v, current epoch: %v", newPgMaps.Epoch, s.pgMaps.Epoch))
		}

	case protos.OSDMAP:
		return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie unsupport sync map request which type is %v", protos.OSDMAP))
	case protos.POOLMAP:
		s.poolMap = in.UnionMap.GetPoolmap()
		for _, pool := range s.poolMap.Pools {
			helper.Println(5, "pool name:", pool.Name, " size:", pool.Size_)
		}
		//return &protos.SyncMapReply{}, errors.New(fmt.Sprintf("recevie unsupport sync map request which type is %v", protos.POOLMAP))
	}

	return &protos.SyncMapReply{}, nil
}

func deleteAfterMigrate(pgid string, key []byte) {
	b := &client.Batch{}
	b.Header.GroupID = multiraftbase.GroupID(pgid)
	b.AddRawRequest(&multiraftbase.DeleteRequest{
		Key: multiraftbase.Key(key),
	})

	if err := Server.store.Db.Run(context.Background(), b); err != nil {
		helper.Printf(5, "Error run batch! try put deleteAfterMigrate kv failed", pgid, key, err)
		return
	}
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
		value, err := engine.Get(in.Marker)
		if err != nil {
			helper.Println(5, "print err when migrate key :", in.Marker, string(in.Marker), err)
			return &protos.MigrateGetReply{}, err
		} else {
			defer deleteAfterMigrate(in.ParentPgId, in.Marker)
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
			// exclude all Prefixed kv
			if bytes.HasPrefix(key, []byte{'\x01'}) ||
				bytes.HasPrefix(key, []byte{'\x02'}) ||
				bytes.HasPrefix(key, []byte{'\x03'}) ||
				bytes.HasPrefix(key, []byte{'\x04'}) {
				continue
			}
			helper.Println(5, "print useful migrate key :", string(key))
			hash := protos.Nentropy_str_hash(string(key))
			hashPgId := protos.Nentropy_stable_mod(int(hash), int(pgNumbers), mask)
			if hashPgId == childPgId {
				value, err := item.Value()
				var onode multiraftbase.Onode
				err = onode.Unmarshal(value)
				if err != nil {
					helper.Println(5, "find bad onode while MigrateGet :", key, string(key))
					continue
				}
				value, err = engine.Get(onode.Key)
				if err != nil {
					helper.Println(5, "try fetch data of key failed", string(key), string(onode.Key), err)
					return &protos.MigrateGetReply{}, err
				} else {
					defer deleteAfterMigrate(in.ParentPgId, key)
					return &protos.MigrateGetReply{key, value, key}, nil
				}
			} else {
				continue
			}
		}
	}
	return &protos.MigrateGetReply{}, nil
}

func (s *OsdServer) createReplicaLoop() {
	for {
		var pgMaps *protos.PgMaps

		select {
		case <-s.waitChan:
		case <-s.stopping:
			return
		}

		pgMaps = nil
		var exist bool
		for !exist {
			select {
			case tmpPgMaps := <-s.pgMapChan:
				if pgMaps == nil || pgMaps.Epoch < tmpPgMaps.Epoch {
					pgMaps = tmpPgMaps
				}
			default:
				exist = true
			}
		}

		if pgMaps == nil {
			continue
		}

		s.pgMaps = pgMaps
		s.createOrRemoveReplica(pgMaps)
	}
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
