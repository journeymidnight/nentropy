package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
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
