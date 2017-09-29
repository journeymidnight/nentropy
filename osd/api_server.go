/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//go:generate protoc -I protos protos/store.proto --go_out=plugins=grpc:protos

package osd

import (
	"errors"
	"fmt"
	"os"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"github.com/journeymidnight/nentropy/store"
	"golang.org/x/net/context"
)

// server is used to implement osd.StoreServer
type server struct {
	collections map[string]*store.Collection //Server holds
}

// Write writes a object to store
func (s *server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	dir := string(in.GetPGID())
	var err error

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return &pb.WriteReply{RetCode: -1}, ErrNoSuchPG
	}

	coll, err := store.NewCollection(dir)
	if err != nil {

		return &pb.WriteReply{RetCode: -2}, err
	}
	//(fixme) should not close
	defer coll.Close()

	seterr := coll.Put(in.GetOid(), in.GetValue())
	if seterr != nil {
		return &pb.WriteReply{RetCode: -3}, errors.New("faild setting key to badger")
	}

	return &pb.WriteReply{RetCode: 0}, nil
}

// Read reads an object from store
func (s *server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	dir := string(in.GetPGID())

	var coll *store.Collection
	var err error

	coll, err = store.NewCollection(dir)
	if err != nil {
		if err == store.ErrDirNotExists {
			return &pb.ReadReply{RetCode: -1}, ErrNoSuchPG
		}
		return &pb.ReadReply{RetCode: -2}, ErrFailedOpenBadgerStore
	}
	defer coll.Close()

	val, err := coll.Get(in.GetOid())
	if err != nil {
		fmt.Println(err)
		return &pb.ReadReply{RetCode: -3}, err
	}

	if len(val) <= 0 {
		return &pb.ReadReply{RetCode: -4}, errors.New("no value for this key")
	}

	return &pb.ReadReply{RetCode: 0, ReadBuf: val}, nil
}

//Remove removes a object from store
func (s *server) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
	dir := string(in.GetPGID())
	var coll *store.Collection
	var err error

	coll, err = store.NewCollection(dir)
	if err != nil {
		if err == store.ErrDirNotExists {
			return &pb.RemoveReply{RetCode: -1}, ErrNoSuchPG
		}
		return &pb.RemoveReply{RetCode: -2}, ErrFailedOpenBadgerStore
	}
	defer coll.Close()

	if err := coll.Delete(in.GetOid()); err != nil {
		return &pb.RemoveReply{RetCode: -1}, errors.New("faild removing key to badger")
	}

	return &pb.RemoveReply{RetCode: 0}, nil
}

//CreatePG create a pg
func (s *server) CreatePG(ctx context.Context, in *pb.CreatePgRequest) (*pb.CreatePgReply, error) {
	dir := string(in.GetPGID())
	_, err := os.Stat(dir)
	if err == nil {
		return &pb.CreatePgReply{RetCode: -1}, errors.New("pg already exists")
	} else if os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
		coll, err := store.NewCollection(dir)
		if err != nil {
			return &pb.CreatePgReply{RetCode: -2}, err
		}

		//(fixme)should not close
		defer coll.Close()

		//after create an pg, load pg to local memory
		//collmap.collections[dir] = coll
		return &pb.CreatePgReply{RetCode: 0}, nil
	}
	return &pb.CreatePgReply{RetCode: -3}, err
}

//RemovePG removes a pg
func (s *server) RemovePG(ctx context.Context, in *pb.RemovePgRequest) (*pb.RemovePgReply, error) {
	dir := string(in.GetPGID())
	_, err := os.Stat(dir)
	if err == nil {
		coll, _ := store.NewCollection(dir)
		coll.Close()
		coll.Remove()
		//delete(collmap.collections, dir)

		return &pb.RemovePgReply{RetCode: 0}, nil
	} else if os.IsNotExist(err) {
		return &pb.RemovePgReply{RetCode: -1}, ErrNoSuchPG
	}
	return &pb.RemovePgReply{RetCode: -2}, err
}
