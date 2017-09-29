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
	"os"

	"github.com/dgraph-io/badger"
	pb "github.com/journeymidnight/nentropy/osd/protos"
	"github.com/journeymidnight/nentropy/store"
	"golang.org/x/net/context"
)

// Server is used to implement osd.StoreServer
type Server struct{}

// Write writes a object to store
func (s *Server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	opt := badger.DefaultOptions
	dir := string(in.GetPGID())

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
	}

	opt.Dir = dir
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)
	defer kv.Close()

	seterr := kv.Set(in.GetOid(), in.GetValue(), 0)

	if seterr != nil {
		return &pb.WriteReply{RetCode: -1}, errors.New("faild setting key to badger")
	}

	return &pb.WriteReply{RetCode: 0}, nil
}

// Read reads an object from store
func (s *Server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	dir := string(in.GetPGID())
	coll, err := store.NewCollection(dir, true)
	if err != nil {
		if err == store.ErrDirNotExists {
			return &pb.ReadReply{RetCode: -1}, ErrNoSuchPG
		}
		return &pb.ReadReply{RetCode: -2}, ErrFailedOpenBadgerStore
	}

	val, err := coll.Get(in.GetOid())
	if err != nil {
		return &pb.ReadReply{RetCode: -3}, err
	}

	if len(val) <= 0 {
		return &pb.ReadReply{RetCode: -4}, errors.New("no value for this key")
	}

	return &pb.ReadReply{RetCode: 0, ReadBuf: val}, nil
}

//Remove removes a object from store
func (s *Server) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
	opt := badger.DefaultOptions
	dir := string(in.GetPGID())

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return &pb.RemoveReply{RetCode: -1}, errors.New("no such pg")
	}

	opt.Dir = dir
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)
	defer kv.Close()
	if err := kv.Delete(in.GetOid()); err != nil {
		return &pb.RemoveReply{RetCode: -1}, errors.New("faild removing key to badger")
	}

	return &pb.RemoveReply{RetCode: 0}, nil
}
