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

package store

import (
	"errors"
	"os"

	"github.com/dgraph-io/badger"
	pb "github.com/journeymidnight/nentropy/store/protos"
	"golang.org/x/net/context"
)

// server is used to implement store.StoreServer
type server struct{}

func (s *server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
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

func (s *server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	opt := badger.DefaultOptions
	dir := string(in.GetPGID())

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return &pb.ReadReply{RetCode: -1}, errors.New("no such pg")
	}

	opt.Dir = dir
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)
	defer kv.Close()

	var item badger.KVItem
	if err := kv.Get(in.GetOid(), &item); err != nil {
		return &pb.ReadReply{RetCode: -2}, errors.New("faild to get key")
	}
	var val []byte
	err := item.Value(func(v []byte) {
		val = make([]byte, len(v))
		copy(val, v)
	})
	if err != nil {
		return &pb.ReadReply{RetCode: -3}, errors.New("faild to copy value")
	}

	if len(val) <= 0 {
		return &pb.ReadReply{RetCode: -4}, errors.New("no value for this key")
	}

	return &pb.ReadReply{RetCode: 0, ReadBuf: val}, nil
}

func (s *server) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
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
