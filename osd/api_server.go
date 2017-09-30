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
	"sync"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"github.com/journeymidnight/nentropy/store"
	"golang.org/x/net/context"
)

// Server is used to implement osd.StoreServer
type Server struct {
	rwlock      sync.RWMutex
	collections map[string]*store.Collection //Server holds
}

// NewServer creates a Server
func NewServer() *Server {
	return &Server{collections: make(map[string]*store.Collection)}
}

// Write writes a object to store
func (s *Server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	dir := string(in.GetPGID())

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	seterr := coll.Put(in.GetOid(), in.GetValue())
	if seterr != nil {
		return nil, errors.New("faild setting key to badger")
	}

	return &pb.WriteReply{RetCode: 0}, nil
}

// Read reads an object from store
func (s *Server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	dir := string(in.GetPGID())

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	val, err := coll.Get(in.GetOid())
	if err != nil {
		return nil, err
	}

	if len(val) <= 0 {
		return nil, errors.New("no value for this key")
	}

	return &pb.ReadReply{RetCode: 0, ReadBuf: val}, nil
}

//Remove removes a object from store
func (s *Server) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
	dir := string(in.GetPGID())
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	if err := coll.Delete(in.GetOid()); err != nil {
		return nil, errors.New("faild removing key to badger")
	}

	return &pb.RemoveReply{RetCode: 0}, nil
}

//CreatePG create a pg
func (s *Server) CreatePG(ctx context.Context, in *pb.CreatePgRequest) (*pb.CreatePgReply, error) {
	dir := string(in.GetPGID())
	_, err := os.Stat(dir)
	if err == nil {
		return nil, errors.New("pg already exists")
	} else if os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
		coll, err := store.NewCollection(dir)
		if err != nil {
			return nil, err
		}

		//after create an pg, load pg to local memory
		s.rwlock.Lock()
		s.collections[dir] = coll
		s.rwlock.Unlock()

		return &pb.CreatePgReply{RetCode: 0}, nil
	}
	return nil, err
}

//RemovePG removes a pg
func (s *Server) RemovePG(ctx context.Context, in *pb.RemovePgRequest) (*pb.RemovePgReply, error) {
	dir := string(in.GetPGID())

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	s.rwlock.Lock()
	coll.Remove()
	delete(s.collections, dir)
	s.rwlock.Unlock()

	return &pb.RemovePgReply{RetCode: 0}, nil
}
