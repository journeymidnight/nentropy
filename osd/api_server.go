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
	"fmt"
	"os"
	"sync"

	pb "github.com/journeymidnight/nentropy/osd/protos"
	"github.com/journeymidnight/nentropy/store"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2/bson"
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

type syncBatch struct {
	batch *store.WriteBatch
	coll  *store.Collection

	persist chan error //used to notify an batch has been persist to badger storeo
	oid     []byte
}

func createOrGetOnonde(coll *store.Collection, oid []byte) (o *onode) {
	nodebuffer, err := coll.Get(oid)
	//if cann't get an onode, we should create a new one for this object
	if err != nil || len(nodebuffer) <= 0 {
		o = newOnode(oid)
	} else {
		var p onode
		bson.Unmarshal(nodebuffer, &p)
		o = &p
	}
	return
}

// Write writes a object to store
func (s *Server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteReply, error) {
	dir := string(in.GetPGID())
	fmt.Printf("write request: %+v\r\n", in)

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	// find the onode, onode also stores in the same collection
	// (fixme)use oid as the key to get an onode
	oid := in.GetOid()
	n := createOrGetOnonde(coll, oid)

	fmt.Printf("Write: onode is  %+v\r\n", n)
	batch := store.NewWriteBatch()
	length := in.GetLength()
	offset := in.GetOffset()
	value := in.GetValue()
	valuelen := uint64(len(value))
	stripeSize := n.StripeSize

	//cannot write more than you provide
	if length > valuelen {
		length = valuelen
	}
	// valueLocator is used to locate the user input value correctly
	valueLocator := uint64(0)

	for length > 0 {
		// align to stripeSize
		offsetRem := offset % stripeSize
		endRem := (offset + length) % stripeSize
		fmt.Println("endRem is ", endRem)

		// how many stripes for remaining data
		remainStripeNumber := length / stripeSize
		fmt.Println("remainStripeNumber is ", remainStripeNumber)

		// situation 1: aligned write, if there is a whole stripe of data, no matter rewrite or write a new one, we can safely write it
		if offsetRem == 0 && remainStripeNumber > 0 {
			fmt.Printf("full stripe at offset: %d\r\n", offset)
			stripeWrite(batch, offset, n, value[valueLocator:valueLocator+stripeSize])
			offset += stripeSize
			length -= stripeSize
			valueLocator += stripeSize
			continue
		}

		// read at the aligned offset(align to stripe size)
		stripeOff := offset - offsetRem

		// read the original data from store
		prev, _ := stripeRead(coll, stripeOff, n)
		prevLen := uint64(len(prev))
		fmt.Printf("read previous stripe at offset %d, got %d \r\n", stripeOff, prevLen)

		var buf []byte

		// if we are not aligned to stripe size, maybe we need to add zeros or reusing some of the original data
		if offsetRem > 0 {
			var p uint64
			if prevLen < offsetRem {
				p = prevLen
			} else {
				p = offsetRem
			}

			// situation 2: resuing leading p bytes from the original data
			if p > 0 {
				fmt.Printf("reusing leading %d bytes \r\n", p)
				buf = append(buf, prev[:p]...)
			}
			// situation 3: original data(aka prevLen) shorter than our new offset, so we append zeros to the space for area (prevLen, offsetRem)
			// notice that, if previous data is totally empty, we also need to add leading zeros
			if p < offsetRem {
				fmt.Printf("add leading %d zeros for (prevLen, offsetRem) aka (%d, %d)\r\n", offsetRem-p, prevLen, offsetRem)
				//(fixme) this is ugly
				for i := uint64(0); i < offsetRem-p; i++ {
					buf = append(buf, 0)
				}
			}
		}

		// situation 4: we try use the whole remaning length of this stripe, but the new data length may be shorter than the remaning length
		// in which case, we only use (endRem - offsetRem)
		use := stripeSize - offsetRem
		if use > length {
			use = endRem - offsetRem
		}

		fmt.Printf("using %d bytes for this stripe \r\n", use)
		buf = append(buf, value[valueLocator:use+valueLocator]...)
		fmt.Println("Write: buf is ", buf)
		valueLocator += use

		if endRem > 0 && remainStripeNumber == 0 {
			// situation 5: at the end of the stripe, reusing original bytes if we don't modify it
			if endRem < prevLen {
				l := prevLen - endRem
				buf = append(buf, prev[endRem:prevLen]...)
				fmt.Printf("resue trailing %d bytes \r\n", l)
			}
		}
		fmt.Println("Write: final write buf is ", buf, "offset is ", stripeOff)
		stripeWrite(batch, stripeOff, n, buf)
		offset += use
		length -= use
	}

	if offset > n.Size {
		n.Size = offset
		fmt.Printf("extending size to %d \r\n", offset+length)
	}

	//put onode to store
	newbuf, _ := bson.Marshal(n)
	batch.Put(oid, newbuf)

	fmt.Println("finished processing stripes")

	//put the batch to sync channel
	b := &syncBatch{batch: batch, coll: coll, persist: make(chan error), oid: oid}
	syncChan <- b

	select {
	case <-b.persist:
		return &pb.WriteReply{RetCode: 0}, nil
	}
}

var syncChan = make(chan *syncBatch)

// syncThread syncs an batch to a specific store
func syncThread(done <-chan struct{}) {
	for {
		select {
		case bat := <-syncChan:
			err := bat.coll.Write(bat.batch)
			bat.persist <- err
			fmt.Println("finished process batch for object ", string(bat.oid))
		case <-done:
			fmt.Println("quiting sync thread")
			return
		}
	}
}

func min(a, b uint64) (c uint64) {
	if a < b {
		c = a
	} else {
		c = b
	}
	return
}

// Read reads an object from store
func (s *Server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Printf("read request: %+v\r\n", in)
	dir := string(in.GetPGID())

	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	// this pg should be cached, otherwise return error
	coll, ok := s.collections[dir]
	if !ok {
		return nil, ErrNoSuchPG
	}

	//(fixme)find the onode first, should use cache
	val, err := coll.Get(in.GetOid())
	if err != nil {
		return nil, err
	}

	if len(val) <= 0 {
		return nil, ErrNoValueForKey
	}

	var n onode
	bson.Unmarshal(val, &n)
	offset := in.GetOffset()
	stripeSize := n.StripeSize
	size := n.Size
	length := in.GetLength()

	if offset+length > size {
		length = size - offset
	}

	var buf []byte
	stripeOff := offset % stripeSize
	for length > 0 {
		stripebuf, _ := stripeRead(coll, offset-stripeOff, &n)
		buflen := uint64(len(stripebuf))
		fmt.Printf("got %d bytes for offset %d\r\n", buflen, offset-stripeOff)
		fmt.Println("stripebuf is ", stripebuf)

		swant := min(stripeSize-stripeOff, length)
		if buflen > 0 {
			if swant == buflen {
				buf = append(buf, stripebuf...)
				fmt.Printf("taking full stripe at offset %d \r\n", stripeOff)
			} else {
				l := min(stripeSize-stripeOff, swant)

				//maybe wrong
				buf = append(buf, stripebuf[stripeOff:l+stripeOff]...)
				fmt.Printf("taking at offset %d ~ %d\r\n", stripeOff, l)

				if l < swant {
					fmt.Printf("adding %d zeros\r\n", swant-l)
					for i := uint64(0); i < swant-l; i++ {
						buf = append(buf, 0)
					}
				}
			}
		} else {
			fmt.Printf(" adding %d zeros\r\n", swant)
			for i := uint64(0); i < swant; i++ {
				buf = append(buf, 0)
			}
		}

		offset += swant
		length -= swant
		stripeOff = 0
		fmt.Println("READ: current buf is ", buf)
	}

	return &pb.ReadReply{RetCode: 0, ReadBuf: buf}, nil
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
		return nil, ErrFailedRemovingKey
	}

	return &pb.RemoveReply{RetCode: 0}, nil
}

//CreatePG create a pg
func (s *Server) CreatePG(ctx context.Context, in *pb.CreatePgRequest) (*pb.CreatePgReply, error) {
	dir := string(in.GetPGID())
	_, err := os.Stat(dir)
	if err == nil {
		return nil, ErrPGAlreadyExists
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
	coll.Close()
	coll.Remove()
	delete(s.collections, dir)
	s.rwlock.Unlock()

	return &pb.RemovePgReply{RetCode: 0}, nil
}
