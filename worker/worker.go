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

// Package worker contains code for internal worker communication to perform
// queries and mutations.
package worker

import (
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/x"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"bytes"
	"encoding/gob"
)

var (
	workerServer     *grpc.Server
	// In case of flaky network connectivity we would try to keep upto maxPendingEntries in wal
	// so that the nodes which have lagged behind leader can just replay entries instead of
	// fetching snapshot if network disconnectivity is greater than the interval at which snapshots
	// are taken
)

func workerPort() int {
	return Config.WorkerPort
}

func Init() {
	workerServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(x.GrpcMaxSize),
		grpc.MaxSendMsgSize(x.GrpcMaxSize),
		grpc.MaxConcurrentStreams(math.MaxInt32))
}

// grpcWorker struct implements the gRPC server interface.
type grpcWorker struct {
	sync.Mutex
	reqids map[uint64]bool
}

// addIfNotPresent returns false if it finds the reqid already present.
// Otherwise, adds the reqid in the list, and returns true.
func (w *grpcWorker) addIfNotPresent(reqid uint64) bool {
	w.Lock()
	defer w.Unlock()
	if w.reqids == nil {
		w.reqids = make(map[uint64]bool)
	} else if _, has := w.reqids[reqid]; has {
		return false
	}
	w.reqids[reqid] = true
	return true
}

// Hello rpc call is used to check connection with other workers after worker
// tcp server for this instance starts.
func (w *grpcWorker) Echo(ctx context.Context, in *protos.Payload) (*protos.Payload, error) {
	return &protos.Payload{Data: in.Data}, nil
}

// RunServer initializes a tcp server on port which listens to requests from
// other workers for internal communication.
func RunServer(bindall bool) {
	laddr := "localhost"
	if bindall {
		laddr = "0.0.0.0"
	}
	var err error
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, workerPort()))
	if err != nil {
		log.Fatalf("While running server: %v", err)
		return
	}
	x.Printf("Worker listening at address: %v", ln.Addr())

	protos.RegisterWorkerServer(workerServer, &grpcWorker{})
	workerServer.Serve(ln)
}

// BlockingStop stops all the nodes, server between other workers and syncs all marks.
func BlockingStop() {
	stopAllNodes()           // blocking stop all nodes
	if workerServer != nil { // possible if Config.InMemoryComm == true
		workerServer.GracefulStop() // blocking stop server
	}
	// blocking sync all marks
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	snapshotAll()
}

func ProposeMessage(key string, val string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{key, val}); err != nil {
		log.Fatal(err)
	}
	proposal := protos.Proposal{Data: []byte(buf.String())}
	cluster().node.ProposeAndWait(context.TODO(), &proposal)
	return nil
}

func Lookup(key string) (string, bool) {
	val, _ := cluster().node.kvStore.Lookup(key)
	x.Println("lookup() key:", key, "val:", val)
	return val, true
}
