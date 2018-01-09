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

package main

import (
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/memberlist"
	"net"
)

var (
	logger *log.Logger
	cfg    *Config

	WALstore *badger.DB
)

type ServerState struct {
	WALstore *badger.DB
}

func initStorage() {
	dbOpts := badger.DefaultOptions
	dbOpts.SyncWrites = true
	dbOpts.Dir = config.WALDir
	dbOpts.ValueDir = config.WALDir
	dbOpts.TableLoadingMode = options.MemoryMap

	var err error
	WALstore, err = badger.Open(dbOpts)
	helper.Checkf(err, "Error while creating badger KV WAL store")
}

func disposeStorage() {
	WALstore.Close()
}

func newGrpcServer() *grpc.Server {
	var opts []grpc.ServerOption
	// By default Go GRPC traces all requests.
	// grpc.EnableTracing = false
	return grpc.NewServer(opts...)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg = MakeConfig()
	logger = helper.Logger

	initStorage()
	defer disposeStorage()

	grpcSrv := newGrpcServer()

	go StartRaftNodes(WALstore, grpcSrv)

	helper.Logger.Println(5, "raftid, advertiseaddr", cfg.RaftId, cfg.AdvertiseAddr)
	memberlist.Init(true, false, cfg.RaftId, cfg.AdvertiseAddr, logger.Logger, cfg.JoinMemberAddr)
	memberlist.SetNotifyFunc(NotifyMemberEvent)

	runServer(grpcSrv)

	laddr := "0.0.0.0"
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, config.MonPort))
	if err != nil {
		helper.Logger.Fatalf(0, "While running server: %v", err)
		return
	}
	go grpcSrv.Serve(ln)

	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)
	defer close(sdCh)
	// sigint : Ctrl-C, sigquit : Ctrl-\ (backslash), sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	for {
		select {
		case _, ok := <-sdCh:
			if !ok {
				return
			}
			os.Exit(1) // temporarily add
		}
	}
}
