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
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/consistent"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/memberlist"
)

var (
	state  ServerState
	logger *log.Logger
)

type ServerState struct {
	FinishCh   chan struct{} // channel to wait for all pending reqs to finish.
	ShutdownCh chan struct{} // channel to signal shutdown.

	WALstore *badger.KV
}

func (s *ServerState) initStorage() {
	// Write Ahead Log directory
	helper.Checkf(os.MkdirAll(helper.CONFIG.WALDir, 0700), "Error while creating WAL dir.")
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = helper.CONFIG.WALDir
	kvOpt.ValueDir = helper.CONFIG.WALDir
	kvOpt.TableLoadingMode = options.MemoryMap

	var err error
	s.WALstore, err = badger.NewKV(&kvOpt)
	helper.Checkf(err, "Error while creating badger KV WAL store")
}

func (s *ServerState) Dispose() {
	s.WALstore.Close()
}

func NewServerState() (state ServerState) {
	state.FinishCh = make(chan struct{})
	state.ShutdownCh = make(chan struct{})
	state.initStorage()
	return state
}

func setupConfigOpts() {
	helper.CONFIG = helper.DefaultOption

	flag.StringVar(&helper.CONFIG.WALDir, "w", helper.DefaultOption.WALDir,
		"Directory to store raft write-ahead logs.")
	flag.IntVar(&helper.CONFIG.MonPort, "monPort", helper.DefaultOption.MonPort,
		"Port used by mon for internal communication.")
	flag.IntVar(&helper.CONFIG.NumPendingProposals, "pending_proposals", helper.DefaultOption.NumPendingProposals,
		"Number of pending mutation proposals. Useful for rate limiting.")
	flag.Float64Var(&helper.CONFIG.Tracing, "trace", helper.DefaultOption.Tracing,
		"The ratio of queries to trace.")
	flag.StringVar(&helper.CONFIG.Monitors, "mons", helper.DefaultOption.Monitors,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.Uint64Var(&helper.CONFIG.RaftId, "idx", helper.DefaultOption.RaftId,
		"RAFT ID that this server will use to join RAFT cluster.")
	flag.Uint64Var(&helper.CONFIG.MaxPendingCount, "sc", helper.DefaultOption.MaxPendingCount,
		"Max number of pending entries in wal after which snapshot is taken")
	flag.BoolVar(&helper.CONFIG.JoinMon, "joinMon", false,
		"add the node to the mon cluster.")
	flag.StringVar(&helper.CONFIG.MyAddr, "my", helper.DefaultOption.MyAddr,
		"addr:port of this server, so other mon servers can talk to this.")
	flag.IntVar(&helper.CONFIG.MemberBindPort, "memberBindPort", helper.DefaultOption.MemberBindPort,
		"Port used by memberlist for internal communication.")
	flag.StringVar(&helper.CONFIG.JoinMemberAddr, "joinMemberAddr", helper.DefaultOption.JoinMemberAddr,
		"a valid member addr to join.")

	flag.Parse()
	if !flag.Parsed() {
		logger.Fatal(0, "Unable to parse flags")
	}

	Config.MonPort = helper.CONFIG.MonPort
	Config.NumPendingProposals = helper.CONFIG.NumPendingProposals
	Config.Tracing = helper.CONFIG.Tracing
	Config.Monitors = helper.CONFIG.Monitors
	Config.MyAddr = helper.CONFIG.MyAddr
	Config.RaftId = helper.CONFIG.RaftId
	Config.MaxPendingCount = helper.CONFIG.MaxPendingCount
	Config.JoinMon = helper.CONFIG.JoinMon

}

func shutdownServer() {
	logger.Printf(10, "Got clean exit request")
	// stop profiling
	state.ShutdownCh <- struct{}{} // exit grpc and http servers.

	// wait for grpc and http servers to finish pending reqs and
	// then stop all nodes, internal grpc servers and sync all the marks
	go func() {
		defer func() { state.ShutdownCh <- struct{}{} }()

		// wait for grpc, http and http2 servers to stop
		<-state.FinishCh
		<-state.FinishCh
		<-state.FinishCh

		BlockingStop()
	}()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Setting a higher number here allows more disk I/O calls to be scheduled, hence considerably
	// improving throughput. The extra CPU overhead is almost negligible in comparison. The
	// benchmark notes are located in badger-bench/randread.
	runtime.GOMAXPROCS(128)

	setupConfigOpts() // flag.Parse is called here.
	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger = log.New(os.Stdout, "[nentropy]", log.LstdFlags, helper.CONFIG.LogLevel)
		logger.Print(0, "Failed to open log file "+helper.CONFIG.LogPath)
	} else {
		defer f.Close()
		logger = log.New(f, "[nentropy]", log.LstdFlags, helper.CONFIG.LogLevel)
	}
	helper.Logger = logger
	Config.Logger = logger

	state = NewServerState()
	defer state.Dispose()

	// By default Go GRPC traces all requests.
	grpc.EnableTracing = false
	RunServer() // For internal communication.

	go StartRaftNodes(state.WALstore)

	memberlist.Init(true, helper.CONFIG.MyAddr, logger.Logger)
	memberlist.SetNotifyFunc(NotifyMemberEvent)

	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)
	var numShutDownSig int
	defer close(sdCh)
	osdMap, err := GetCurrOsdMap()
	clus.hashRing = consistent.New(&osdMap)
	runServer()
	// sigint : Ctrl-C, sigquit : Ctrl-\ (backslash), sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	for {
		select {
		case _, ok := <-sdCh:
			if !ok {
				return
			}
			os.Exit(1) // temporarily add
			numShutDownSig++
			logger.Println(5, "Caught Ctrl-C. Terminating now (this may take a few seconds)...")
			if numShutDownSig == 1 {
				shutdownServer()
			} else if numShutDownSig == 3 {
				logger.Println(5, "Signaled thrice. Aborting!")
				os.Exit(1)
			}
		}
	}
}
