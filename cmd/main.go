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
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/worker"
	"github.com/journeymidnight/nentropy/x"
	"github.com/dgraph-io/badger"
)

var (
	baseHttpPort int
	bindall      bool

	state        ServerState
)

type ServerState struct {
	FinishCh   chan struct{} // channel to wait for all pending reqs to finish.
	ShutdownCh chan struct{} // channel to signal shutdown.

	WALstore *badger.KV
}

func (s *ServerState) initStorage() {
	// Write Ahead Log directory
	x.Checkf(os.MkdirAll(Config.WALDir, 0700), "Error while creating WAL dir.")
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = Config.WALDir
	kvOpt.ValueDir = Config.WALDir
	kvOpt.TableLoadingMode = options.MemoryMap

	var err error
	s.WALstore, err = badger.NewKV(&kvOpt)
	x.Checkf(err, "Error while creating badger KV WAL store")
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
	var config Options
	defaults := DefaultConfig
	flag.StringVar(&config.WALDir, "w", defaults.WALDir,
		"Directory to store raft write-ahead logs.")

	flag.IntVar(&config.WorkerPort, "workerport", defaults.WorkerPort,
		"Port used by worker for internal communication.")
	flag.IntVar(&config.NumPendingProposals, "pending_proposals", defaults.NumPendingProposals,
		"Number of pending mutation proposals. Useful for rate limiting.")
	flag.Float64Var(&config.Tracing, "trace", defaults.Tracing,
		"The ratio of queries to trace.")
	flag.StringVar(&config.PeerAddr, "peer", defaults.PeerAddr,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.Uint64Var(&config.RaftId, "idx", defaults.RaftId,
		"RAFT ID that this server will use to join RAFT cluster.")
	flag.Uint64Var(&config.MaxPendingCount, "sc", defaults.MaxPendingCount,
		"Max number of pending entries in wal after which snapshot is taken")
	flag.BoolVar(&config.Join, "join", false,
		"add the node to the cluster.")

	flag.IntVar(&baseHttpPort, "port", 8080, "Port to run HTTP service on.")
	flag.BoolVar(&bindall, "bindall", false,
		"Use 0.0.0.0 instead of localhost to bind to all addresses on local machine.")

	flag.Parse()
	if !flag.Parsed() {
		log.Fatal("Unable to parse flags")
	}

	Config = config

	worker.Config.WorkerPort = Config.WorkerPort
	worker.Config.NumPendingProposals = Config.NumPendingProposals
	worker.Config.Tracing = Config.Tracing
	worker.Config.PeerAddr = Config.PeerAddr
	worker.Config.RaftId = Config.RaftId
	worker.Config.MaxPendingCount = Config.MaxPendingCount
	worker.Config.Join = Config.Join

}

func httpPort() int {
	return baseHttpPort
}

// handlerInit does some standard checks. Returns false if something is wrong.
func handlerInit(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != "GET" {
		x.SetStatus(w, x.ErrorInvalidMethod, "Invalid method")
		return false
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(ip).IsLoopback() {
		x.SetStatus(w, x.ErrorUnauthorized, fmt.Sprintf("Request from IP: %v", ip))
		return false
	}
	return true
}

func shutDownHandler(w http.ResponseWriter, r *http.Request) {
	if !handlerInit(w, r) {
		return
	}

	shutdownServer()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"code": "Success", "message": "Server is shutting down"}`))
}

func shutdownServer() {
	x.Printf("Got clean exit request")
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

		worker.BlockingStop()
	}()
}

func serveHTTP(l net.Listener) {
	defer func() { state.FinishCh <- struct{}{} }()
	srv := &http.Server{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 600 * time.Second,
		IdleTimeout:  2 * time.Minute,
	}

	err := srv.Serve(l)
	log.Printf("Stopped taking more http(s) requests. Err: %s", err.Error())
	ctx, cancel := context.WithTimeout(context.Background(), 630*time.Second)
	defer cancel()
	err = srv.Shutdown(ctx)
	log.Printf("All http(s) requests finished.")
	if err != nil {
		log.Printf("Http(s) shutdown err: %v", err.Error())
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Setting a higher number here allows more disk I/O calls to be scheduled, hence considerably
	// improving throughput. The extra CPU overhead is almost negligible in comparison. The
	// benchmark notes are located in badger-bench/randread.
	runtime.GOMAXPROCS(128)

	setupConfigOpts() // flag.Parse is called here.
	x.Init()

	state = NewServerState()
	defer state.Dispose()

	worker.Init()

	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)
	var numShutDownSig int
	defer close(sdCh)
	// sigint : Ctrl-C, sigquit : Ctrl-\ (backslash), sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case _, ok := <-sdCh:
				if !ok {
					return
				}
				numShutDownSig++
				x.Println("Caught Ctrl-C. Terminating now (this may take a few seconds)...")
				if numShutDownSig == 1 {
					shutdownServer()
				} else if numShutDownSig == 3 {
					x.Println("Signaled thrice. Aborting!")
					os.Exit(1)
				}
			}
		}
	}()
	_ = numShutDownSig

	// Setup external communication.
	che := make(chan error, 1)
	// By default Go GRPC traces all requests.
	grpc.EnableTracing = false
	go worker.RunServer(bindall) // For internal communication.
	time.Sleep(1 * time.Second)

	go worker.StartRaftNodes(state.WALstore, bindall)

	// the key-value http handler will propose updates to raft
	addr := "localhost"
	if bindall {
		addr = "0.0.0.0"
	}
	laddr := fmt.Sprintf("%s:%d", addr, httpPort())
	listener, err := net.Listen("tcp", laddr)
	serveHttpKVAPI(listener)

	go func() {
		<- state.ShutdownCh
		listener.Close()
	}()

	//che <- err                // final close for main.

	if err = <-che; !strings.Contains(err.Error(),
		"use of closed network connection") {
		log.Fatal(err)
	}
}
