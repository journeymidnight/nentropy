package main

import (
	"flag"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/memberlist"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	logger       *log.Logger
	state        ServerState
	raftListener net.Listener
	memberlist   *memberlist.Memberlist
	raftPort     int
)

type ServerState struct {
	FinishCh   chan struct{} // channel to wait for all pending reqs to finish.
	ShutdownCh chan struct{} // channel to signal shutdown.
}

func NewServerState() (state ServerState) {
	state.FinishCh = make(chan struct{})
	state.ShutdownCh = make(chan struct{})
	return state
}

func (s *ServerState) Dispose() {
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

func setupConfigOpts() {
	helper.CONFIG = helper.DefaultOption

	flag.IntVar(&helper.CONFIG.MonPort, "monPort", helper.DefaultOption.MonPort,
		"Port used by mon for internal communication.")
	flag.StringVar(&helper.CONFIG.Monitors, "mons", helper.DefaultOption.Monitors,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.IntVar(&helper.CONFIG.MemberBindPort, "memberBindPort", 0,
		"Port used by memberlist for internal communication.")
	flag.StringVar(&helper.CONFIG.JoinMemberAddr, "joinMemberAddr", helper.DefaultOption.JoinMemberAddr,
		"a valid member addr to join.")
	flag.Uint64Var(&helper.CONFIG.RaftId, "idx", helper.DefaultOption.RaftId,
		"RAFT ID that this server will use to join RAFT cluster.")

	flag.Parse()
	if !flag.Parsed() {
		logger.Fatal(0, "Unable to parse flags")
	}
}

func main() {
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

	//take up a raft port at first
	raftListener, err = net.Listen("tcp", fmt.Sprintf("localhost:%d", 0))
	if err != nil {
		logger.Fatalf(5, "failed to listen: %v", err)
	}
	raftPort = raftListener.Addr().(*net.TCPAddr).Port

	//raft init
	list := memberlist.Init(false, helper.CONFIG.RaftId, helper.CONFIG.MyAddr, uint16(raftPort), logger.Logger)
	state = NewServerState()
	defer state.Dispose()
	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)
	var numShutDownSig int
	defer close(sdCh)
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

			} else if numShutDownSig == 3 {
				logger.Println(5, "Signaled thrice. Aborting!")
				os.Exit(1)
			}
		}
	}

}
