package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/util/stop"
	"golang.org/x/net/context"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	logger      *log.Logger
	rpcListener net.Listener
	raftPort    int
	cfg         *Config
	stopper     *stop.Stopper
)

func main() {
	cfg = MakeConfig()
	logger = helper.Logger

	stopper = stop.NewStopper()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	errChan := make(chan error, 1)
	var s *OsdServer
	go func() {
		defer func() {
			if s != nil {
				if r := recover(); r != nil {
					panic(r)
				}
			}
		}()
		ctx := context.Background()
		if err := func() error {
			s, err := NewOsdServer(ctx, *cfg, stopper)
			if err != nil {
				return errors.New("failed to create server")
			}

			if err := s.Start(ctx); err != nil {
				return errors.New("failed to start server")
			}

			return nil
		}(); err != nil {
			errChan <- err
		}

	}()

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case err := <-errChan:
		// SetSync both flushes and ensures that subsequent log writes are flushed too.
		logger.Printf(5, "osd quit %v", err)
		return
	case <-stopper.ShouldStop():
		// Server is being stopped externally and our job is finished
		// here since we don't know if it's a graceful shutdown or not.
		<-stopper.IsStopped()
		// SetSync both flushes and ensures that subsequent log writes are flushed too.
		return
	case sig := <-signalCh:
		// We start synchronizing log writes from here, because if a
		// signal was received there is a non-zero chance the sender of
		// this signal will follow up with SIGKILL if the shutdown is not
		// timely, and we don't want logs to be lost.
		logger.Printf(5, "received signal '%s'", sig)
		if sig == os.Interrupt {
			// Graceful shutdown after an interrupt should cause the process
			// to terminate with a non-zero exit code; however SIGTERM is
			// "legitimate" and should be acknowledged with a success exit
			// code. So we keep the error state here for later.
			msgDouble := "Note: a second interrupt will skip graceful shutdown and terminate forcefully"
			fmt.Fprintln(os.Stdout, msgDouble)
		}
		//go func() {
		//	serverStatusMu.Lock()
		//	serverStatusMu.draining = true
		//	needToDrain := serverStatusMu.created
		//	serverStatusMu.Unlock()
		//	if needToDrain {
		//		if _, err := s.Drain(server.GracefulDrainModes); err != nil {
		//			// Don't use shutdownCtx because this is in a goroutine that may
		//			// still be running after shutdownCtx's span has been finished.
		//			log.Warning(context.Background(), err)
		//		}
		//	}
		//	stopper.Stop(context.Background())
		//}()
	}

	return

}
