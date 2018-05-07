package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	logger   *log.Logger
	Listener net.Listener
	raftPort int
	cfg      *Config
	stopper  *stop.Stopper
	Server   *OsdServer
)

func main() {
	cfg = MakeConfig()
	logger = helper.Logger

	stopper = stop.NewStopper()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	errChan := make(chan error, 1)
	var err error
	Listener, err = net.Listen("tcp", ":0")
	if err != nil {
		panic("take up a random port failed")
	}
	helper.Println(5, "Listen at :", Listener.Addr())
	cfg.Tracer = tracing.NewTracer()
	cfg.Tracer.Configure()
	sp := cfg.Tracer.StartSpan("Server Start")
	ctx := opentracing.ContextWithSpan(context.Background(), sp)
	defer sp.Finish()
	go func() {
		defer func() {
			if Server != nil {
				if r := recover(); r != nil {
					panic(r)
				}
			}
		}()
		var err error
		cfg.AmbientCtx = log.NewAmbientContext(ctx, cfg.Tracer)
		if err = func() error {
			Server, err = NewOsdServer(*cfg, stopper)
			if err != nil {
				return errors.New("failed to create server : " + err.Error())
			}

			if err = Server.Start(ctx); err != nil {
				return errors.New("failed to start server : " + err.Error())
			}

			return nil
		}(); err != nil {
			errChan <- err
		}

	}()

	defer helper.Printf(0, "exit...")

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case err := <-errChan:
		// SetSync both flushes and ensures that subsequent log writes are flushed too.
		helper.Printf(5, "osd quit %v", err)
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
		helper.Printf(5, "received signal '%s'", sig)
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
