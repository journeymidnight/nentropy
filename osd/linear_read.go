package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/journeymidnight/nentropy/helper"
)

func (r *Replica) linearizableReadLoop() {
	var rs raft.ReadState

	for {
		ctx := make([]byte, 8)
		binary.BigEndian.PutUint64(ctx, r.reqIDGen.Next())

		select {
		case <-r.readwaitc:
		case <-r.stopping:
			return
		}
		nextnr := helper.NewNotifier()

		r.readMu.Lock()
		nr := r.readNotifier
		r.readNotifier = nextnr
		r.readMu.Unlock()

		r.raftMu.Lock()
		r.mu.internalRaftGroup.ReadIndex(ctx)
		r.raftMu.Unlock()

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {
			select {
			case rs = <-r.readStateC:
				done = bytes.Equal(rs.RequestCtx, ctx)
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					helper.Printf(5, "ignored out-of-date read index response (want %v, got %v)", rs.RequestCtx, ctx)
				}
			case <-time.After(5 * time.Second):
				helper.Printf(5, "timed out waiting for read index response")
				nr.Notify(errors.New("netropy: request timed out"))
				timeout = true
			case <-r.stopping:
				return
			}
		}
		if !done {
			continue
		}

		if ai := r.mu.state.RaftAppliedIndex; ai < rs.Index {
			select {
			case <-r.applyWait.Wait(rs.Index):
			case <-r.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.Notify(nil)
	}
}

func (r *Replica) linearizableReadNotify(ctx context.Context) error {
	r.readMu.RLock()
	nc := r.readNotifier
	r.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case r.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.GetChan():
		return nc.GetErr()
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return errors.New("netropy: server stopped")
	}
}
