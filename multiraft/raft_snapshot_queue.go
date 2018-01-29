// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package multiraft

import (
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
)

const (
	// raftSnapshotQueueTimerDuration is the duration between Raft snapshot of
	// queued replicas.
	raftSnapshotQueueTimerDuration = 0 // zero duration to process Raft snapshots greedily

	raftSnapshotPriority float64 = 0
)

// raftSnapshotQueue manages a queue of replicas which may need to catch a
// replica up with a snapshot to their range.
type raftSnapshotQueue struct {
	*baseQueue
}

// newRaftSnapshotQueue returns a new instance of raftSnapshotQueue.
func newRaftSnapshotQueue(store *Store) *raftSnapshotQueue {
	rq := &raftSnapshotQueue{}
	rq.baseQueue = newBaseQueue(
		"raftsnapshot", rq, store,
		queueConfig{
			maxSize: defaultQueueMaxSize,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft snapshots cannot be performed by the replicateQueue.
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
		},
	)
	return rq
}

func (rq *raftSnapshotQueue) shouldQueue(
	ctx context.Context, repl *Replica,
) (shouldQ bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for _, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
				helper.Printf(5, "raft snapshot needed, enqueuing")
				return true, raftSnapshotPriority
			}
		}
	}
	return false, 0
}

func (rq *raftSnapshotQueue) process(
	ctx context.Context, repl *Replica,
) error {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
				helper.Printf(5, "sending raft snapshot")
				if err := rq.processRaftSnapshot(ctx, repl, multiraftbase.ReplicaID(id)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id multiraftbase.ReplicaID,
) error {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas)
	}
	err := repl.sendSnapshot(ctx, repDesc, snapTypeRaft, multiraftbase.SnapshotRequest_RECOVERY)
	// Report the snapshot status to Raft, which expects us to do this once
	// we finish sending the snapshot.
	repl.reportSnapshotStatus(uint64(id), err)
	return err
}

func (*raftSnapshotQueue) timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *raftSnapshotQueue) purgatoryChan() <-chan struct{} {
	return nil
}
