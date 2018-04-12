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

package main

import (
	"golang.org/x/net/context"
	"time"

	"github.com/journeymidnight/nentropy/osd/client"
)

const (
	// replicaGCQueueTimerDuration is the duration between GCs of queued replicas.
	replicaGCQueueTimerDuration = 50 * time.Millisecond

	// ReplicaGCQueueInactivityThreshold is the inactivity duration after which
	// a range will be considered for garbage collection. Exported for testing.
	ReplicaGCQueueInactivityThreshold = 10 * 24 * time.Hour // 10 days
	// ReplicaGCQueueCandidateTimeout is the duration after which a range in
	// candidate Raft state (which is a typical sign of having been removed
	// from the group) will be considered for garbage collection.
	ReplicaGCQueueCandidateTimeout = 1 * time.Second
)

// Priorities for the replica GC queue.
const (
	replicaGCPriorityDefault float64 = 0

	// Replicas that have been removed from the range spend a lot of
	// time in the candidate state, so treat them as higher priority.
	replicaGCPriorityCandidate = 1

	// The highest priority is used when we have definite evidence
	// (external to replicaGCQueue) that the replica has been removed.
	replicaGCPriorityRemoved = 2
)

// replicaGCQueue manages a queue of replicas to be considered for garbage
// collections. The GC process asynchronously removes local data for
// ranges that have been rebalanced away from this store.
type replicaGCQueue struct {
	*baseQueue
	db *client.DB
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(store *Store, db *client.DB) *replicaGCQueue {
	rgcq := &replicaGCQueue{
		db: db,
	}
	rgcq.baseQueue = newBaseQueue(
		"replicaGC", rgcq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
		},
	)
	return rgcq
}

// shouldQueue determines whether a replica should be queued for GC,
// and if so at what priority. To be considered for possible GC, a
// replica's range lease must not have been active for longer than
// ReplicaGCQueueInactivityThreshold. Further, the last replica GC
// check must have occurred more than ReplicaGCQueueInactivityThreshold
// in the past.
func (rgcq *replicaGCQueue) shouldQueue(
	ctx context.Context, repl *Replica,
) (bool, float64) {
	return false, 0
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (rgcq *replicaGCQueue) process(
	ctx context.Context, repl *Replica,
) error {
	// Note that the Replicas field of desc is probably out of date, so
	// we should only use `desc` for its static fields like RangeID and
	// StartKey (and avoid rng.GetReplica() for the same reason).

	return nil
}

func (*replicaGCQueue) timer(_ time.Duration) time.Duration {
	return replicaGCQueueTimerDuration
}

// purgatoryChan returns nil.
func (*replicaGCQueue) purgatoryChan() <-chan struct{} {
	return nil
}
