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
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"sort"
)

const (
	// TransferLeaderQueueTimerDuration is the duration between truncations. This needs
	// to be relatively short so that truncations can keep up with raft log entry
	// creation.
	TransferLeaderQueueTimerDuration = 1000 * time.Millisecond
	// TransferLeaderQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	TransferLeaderQueueStaleThreshold = 100
	// TransferLeaderQueueStaleSize is the minimum size of the Raft log that we'll
	// truncate even if there are fewer than TransferLeaderQueueStaleThreshold entries
	// to truncate. The value of 64 KB was chosen experimentally by looking at
	// when Raft log truncation usually occurs when using the number of entries
	// as the sole criteria.
	TransferLeaderQueueStaleSize = 64 << 10
)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type transferLeaderQueue struct {
	*baseQueue
	db *client.DB
}

// newRaftLogQueue returns a new instance of raftLogQueue.
func newTransferLeaderQueue(store *Store, db *client.DB) *transferLeaderQueue {
	rlq := &transferLeaderQueue{
		db: db,
	}
	rlq.baseQueue = newBaseQueue(
		"transferleader", rlq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
		},
	)
	return rlq
}

func getIndex(raftStatus *raft.Status) uint64 {
	match := make([]uint64, 0, len(raftStatus.Progress)+1)
	for _, progress := range raftStatus.Progress {
		match = append(match, progress.Match)
	}
	sort.Sort(uint64Slice(match))
	quorum := computeQuorum(len(match))
	return match[len(match)-quorum]
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds TransferLeaderQueueStaleThreshold.
func (rlq *transferLeaderQueue) shouldQueue(
	ctx context.Context, r *Replica,
) (shouldQ bool, priority float64) {
	raftStatus := r.RaftStatus()
	if raftStatus == nil {
		helper.Printf(5, "the raft group doesn't exist")
		return false, 0
	}

	// Is this the raft leader? We only perform log truncation on the raft leader
	// which has the up to date info on followers.
	if raftStatus.RaftState != raft.StateLeader {
		return false, 0
	}

	state, err := GetPgState(string(r.GroupID))
	if err != nil {
		helper.Println(5, "Error getting pg state. pgId:", string(r.GroupID))
		return false, 0
	}
	if state != (protos.PG_STATE_ACTIVE | protos.PG_STATE_CLEAN) {
		helper.Println(5, "shouldQueue pg ", string(r.GroupID), " state ", state)
		return false, 0
	}

	var id int32
	var ok bool
	if id, ok = GetExpectedReplicaId(string(r.GroupID)); !ok {
		helper.Println(5, "Error getting expected replica id!")
		return false, 0
	}
	repSize, ok := GetRepLenInPgMap(string(r.GroupID))
	if !ok {
		helper.Println(5, "Error getting pool size")
		return false, 0
	}

	if multiraftbase.ReplicaID(id) == r.mu.replicaID && len(raftStatus.Progress) == int(repSize) {
		return false, 0
	}

	return true, 0
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds TransferLeaderQueueStaleThreshold.
func (rlq *transferLeaderQueue) process(ctx context.Context, r *Replica) error {
	var id int32
	var ok bool
	if id, ok = GetExpectedReplicaId(string(r.GroupID)); !ok {
		return errors.New("")
	}
	r.mu.Lock()
	if multiraftbase.ReplicaID(id) != r.mu.replicaID {
		helper.Printf(5, "Transfer leader id from %d to %d, groupID %v", r.mu.replicaID, id, r.GroupID)
		r.mu.internalRaftGroup.TransferLeader(uint64(id))
		r.mu.Unlock()
		return nil
	}
	repSize, ok := GetRepLenInPgMap(string(r.GroupID))
	if !ok {
		helper.Println(5, "Error getting pool size")
		r.mu.Unlock()
		return nil
	}
	desc := r.mu.state.Desc
	if len(desc.Replicas) <= int(repSize) {
		r.mu.Unlock()
		return nil
	}
	var reps []protos.PgReplica
	replicas, ok := GetPgReplicas(string(r.GroupID))
	if !ok {
		helper.Println(5, "Error getting pg replicas")
		r.mu.Unlock()
		return nil
	}
	for _, rep := range desc.Replicas {
		var exist bool
		for _, sub := range replicas {
			replicaID := multiraftbase.ReplicaID(sub.ReplicaIndex)
			nodeID := multiraftbase.NodeID(fmt.Sprintf("osd.%d", sub.OsdId))
			if replicaID == rep.ReplicaID && nodeID == rep.NodeID {
				exist = true
			}
		}
		if !exist {
			var osdId int32
			fmt.Sscanf(string(rep.NodeID), "osd.%d", &osdId)
			reps = append(reps, protos.PgReplica{
				OsdId:        osdId,
				ReplicaIndex: int32(rep.ReplicaID),
			})
		}
	}
	r.mu.Unlock()
	helper.Println(5, "transferLeaderQueue: groupId:", r.GroupID, " remove:", reps, " replicaId:", r.mu.replicaID)
	proposeConfChange(multiraftbase.ConfType_REMOVE_REPLICA, r.GroupID, reps)

	return nil
}

// timer returns interval between processing successive queued truncations.
func (*transferLeaderQueue) timer(_ time.Duration) time.Duration {
	return TransferLeaderQueueTimerDuration
}

// purgatoryChan returns nil.
func (*transferLeaderQueue) purgatoryChan() <-chan struct{} {
	return nil
}
