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
	"sort"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/util/envutil"
)

const (
	// RaftLogQueueTimerDuration is the duration between truncations. This needs
	// to be relatively short so that truncations can keep up with raft log entry
	// creation.
	RaftLogQueueTimerDuration = 50 * time.Millisecond
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	RaftLogQueueStaleThreshold = 100
	// RaftLogQueueStaleSize is the minimum size of the Raft log that we'll
	// truncate even if there are fewer than RaftLogQueueStaleThreshold entries
	// to truncate. The value of 64 KB was chosen experimentally by looking at
	// when Raft log truncation usually occurs when using the number of entries
	// as the sole criteria.
	RaftLogQueueStaleSize = 64 << 10
)

// raftLogMaxSize limits the maximum size of the Raft log.
var raftLogMaxSize = envutil.EnvOrDefaultInt64("NENTROPY_RAFT_LOG_MAX_SIZE", 4<<20 /* 4 MB */)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	*baseQueue
	db *client.DB
}

// newRaftLogQueue returns a new instance of raftLogQueue.
func newRaftLogQueue(store *Store, db *client.DB) *raftLogQueue {
	rlq := &raftLogQueue{
		db: db,
	}
	rlq.baseQueue = newBaseQueue(
		"raftlog", rlq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
		},
	)
	return rlq
}

func shouldTruncate(truncatableIndexes uint64, raftLogSize int64) bool {
	return truncatableIndexes >= RaftLogQueueStaleThreshold ||
		(truncatableIndexes > 0 && raftLogSize >= RaftLogQueueStaleSize)
}

// getTruncatableIndexes returns the number of truncatable indexes, the oldest
// index that cannot be truncated, and the current Raft log size. See
// computeTruncatableIndex.
func getTruncatableIndexes(ctx context.Context, r *Replica) (uint64, uint64, int64, error) {
	groupID := r.GroupID
	raftStatus := r.RaftStatus()
	if raftStatus == nil {
		helper.Printf(5, "the raft group doesn't exist for r%d", groupID)
		return 0, 0, 0, nil
	}

	// Is this the raft leader? We only perform log truncation on the raft leader
	// which has the up to date info on followers.
	if raftStatus.RaftState != raft.StateLeader {
		return 0, 0, 0, nil
	}

	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
	// We target the raft log size at the size of the replicated data. When
	// writing to a replica, it is common for the raft log to become larger than
	// the replicated data as the raft log contains the overhead of the
	// BatchRequest which includes the full transaction state as well as begin
	// and end transaction operations. If the estimated raft log size becomes
	// larger than the replica size, we're better off recovering the replica
	// using a snapshot.
	r.mu.maxBytes = 64 << 20 // 64MB
	targetSize := r.mu.state.Stats.TotalBytes
	if targetSize > r.mu.maxBytes {
		targetSize = r.mu.maxBytes
	}
	if targetSize > raftLogMaxSize {
		targetSize = raftLogMaxSize
	}
	firstIndex, err := r.raftFirstIndexLocked()
	pendingSnapshotIndex := r.mu.pendingSnapshotIndex
	lastIndex := r.mu.lastIndex
	r.mu.Unlock()
	if err != nil {
		return 0, 0, 0, errors.Errorf("error retrieving first index for r%d: %s", groupID, err)
	}
	truncatableIndex := computeTruncatableIndex(
		raftStatus, raftLogSize, targetSize, firstIndex, lastIndex, pendingSnapshotIndex)
	// Return the number of truncatable indexes.
	return truncatableIndex - firstIndex, truncatableIndex, raftLogSize, nil
}

// computeTruncatableIndex returns the oldest index that cannot be
// truncated. If there is a behind node, we want to keep old raft logs so it
// can catch up without having to send a full snapshot. However, if a node down
// is down long enough, sending a snapshot is more efficient and we should
// truncate the log to the next behind node or the quorum committed index. We
// currently truncate when the raft log size is bigger than the range
// size.
//
// Note that when a node is behind we continue to let the raft log build up
// instead of truncating to the commit index. Consider what would happen if we
// truncated to the commit index whenever a node is behind and thus needs to be
// caught up via a snapshot. While we're generating the snapshot, sending it to
// the behind node and waiting for it to be applied we would continue to
// truncate the log. If the snapshot generation and application takes too long
// the behind node will be caught up to a point behind the current first index
// and thus require another snapshot, likely entering a never ending loop of
// snapshots. See #8629.
func computeTruncatableIndex(
	raftStatus *raft.Status,
	raftLogSize int64,
	targetSize int64,
	firstIndex uint64,
	lastIndex uint64,
	pendingSnapshotIndex uint64,
) uint64 {
	quorumIndex := getQuorumIndex(raftStatus, pendingSnapshotIndex)
	truncatableIndex := quorumIndex

	if raftLogSize <= targetSize {
		// Only truncate to one of the follower indexes if the raft log is less
		// than the target size. If the raft log is greater than the target size we
		// always truncate to the quorum commit index.
		for _, progress := range raftStatus.Progress {
			index := progress.Match
			if truncatableIndex > index {
				truncatableIndex = index
			}
		}
		// The pending snapshot index acts as a placeholder for a replica that is
		// about to be added to the range. We don't want to truncate the log in a
		// way that will require that new replica to be caught up via a Raft
		// snapshot.
		if pendingSnapshotIndex > 0 && truncatableIndex > pendingSnapshotIndex {
			truncatableIndex = pendingSnapshotIndex
		}
	}

	if truncatableIndex < firstIndex {
		truncatableIndex = firstIndex
	}
	// Never truncate past the quorum commit index (this can only occur if
	// firstIndex > quorumIndex).
	if truncatableIndex > quorumIndex {
		truncatableIndex = quorumIndex
	}
	// Never truncate past the last index. Naively, you would expect lastIndex to
	// never be smaller than quorumIndex, but RaftStatus.Progress.Match is
	// updated on the leader when a command is proposed and in a single replica
	// Raft group this also means that RaftStatus.Commit is updated at propose
	// time.
	if truncatableIndex > lastIndex {
		truncatableIndex = lastIndex
	}
	return truncatableIndex
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}

// getQuorumIndex returns the index which a quorum of the nodes have
// committed. The pendingSnapshotIndex indicates the index of a pending
// snapshot which is considered part of the Raft group even though it hasn't
// been added yet. Note that getQuorumIndex may return 0 if the progress map
// doesn't contain information for a sufficient number of followers (e.g. the
// local replica has only recently become the leader). In general, the value
// returned by getQuorumIndex may be smaller than raftStatus.Commit which is
// the log index that has been committed by a quorum of replicas where that
// quorum was determined at the time the index was written. If you're thinking
// of using getQuorumIndex for some purpose, consider that raftStatus.Commit
// might be more appropriate (e.g. determining if a replica is up to date).
func getQuorumIndex(raftStatus *raft.Status, pendingSnapshotIndex uint64) uint64 {
	match := make([]uint64, 0, len(raftStatus.Progress)+1)
	for _, progress := range raftStatus.Progress {
		match = append(match, progress.Match)
	}
	if pendingSnapshotIndex != 0 {
		match = append(match, pendingSnapshotIndex)
	}
	sort.Sort(uint64Slice(match))
	quorum := computeQuorum(len(match))
	return match[len(match)-quorum]
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) shouldQueue(
	ctx context.Context, r *Replica,
) (shouldQ bool, priority float64) {
	truncatableIndexes, _, raftLogSize, err := getTruncatableIndexes(ctx, r)
	if err != nil {
		helper.Println(5, err)
		return false, 0
	}

	return shouldTruncate(truncatableIndexes, raftLogSize), float64(raftLogSize)
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(ctx context.Context, r *Replica) error {
	truncatableIndexes, oldestIndex, raftLogSize, err := getTruncatableIndexes(ctx, r)
	if err != nil {
		return err
	}

	// Can and should the raft logs be truncated?
	if shouldTruncate(truncatableIndexes, raftLogSize) {
		r.mu.Lock()
		raftLogSize := r.mu.raftLogSize
		term, err := r.raftTermRLocked(oldestIndex)
		if err != nil {
			helper.Println(5, "Error getting term.")
		}
		r.mu.Unlock()

		helper.Printf(5, "truncating raft log %d-%d: size=%d",
			oldestIndex-truncatableIndexes, oldestIndex, raftLogSize)

		b := &client.Batch{}
		b.Header.GroupID = r.GroupID
		b.AddRawRequest(&multiraftbase.TruncateLogRequest{
			Index:   oldestIndex,
			Term:    term,
			GroupID: r.GroupID,
		})
		if err := rlq.db.Run(ctx, b); err != nil {
			helper.Println(5, "result is:", err)
			return err
		}
	}
	return nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer(_ time.Duration) time.Duration {
	return RaftLogQueueTimerDuration
}

// purgatoryChan returns nil.
func (*raftLogQueue) purgatoryChan() <-chan struct{} {
	return nil
}

var _ sort.Interface = uint64Slice(nil)

// uint64Slice implements sort.Interface
type uint64Slice []uint64

// Len implements sort.Interface
func (a uint64Slice) Len() int { return len(a) }

// Swap implements sort.Interface
func (a uint64Slice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
