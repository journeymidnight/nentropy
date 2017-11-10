package multiraft

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"sync"
	"time"
)

const ()

type Replica struct {
	RaftID string //could be 1.1 for osd, or mon for monitor
	mu     struct {
		sync.Mutex
		state multiraftbase.ReplicaState
		// Last index/term persisted to the raft log (not necessarily
		// committed). Note that lastTerm may be 0 (and thus invalid) even when
		// lastIndex is known, in which case the term will have to be retrieved
		// from the Raft log entry. Use the invalidLastTerm constant for this
		// case.
		lastIndex, lastTerm uint64
		// The most recent commit index seen in a message from the leader. Used by
		// the follower to estimate the number of Raft log entries it is
		// behind. This field is only valid when the Replica is a follower.
		estimatedCommitIndex uint64
		// The raft log index of a pending preemptive snapshot. Used to prohibit
		// raft log truncation while a preemptive snapshot is in flight. A value of
		// 0 indicates that there is no pending snapshot.
		pendingSnapshotIndex uint64
		// raftLogSize is the approximate size in bytes of the persisted raft log.
		// On server restart, this value is assumed to be zero to avoid costly scans
		// of the raft log. This will be correct when all log entries predating this
		// process have been truncated.
		raftLogSize int64
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID int32
		leaderID  int32
		// The most recently added replica for the range and when it was added.
		// Used to determine whether a replica is new enough that we shouldn't
		// penalize it for being slightly behind. These field gets cleared out once
		// we know that the replica has caught up.
		lastReplicaAdded     int32
		lastReplicaAddedTime time.Time

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int
	}
	unreachablesMu struct {
		syncutil.Mutex
		remotes map[int32]struct{}
	}
}
