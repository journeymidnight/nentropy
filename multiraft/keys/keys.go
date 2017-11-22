package keys

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
)

// GroupIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
type GroupIDPrefixBuf multiraftbase.Key

// RaftLogKey returns a system-local key for a Raft log entry.
func (b GroupIDPrefixBuf) RaftLogKey(logIndex uint64) protos.Key {
	return encoding.EncodeUint64Ascending(b.RaftLogPrefix(), logIndex)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b GroupIDPrefixBuf) RaftHardStateKey() protos.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b GroupIDPrefixBuf) RaftTruncatedStateKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftTruncatedStateSuffix...)
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func (b GroupIDPrefixBuf) RaftLastIndexKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLastIndexSuffix...)
}

// MakeGroupIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeGroupIDPrefixBuf(groupID roachpb.GroupID) GroupIDPrefixBuf {
	return GroupIDPrefixBuf(MakeRangeIDPrefix(groupID))
}
