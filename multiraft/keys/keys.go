package keys

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/journeymidnight/nentropy/protos"
)

// RangeIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
type RangeIDPrefixBuf protos.Key

// RaftLogKey returns a system-local key for a Raft log entry.
func (b RangeIDPrefixBuf) RaftLogKey(logIndex uint64) protos.Key {
	return encoding.EncodeUint64Ascending(b.RaftLogPrefix(), logIndex)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b RangeIDPrefixBuf) RaftHardStateKey() protos.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b RangeIDPrefixBuf) RaftTruncatedStateKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftTruncatedStateSuffix...)
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func (b RangeIDPrefixBuf) RaftLastIndexKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLastIndexSuffix...)
}

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeRangeIDPrefixBuf(groupID roachpb.GroupID) RangeIDPrefixBuf {
	return RangeIDPrefixBuf(MakeRangeIDPrefix(groupID))
}
