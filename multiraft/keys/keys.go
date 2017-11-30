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

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeGroupIDPrefixBuf(rangeID multiraftbase.GroupID) GroupIDPrefixBuf {
	return GroupIDPrefixBuf(MakeGroupIDPrefix(rangeID))
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(groupID multiraftbase.GroupID, logIndex uint64) multiraftbase.Key {
	return MakeGroupIDPrefixBuf(groupID).RaftLogKey(logIndex)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func (b GroupIDPrefixBuf) RaftLogKey(logIndex uint64) multiraftbase.Key {
	return encoding.EncodeUint64Ascending([]byte("raft-log-"), logIndex)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b GroupIDPrefixBuf) RaftHardStateKey() multiraftbase.Key {
	return append([]byte("unreplicated-hardstate-"), []byte("rfth")...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b GroupIDPrefixBuf) RaftTruncatedStateKey() multiraftbase.Key {
	return append([]byte("truncated-state-"), []byte("rftt")...)
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func (b GroupIDPrefixBuf) RaftLastIndexKey() multiraftbase.Key {
	return append([]byte("last-index-"), []byte("rfti")...)
}

func makePrefixWithGroupID(prefix []byte, groupID multiraftbase.GroupID) multiraftbase.Key {
	// Size the key buffer so that it is large enough for most callers.
	key := make(multiraftbase.Key, 0, 32)
	key = append(key, prefix...)
	key = encoding.EncodeStringAscending(key, string(groupID))
	return key
}

// MakeGroupIDPrefix creates a range-local key prefix from
// rangeID for both replicated and unreplicated data.
func MakeGroupIDPrefix(groupID multiraftbase.GroupID) multiraftbase.Key {
	return makePrefixWithGroupID([]byte("group-"), groupID)
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func (b GroupIDPrefixBuf) RaftAppliedIndexKey() multiraftbase.Key {
	return append([]byte("applied-index-"), []byte("r")...)
}

// RangeReplicaDestroyedErrorKey returns a range-local key for
// the range's replica destroyed error.
func (b GroupIDPrefixBuf) GroupReplicaDestroyedErrorKey() multiraftbase.Key {
	return append([]byte("replica-destroyed-"), []byte("rrde")...)
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func GroupDescriptorKey(groupID multiraftbase.GroupID) multiraftbase.Key {
	return MakeGroupIDPrefix(groupID)
}
