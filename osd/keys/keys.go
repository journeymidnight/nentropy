package keys

import (
	"bytes"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
)

const (
	localPrefixByte  = '\x01'
	localMaxByte     = '\x02'
	metaPrefixByte   = localMaxByte
	metaMaxByte      = '\x03'
	systemPrefixByte = metaMaxByte
	systemMaxByte    = '\x04'
	dataPrefixByte   = systemMaxByte
	dataMaxByte      = '\x05'
)

var (
	localPrefix                 = multiraftbase.Key{localPrefixByte}
	dataPrefix                  = multiraftbase.Key{dataPrefixByte}
	localGroupIDReplicatedInfix = []byte("r")
	// LocalRaftAppliedIndexSuffix is the suffix for the raft applied index.
	LocalRaftAppliedIndexSuffix = []byte("rfta")

	LocalGroupReplicaMembersSuffix = []byte("g")

	localGroupIDUnreplicatedInfix = []byte("u")

	LocalGroupReplicaDestroyedErrorSuffix = []byte("rrde")

	LocalRaftLastIndexSuffix = []byte("rfti")

	LocalRaftTruncatedStateSuffix = []byte("rftt")

	LocalRaftHardStateSuffix = []byte("rfth")

	LocalRaftLogSuffix = []byte("rftl")

	LocalGroupIDPrefix = multiraftbase.RKey(makeKey(localPrefix, multiraftbase.Key("i")))
)

func makeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

// GroupIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
type GroupIDPrefixBuf multiraftbase.Key

func (b GroupIDPrefixBuf) replicatedPrefix() multiraftbase.Key {
	return append(multiraftbase.Key(b), localGroupIDReplicatedInfix...)
}

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeGroupIDPrefixBuf(groupID multiraftbase.GroupID) GroupIDPrefixBuf {
	return GroupIDPrefixBuf(MakeGroupIDPrefix(groupID))
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(groupID multiraftbase.GroupID, logIndex uint64) multiraftbase.Key {
	return MakeGroupIDPrefixBuf(groupID).RaftLogKey(logIndex)
}

// RaftLogPrefix returns the system-local prefix shared by all entries
// in a Raft log.
func (b GroupIDPrefixBuf) RaftLogPrefix() multiraftbase.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLogSuffix...)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func (b GroupIDPrefixBuf) RaftLogKey(logIndex uint64) multiraftbase.Key {
	return helper.EncodeUint64Ascending(b.RaftLogPrefix(), logIndex)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b GroupIDPrefixBuf) RaftHardStateKey() multiraftbase.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b GroupIDPrefixBuf) RaftTruncatedStateKey() multiraftbase.Key {
	return append(b.replicatedPrefix(), LocalRaftTruncatedStateSuffix...)
}

// RaftLastIndexKey returns a system-local key for the last index of the
// Raft log.
func (b GroupIDPrefixBuf) RaftLastIndexKey() multiraftbase.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLastIndexSuffix...)
}

func makePrefixWithGroupID(prefix []byte, groupID multiraftbase.GroupID, infix multiraftbase.RKey) multiraftbase.Key {
	// Size the key buffer so that it is large enough for most callers.
	key := make(multiraftbase.Key, 0, 32)
	key = append(key, prefix...)
	key = helper.EncodeStringAscending(key, string(groupID))
	key = append(key, infix...)
	return key
}

func makeDataPrefix() multiraftbase.Key {
	key := make(multiraftbase.Key, 0)
	key = append(key, dataPrefix...)
	return key
}

func DataKey(logTerm, logIndex uint64) multiraftbase.Key {
	result := helper.EncodeUint64Ascending(makeDataPrefix(), logTerm)
	return helper.EncodeUint64Ascending(result, logIndex)
}

// MakeGroupIDPrefix creates a range-local key prefix from
// rangeID for both replicated and unreplicated data.
func MakeGroupIDPrefix(groupID multiraftbase.GroupID) multiraftbase.Key {
	return makePrefixWithGroupID(LocalGroupIDPrefix, groupID, nil)
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func (b GroupIDPrefixBuf) RaftAppliedIndexKey() multiraftbase.Key {
	return append(b.replicatedPrefix(), LocalRaftAppliedIndexSuffix...)
}

func GroupStatusKey(groupID multiraftbase.GroupID) multiraftbase.Key {
	return append(MakeGroupIDPrefixBuf(groupID).unreplicatedPrefix(), LocalGroupReplicaMembersSuffix...)
}

func (b GroupIDPrefixBuf) unreplicatedPrefix() multiraftbase.Key {
	return append(multiraftbase.Key(b), localGroupIDUnreplicatedInfix...)
}

// RangeReplicaDestroyedErrorKey returns a range-local key for
// the range's replica destroyed error.
func (b GroupIDPrefixBuf) GroupReplicaDestroyedErrorKey() multiraftbase.Key {
	return append(b.unreplicatedPrefix(), LocalGroupReplicaDestroyedErrorSuffix...)
}

// GroupDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func GroupDescriptorKey(groupID multiraftbase.GroupID) multiraftbase.Key {
	return MakeGroupIDPrefix(groupID)
}

// MakeRangeIDUnreplicatedPrefix creates a range-local key prefix from
// rangeID for all unreplicated data.
func MakeGroupIDUnreplicatedPrefix(groupID multiraftbase.GroupID) multiraftbase.Key {
	return makePrefixWithGroupID(LocalGroupIDPrefix, groupID, localGroupIDUnreplicatedInfix)
}
