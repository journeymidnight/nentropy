package multiraftbase

import (
	"strconv"

	"encoding/binary"
	"github.com/journeymidnight/nentropy/protos"
)

// CmdIDKey is a Raft command id.
type CmdIDKey string

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
// 0 is not a valid NodeID.
type NodeID int32

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// NewStoreNotFoundError initializes a new StoreNotFoundError.
func NewStoreNotFoundError(storeID StoreID) *protos.StoreNotFoundError {
	return &protos.StoreNotFoundError{
		StoreID: storeID,
	}
}

// ReplicaID is a custom type for a range replica ID.
type ReplicaID int32

// String implements the fmt.Stringer interface.
func (r ReplicaID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// GetReplicaDescriptor returns the replica which matches the specified store
// ID.
func (r GroupDescriptor) GetReplicaDescriptor(storeID StoreID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas {
		if repDesc.StoreID == storeID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// GetReplicaDescriptorByID returns the replica which matches the specified store
// ID.
func (r GroupDescriptor) GetReplicaDescriptorByID(replicaID ReplicaID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas {
		if repDesc.ReplicaID == replicaID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// IsInitialized returns false if this descriptor represents an
// uninitialized range.
// TODO(bdarnell): unify this with Validate().
func (r GroupDescriptor) IsInitialized() bool {
	return r.GroupID != ""
}

// Key is a custom type for a byte string in proto
// messages which refer to Cockroach keys.
type Key []byte

// SetInt encodes the specified int64 value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetInt(i int64) {
	v.RawBytes = make([]byte, headerSize+binary.MaxVarintLen64)
	n := binary.PutVarint(v.RawBytes[headerSize:], i)
	v.RawBytes = v.RawBytes[:headerSize+n]
	v.setTag(ValueType_INT)
}
