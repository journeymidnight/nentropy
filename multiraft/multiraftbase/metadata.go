package multiraftbase

import (
	"strconv"

	"encoding/binary"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/pkg/errors"
)

// CmdIDKey is a Raft command id.
type CmdIDKey string

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
func (r GroupDescriptor) GetReplicaDescriptor(nodeID NodeID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas {
		if repDesc.NodeID == nodeID {
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

// Validate performs some basic validation of the contents of a replica descriptor.
func (r ReplicaDescriptor) Validate() error {
	if r.NodeID == "" {
		return errors.Errorf("NodeID must not be empty")
	}
	if r.StoreID == 0 {
		return errors.Errorf("StoreID must not be zero")
	}
	if r.ReplicaID == 0 {
		return errors.Errorf("ReplicaID must not be zero")
	}
	return nil
}

// Validate performs some basic validation of the contents of a range descriptor.
func (r GroupDescriptor) Validate() error {
	if r.PoolId == 0 {
		return errors.Errorf("PoolId must be non-zero")
	}
	seen := map[ReplicaID]struct{}{}
	for i, rep := range r.Replicas {
		if err := rep.Validate(); err != nil {
			return errors.Errorf("replica %d is invalid: %s", i, err)
		}
		if _, ok := seen[rep.ReplicaID]; ok {
			return errors.Errorf("ReplicaID %d was reused", rep.ReplicaID)
		}
		seen[rep.ReplicaID] = struct{}{}
		if rep.ReplicaID >= r.NextReplicaID {
			return errors.Errorf("ReplicaID %d must be less than NextReplicaID %d",
				rep.ReplicaID, r.NextReplicaID)
		}
	}
	return nil
}
