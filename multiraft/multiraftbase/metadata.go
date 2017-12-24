package multiraftbase

import (
	"strconv"

	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
)

// CmdIDKey is a Raft command id.
type CmdIDKey string

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return string(n)
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

const (
	checksumUninitialized = 0
	checksumSize          = 4
	tagPos                = checksumSize
	headerSize            = tagPos + 1
)

// ValueType defines a set of type constants placed in the "tag" field of Value
// messages. These are defined as a protocol buffer enumeration so that they
// can be used portably between our Go and C code. The tags are used by the
// RocksDB Merge Operator to perform specialized merges.
type ValueType int32

const (
	// This is a subset of the SQL column type values, representing the underlying
	// storage for various types. The DELIMITED_foo entries each represent a foo
	// variant that self-delimits length.
	ValueType_UNKNOWN           ValueType = 0
	ValueType_NULL              ValueType = 7
	ValueType_INT               ValueType = 1
	ValueType_FLOAT             ValueType = 2
	ValueType_BYTES             ValueType = 3
	ValueType_DELIMITED_BYTES   ValueType = 8
	ValueType_TIME              ValueType = 4
	ValueType_DECIMAL           ValueType = 5
	ValueType_DELIMITED_DECIMAL ValueType = 9
	ValueType_DURATION          ValueType = 6
	// TUPLE represents a DTuple, encoded as repeated pairs of varint field number
	// followed by a value encoded Datum.
	ValueType_TUPLE ValueType = 10
	// TIMESERIES is applied to values which contain InternalTimeSeriesData.
	ValueType_TIMESERIES ValueType = 100
)

// IsInitialized returns false if this descriptor represents an
// uninitialized range.
// TODO(bdarnell): unify this with Validate().
func (r GroupDescriptor) IsInitialized() bool {
	return r.GroupID != ""
}

type RKey Key

// Key is a custom type for a byte string in proto
// messages which refer to Cockroach keys.
type Key []byte

func (v *Value) setTag(t ValueType) {
	v.RawBytes[tagPos] = byte(t)
}

// SetInt encodes the specified int64 value into the bytes field of the
// receiver, sets the tag and clears the checksum.
func (v *Value) SetInt(i int64) {
	v.RawBytes = make([]byte, headerSize+binary.MaxVarintLen64)
	n := binary.PutVarint(v.RawBytes[headerSize:], i)
	v.RawBytes = v.RawBytes[:headerSize+n]
	v.setTag(ValueType_INT)
}

// GetTag retrieves the value type.
func (v Value) GetTag() ValueType {
	if len(v.RawBytes) <= tagPos {
		return ValueType_UNKNOWN
	}
	return ValueType(v.RawBytes[tagPos])
}

func (v Value) dataBytes() []byte {
	return v.RawBytes[headerSize:]
}

// GetInt decodes an int64 value from the bytes field of the receiver. If the
// tag is not INT or the value cannot be decoded an error will be returned.
func (v Value) GetInt() (int64, error) {
	if tag := v.GetTag(); tag != ValueType_INT {
		return 0, fmt.Errorf("value type is not %s: %s", ValueType_INT, tag)
	}
	i, n := binary.Varint(v.dataBytes())
	if n <= 0 {
		return 0, fmt.Errorf("int64 varint decoding failed: %d", n)
	}
	return i, nil
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
