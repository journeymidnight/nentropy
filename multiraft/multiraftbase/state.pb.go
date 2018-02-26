// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: state.proto

package multiraftbase

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type RaftTruncatedState struct {
	// The highest index that has been removed from the log.
	Index uint64 `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	// The term corresponding to 'index'.
	Term uint64 `protobuf:"varint,2,opt,name=term,proto3" json:"term,omitempty"`
}

func (m *RaftTruncatedState) Reset()                    { *m = RaftTruncatedState{} }
func (m *RaftTruncatedState) String() string            { return proto.CompactTextString(m) }
func (*RaftTruncatedState) ProtoMessage()               {}
func (*RaftTruncatedState) Descriptor() ([]byte, []int) { return fileDescriptorState, []int{0} }

func (m *RaftTruncatedState) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *RaftTruncatedState) GetTerm() uint64 {
	if m != nil {
		return m.Term
	}
	return 0
}

type Stats struct {
	KeyBytes   int64 `protobuf:"varint,1,opt,name=key_bytes,json=keyBytes,proto3" json:"key_bytes,omitempty"`
	ValBytes   int64 `protobuf:"varint,2,opt,name=val_bytes,json=valBytes,proto3" json:"val_bytes,omitempty"`
	TotalBytes int64 `protobuf:"varint,3,opt,name=total_bytes,json=totalBytes,proto3" json:"total_bytes,omitempty"`
}

func (m *Stats) Reset()                    { *m = Stats{} }
func (m *Stats) String() string            { return proto.CompactTextString(m) }
func (*Stats) ProtoMessage()               {}
func (*Stats) Descriptor() ([]byte, []int) { return fileDescriptorState, []int{1} }

func (m *Stats) GetKeyBytes() int64 {
	if m != nil {
		return m.KeyBytes
	}
	return 0
}

func (m *Stats) GetValBytes() int64 {
	if m != nil {
		return m.ValBytes
	}
	return 0
}

func (m *Stats) GetTotalBytes() int64 {
	if m != nil {
		return m.TotalBytes
	}
	return 0
}

type ReplicaState struct {
	RaftAppliedIndex uint64           `protobuf:"varint,1,opt,name=raft_applied_index,json=raftAppliedIndex,proto3" json:"raft_applied_index,omitempty"`
	Desc             *GroupDescriptor `protobuf:"bytes,2,opt,name=desc" json:"desc,omitempty"`
	// The truncation state of the Raft log.
	TruncatedState *RaftTruncatedState `protobuf:"bytes,3,opt,name=truncated_state,json=truncatedState" json:"truncated_state,omitempty"`
	Stats          Stats               `protobuf:"bytes,4,opt,name=Stats" json:"Stats"`
}

func (m *ReplicaState) Reset()                    { *m = ReplicaState{} }
func (m *ReplicaState) String() string            { return proto.CompactTextString(m) }
func (*ReplicaState) ProtoMessage()               {}
func (*ReplicaState) Descriptor() ([]byte, []int) { return fileDescriptorState, []int{2} }

func (m *ReplicaState) GetRaftAppliedIndex() uint64 {
	if m != nil {
		return m.RaftAppliedIndex
	}
	return 0
}

func (m *ReplicaState) GetDesc() *GroupDescriptor {
	if m != nil {
		return m.Desc
	}
	return nil
}

func (m *ReplicaState) GetTruncatedState() *RaftTruncatedState {
	if m != nil {
		return m.TruncatedState
	}
	return nil
}

func (m *ReplicaState) GetStats() Stats {
	if m != nil {
		return m.Stats
	}
	return Stats{}
}

type PgInfo struct {
	ReplicaState `protobuf:"bytes,1,opt,name=state,embedded=state" json:"state"`
	// The highest (and last) index in the Raft log.
	LastIndex  uint64 `protobuf:"varint,2,opt,name=lastIndex,proto3" json:"lastIndex,omitempty"`
	NumPending uint64 `protobuf:"varint,3,opt,name=num_pending,json=numPending,proto3" json:"num_pending,omitempty"`
	NumDropped uint64 `protobuf:"varint,5,opt,name=num_dropped,json=numDropped,proto3" json:"num_dropped,omitempty"`
	// raft_log_size may be initially inaccurate after a server restart.
	// See storage.Replica.mu.raftLogSize.
	RaftLogSize int64 `protobuf:"varint,6,opt,name=raft_log_size,json=raftLogSize,proto3" json:"raft_log_size,omitempty"`
	// Approximately the amount of quota available.
	ApproximateProposalQuota int64 `protobuf:"varint,7,opt,name=approximate_proposal_quota,json=approximateProposalQuota,proto3" json:"approximate_proposal_quota,omitempty"`
}

func (m *PgInfo) Reset()                    { *m = PgInfo{} }
func (m *PgInfo) String() string            { return proto.CompactTextString(m) }
func (*PgInfo) ProtoMessage()               {}
func (*PgInfo) Descriptor() ([]byte, []int) { return fileDescriptorState, []int{3} }

func (m *PgInfo) GetLastIndex() uint64 {
	if m != nil {
		return m.LastIndex
	}
	return 0
}

func (m *PgInfo) GetNumPending() uint64 {
	if m != nil {
		return m.NumPending
	}
	return 0
}

func (m *PgInfo) GetNumDropped() uint64 {
	if m != nil {
		return m.NumDropped
	}
	return 0
}

func (m *PgInfo) GetRaftLogSize() int64 {
	if m != nil {
		return m.RaftLogSize
	}
	return 0
}

func (m *PgInfo) GetApproximateProposalQuota() int64 {
	if m != nil {
		return m.ApproximateProposalQuota
	}
	return 0
}

func init() {
	proto.RegisterType((*RaftTruncatedState)(nil), "multiraftbase.RaftTruncatedState")
	proto.RegisterType((*Stats)(nil), "multiraftbase.Stats")
	proto.RegisterType((*ReplicaState)(nil), "multiraftbase.ReplicaState")
	proto.RegisterType((*PgInfo)(nil), "multiraftbase.PgInfo")
}
func (this *RaftTruncatedState) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*RaftTruncatedState)
	if !ok {
		that2, ok := that.(RaftTruncatedState)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if this.Index != that1.Index {
		return false
	}
	if this.Term != that1.Term {
		return false
	}
	return true
}
func (this *Stats) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*Stats)
	if !ok {
		that2, ok := that.(Stats)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if this.KeyBytes != that1.KeyBytes {
		return false
	}
	if this.ValBytes != that1.ValBytes {
		return false
	}
	if this.TotalBytes != that1.TotalBytes {
		return false
	}
	return true
}
func (this *ReplicaState) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*ReplicaState)
	if !ok {
		that2, ok := that.(ReplicaState)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if this.RaftAppliedIndex != that1.RaftAppliedIndex {
		return false
	}
	if !this.Desc.Equal(that1.Desc) {
		return false
	}
	if !this.TruncatedState.Equal(that1.TruncatedState) {
		return false
	}
	if !this.Stats.Equal(&that1.Stats) {
		return false
	}
	return true
}
func (this *PgInfo) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*PgInfo)
	if !ok {
		that2, ok := that.(PgInfo)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if !this.ReplicaState.Equal(&that1.ReplicaState) {
		return false
	}
	if this.LastIndex != that1.LastIndex {
		return false
	}
	if this.NumPending != that1.NumPending {
		return false
	}
	if this.NumDropped != that1.NumDropped {
		return false
	}
	if this.RaftLogSize != that1.RaftLogSize {
		return false
	}
	if this.ApproximateProposalQuota != that1.ApproximateProposalQuota {
		return false
	}
	return true
}
func (m *RaftTruncatedState) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RaftTruncatedState) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Index != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintState(dAtA, i, uint64(m.Index))
	}
	if m.Term != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintState(dAtA, i, uint64(m.Term))
	}
	return i, nil
}

func (m *Stats) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Stats) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.KeyBytes != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintState(dAtA, i, uint64(m.KeyBytes))
	}
	if m.ValBytes != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintState(dAtA, i, uint64(m.ValBytes))
	}
	if m.TotalBytes != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintState(dAtA, i, uint64(m.TotalBytes))
	}
	return i, nil
}

func (m *ReplicaState) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReplicaState) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.RaftAppliedIndex != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintState(dAtA, i, uint64(m.RaftAppliedIndex))
	}
	if m.Desc != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintState(dAtA, i, uint64(m.Desc.Size()))
		n1, err := m.Desc.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.TruncatedState != nil {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintState(dAtA, i, uint64(m.TruncatedState.Size()))
		n2, err := m.TruncatedState.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	dAtA[i] = 0x22
	i++
	i = encodeVarintState(dAtA, i, uint64(m.Stats.Size()))
	n3, err := m.Stats.MarshalTo(dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n3
	return i, nil
}

func (m *PgInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PgInfo) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0xa
	i++
	i = encodeVarintState(dAtA, i, uint64(m.ReplicaState.Size()))
	n4, err := m.ReplicaState.MarshalTo(dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n4
	if m.LastIndex != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintState(dAtA, i, uint64(m.LastIndex))
	}
	if m.NumPending != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintState(dAtA, i, uint64(m.NumPending))
	}
	if m.NumDropped != 0 {
		dAtA[i] = 0x28
		i++
		i = encodeVarintState(dAtA, i, uint64(m.NumDropped))
	}
	if m.RaftLogSize != 0 {
		dAtA[i] = 0x30
		i++
		i = encodeVarintState(dAtA, i, uint64(m.RaftLogSize))
	}
	if m.ApproximateProposalQuota != 0 {
		dAtA[i] = 0x38
		i++
		i = encodeVarintState(dAtA, i, uint64(m.ApproximateProposalQuota))
	}
	return i, nil
}

func encodeFixed64State(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32State(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintState(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func NewPopulatedRaftTruncatedState(r randyState, easy bool) *RaftTruncatedState {
	this := &RaftTruncatedState{}
	this.Index = uint64(uint64(r.Uint32()))
	this.Term = uint64(uint64(r.Uint32()))
	if !easy && r.Intn(10) != 0 {
	}
	return this
}

func NewPopulatedStats(r randyState, easy bool) *Stats {
	this := &Stats{}
	this.KeyBytes = int64(r.Int63())
	if r.Intn(2) == 0 {
		this.KeyBytes *= -1
	}
	this.ValBytes = int64(r.Int63())
	if r.Intn(2) == 0 {
		this.ValBytes *= -1
	}
	this.TotalBytes = int64(r.Int63())
	if r.Intn(2) == 0 {
		this.TotalBytes *= -1
	}
	if !easy && r.Intn(10) != 0 {
	}
	return this
}

type randyState interface {
	Float32() float32
	Float64() float64
	Int63() int64
	Int31() int32
	Uint32() uint32
	Intn(n int) int
}

func randUTF8RuneState(r randyState) rune {
	ru := r.Intn(62)
	if ru < 10 {
		return rune(ru + 48)
	} else if ru < 36 {
		return rune(ru + 55)
	}
	return rune(ru + 61)
}
func randStringState(r randyState) string {
	v1 := r.Intn(100)
	tmps := make([]rune, v1)
	for i := 0; i < v1; i++ {
		tmps[i] = randUTF8RuneState(r)
	}
	return string(tmps)
}
func randUnrecognizedState(r randyState, maxFieldNumber int) (dAtA []byte) {
	l := r.Intn(5)
	for i := 0; i < l; i++ {
		wire := r.Intn(4)
		if wire == 3 {
			wire = 5
		}
		fieldNumber := maxFieldNumber + r.Intn(100)
		dAtA = randFieldState(dAtA, r, fieldNumber, wire)
	}
	return dAtA
}
func randFieldState(dAtA []byte, r randyState, fieldNumber int, wire int) []byte {
	key := uint32(fieldNumber)<<3 | uint32(wire)
	switch wire {
	case 0:
		dAtA = encodeVarintPopulateState(dAtA, uint64(key))
		v2 := r.Int63()
		if r.Intn(2) == 0 {
			v2 *= -1
		}
		dAtA = encodeVarintPopulateState(dAtA, uint64(v2))
	case 1:
		dAtA = encodeVarintPopulateState(dAtA, uint64(key))
		dAtA = append(dAtA, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	case 2:
		dAtA = encodeVarintPopulateState(dAtA, uint64(key))
		ll := r.Intn(100)
		dAtA = encodeVarintPopulateState(dAtA, uint64(ll))
		for j := 0; j < ll; j++ {
			dAtA = append(dAtA, byte(r.Intn(256)))
		}
	default:
		dAtA = encodeVarintPopulateState(dAtA, uint64(key))
		dAtA = append(dAtA, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	}
	return dAtA
}
func encodeVarintPopulateState(dAtA []byte, v uint64) []byte {
	for v >= 1<<7 {
		dAtA = append(dAtA, uint8(uint64(v)&0x7f|0x80))
		v >>= 7
	}
	dAtA = append(dAtA, uint8(v))
	return dAtA
}
func (m *RaftTruncatedState) Size() (n int) {
	var l int
	_ = l
	if m.Index != 0 {
		n += 1 + sovState(uint64(m.Index))
	}
	if m.Term != 0 {
		n += 1 + sovState(uint64(m.Term))
	}
	return n
}

func (m *Stats) Size() (n int) {
	var l int
	_ = l
	if m.KeyBytes != 0 {
		n += 1 + sovState(uint64(m.KeyBytes))
	}
	if m.ValBytes != 0 {
		n += 1 + sovState(uint64(m.ValBytes))
	}
	if m.TotalBytes != 0 {
		n += 1 + sovState(uint64(m.TotalBytes))
	}
	return n
}

func (m *ReplicaState) Size() (n int) {
	var l int
	_ = l
	if m.RaftAppliedIndex != 0 {
		n += 1 + sovState(uint64(m.RaftAppliedIndex))
	}
	if m.Desc != nil {
		l = m.Desc.Size()
		n += 1 + l + sovState(uint64(l))
	}
	if m.TruncatedState != nil {
		l = m.TruncatedState.Size()
		n += 1 + l + sovState(uint64(l))
	}
	l = m.Stats.Size()
	n += 1 + l + sovState(uint64(l))
	return n
}

func (m *PgInfo) Size() (n int) {
	var l int
	_ = l
	l = m.ReplicaState.Size()
	n += 1 + l + sovState(uint64(l))
	if m.LastIndex != 0 {
		n += 1 + sovState(uint64(m.LastIndex))
	}
	if m.NumPending != 0 {
		n += 1 + sovState(uint64(m.NumPending))
	}
	if m.NumDropped != 0 {
		n += 1 + sovState(uint64(m.NumDropped))
	}
	if m.RaftLogSize != 0 {
		n += 1 + sovState(uint64(m.RaftLogSize))
	}
	if m.ApproximateProposalQuota != 0 {
		n += 1 + sovState(uint64(m.ApproximateProposalQuota))
	}
	return n
}

func sovState(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozState(x uint64) (n int) {
	return sovState(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RaftTruncatedState) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RaftTruncatedState: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RaftTruncatedState: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Index", wireType)
			}
			m.Index = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Index |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Term", wireType)
			}
			m.Term = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Term |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Stats) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Stats: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Stats: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field KeyBytes", wireType)
			}
			m.KeyBytes = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.KeyBytes |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ValBytes", wireType)
			}
			m.ValBytes = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ValBytes |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field TotalBytes", wireType)
			}
			m.TotalBytes = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.TotalBytes |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReplicaState) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ReplicaState: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ReplicaState: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field RaftAppliedIndex", wireType)
			}
			m.RaftAppliedIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.RaftAppliedIndex |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Desc", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Desc == nil {
				m.Desc = &GroupDescriptor{}
			}
			if err := m.Desc.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TruncatedState", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.TruncatedState == nil {
				m.TruncatedState = &RaftTruncatedState{}
			}
			if err := m.TruncatedState.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Stats", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.Stats.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PgInfo) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PgInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PgInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ReplicaState", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.ReplicaState.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field LastIndex", wireType)
			}
			m.LastIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.LastIndex |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NumPending", wireType)
			}
			m.NumPending = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NumPending |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NumDropped", wireType)
			}
			m.NumDropped = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NumDropped |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field RaftLogSize", wireType)
			}
			m.RaftLogSize = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.RaftLogSize |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ApproximateProposalQuota", wireType)
			}
			m.ApproximateProposalQuota = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ApproximateProposalQuota |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipState(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowState
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowState
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowState
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthState
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowState
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipState(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthState = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowState   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("state.proto", fileDescriptorState) }

var fileDescriptorState = []byte{
	// 492 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x53, 0xc1, 0x6e, 0xd3, 0x40,
	0x10, 0xed, 0xa6, 0x76, 0x48, 0xd7, 0xb4, 0x54, 0xab, 0x1c, 0xac, 0x14, 0xd9, 0x90, 0x13, 0x07,
	0x14, 0x50, 0xb8, 0x15, 0x2e, 0x44, 0x95, 0xa0, 0x15, 0x87, 0xe0, 0x72, 0xe2, 0x62, 0x6d, 0xe2,
	0x8d, 0x65, 0xd5, 0xf6, 0x2e, 0xbb, 0xe3, 0xaa, 0xe9, 0x6f, 0x70, 0xe1, 0xd8, 0xcf, 0xe0, 0x13,
	0x7a, 0xec, 0x17, 0x54, 0x28, 0x5c, 0xf8, 0x00, 0x3e, 0x00, 0xed, 0x6c, 0x5a, 0x92, 0xf4, 0x36,
	0xfb, 0xde, 0x9b, 0xc9, 0x9b, 0x37, 0x31, 0x0d, 0x0c, 0x70, 0x10, 0x03, 0xa5, 0x25, 0x48, 0xb6,
	0x5b, 0x35, 0x25, 0x14, 0x9a, 0xcf, 0x60, 0xc2, 0x8d, 0xe8, 0xed, 0x55, 0x02, 0x78, 0xc6, 0x81,
	0x3b, 0xba, 0xd7, 0xcd, 0x65, 0x2e, 0xb1, 0x7c, 0x65, 0x2b, 0x87, 0xf6, 0x3f, 0x52, 0x96, 0xf0,
	0x19, 0x7c, 0xd1, 0x4d, 0x3d, 0xe5, 0x20, 0xb2, 0x53, 0x3b, 0x90, 0x75, 0xa9, 0x5f, 0xd4, 0x99,
	0xb8, 0x08, 0xc9, 0x33, 0xf2, 0xc2, 0x4b, 0xdc, 0x83, 0x31, 0xea, 0x81, 0xd0, 0x55, 0xd8, 0x42,
	0x10, 0xeb, 0xc3, 0xce, 0xcf, 0xab, 0x98, 0xfc, 0xb9, 0x8a, 0x49, 0xbf, 0xa4, 0xbe, 0x6d, 0x36,
	0xec, 0x80, 0xee, 0x9c, 0x89, 0x79, 0x3a, 0x99, 0x83, 0x30, 0x38, 0x60, 0x3b, 0xe9, 0x9c, 0x89,
	0xf9, 0xc8, 0xbe, 0x2d, 0x79, 0xce, 0xcb, 0x25, 0xd9, 0x72, 0xe4, 0x39, 0x2f, 0x1d, 0x19, 0xd3,
	0x00, 0x24, 0xdc, 0xd3, 0xdb, 0x48, 0x53, 0x84, 0x50, 0xb0, 0xf2, 0x6b, 0x7f, 0x09, 0x7d, 0x9c,
	0x08, 0x55, 0x16, 0x53, 0xee, 0x2c, 0xbf, 0xa4, 0xcc, 0xae, 0x9e, 0x72, 0xa5, 0xca, 0x42, 0x64,
	0xe9, 0xaa, 0xff, 0x7d, 0xcb, 0xbc, 0x77, 0xc4, 0x31, 0xae, 0x32, 0xa4, 0x5e, 0x26, 0xcc, 0x14,
	0x1d, 0x04, 0xc3, 0x68, 0xb0, 0x16, 0xdd, 0xe0, 0x83, 0x96, 0x8d, 0x3a, 0x12, 0x66, 0xaa, 0x0b,
	0x05, 0x52, 0x27, 0xa8, 0x65, 0x27, 0xf4, 0x09, 0xdc, 0xc5, 0x94, 0x62, 0xf0, 0xe8, 0x30, 0x18,
	0x3e, 0xdf, 0x68, 0x7f, 0x18, 0x68, 0xb2, 0x07, 0xeb, 0x01, 0xbf, 0x5e, 0x86, 0x15, 0x7a, 0x38,
	0xa1, 0xbb, 0x31, 0x01, 0xb9, 0x91, 0x77, 0x7d, 0x1b, 0x6f, 0x25, 0x4e, 0x78, 0xe8, 0xe1, 0xda,
	0xdf, 0x5b, 0xb4, 0x3d, 0xce, 0x8f, 0xeb, 0x99, 0x64, 0x6f, 0xa9, 0xef, 0x4c, 0x10, 0x1c, 0x71,
	0xb0, 0x69, 0x62, 0x25, 0x9c, 0x51, 0xc7, 0x4e, 0xba, 0xb9, 0x8d, 0x49, 0xe2, 0x7a, 0xd8, 0x53,
	0xba, 0x53, 0x72, 0x03, 0x18, 0xc6, 0xf2, 0x9e, 0xff, 0x01, 0x7b, 0x87, 0xba, 0xa9, 0x52, 0x25,
	0xea, 0xac, 0xa8, 0x73, 0xdc, 0xd2, 0x4b, 0x68, 0xdd, 0x54, 0x63, 0x87, 0xdc, 0x09, 0x32, 0x2d,
	0x95, 0x12, 0x59, 0xe8, 0xdf, 0x0b, 0x8e, 0x1c, 0xc2, 0xfa, 0x74, 0x17, 0xaf, 0x51, 0xca, 0x3c,
	0x35, 0xc5, 0xa5, 0x08, 0xdb, 0x78, 0xcb, 0xc0, 0x82, 0x9f, 0x64, 0x7e, 0x5a, 0x5c, 0x0a, 0xf6,
	0x8e, 0xf6, 0xb8, 0x52, 0x5a, 0x5e, 0x14, 0x15, 0x07, 0x91, 0x2a, 0x2d, 0x95, 0x34, 0xbc, 0x4c,
	0xbf, 0x35, 0x12, 0x78, 0xf8, 0x08, 0x1b, 0xc2, 0x15, 0xc5, 0x78, 0x29, 0xf8, 0x6c, 0x79, 0x97,
	0xc7, 0x89, 0xd7, 0xf1, 0xf6, 0xfd, 0x51, 0x7c, 0xbd, 0x88, 0xc8, 0xcd, 0x22, 0x22, 0xbf, 0x16,
	0x11, 0xf9, 0xf1, 0x3b, 0xda, 0xfa, 0xba, 0xfe, 0x2d, 0x4c, 0xda, 0xf8, 0x67, 0x7f, 0xf3, 0x2f,
	0x00, 0x00, 0xff, 0xff, 0x5e, 0xd0, 0xef, 0x0b, 0x30, 0x03, 0x00, 0x00,
}
