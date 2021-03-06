// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: recorded_span.proto

/*
	Package tracing is a generated protocol buffer package.

	It is generated from these files:
		recorded_span.proto

	It has these top-level messages:
		RecordedSpan
*/
package tracing

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import time "time"

import github_com_gogo_protobuf_types "github.com/gogo/protobuf/types"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf
var _ = time.Kitchen

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// RecordedSpan is a span that is part of a recording. It can be transferred
// over the wire for snowball tracing.
type RecordedSpan struct {
	// ID of the trace; spans that are part of the same hierarchy share
	// the same trace ID.
	TraceID uint64 `protobuf:"varint,1,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	// ID of the span.
	SpanID uint64 `protobuf:"varint,2,opt,name=span_id,json=spanId,proto3" json:"span_id,omitempty"`
	// Span ID of the parent span.
	ParentSpanID uint64 `protobuf:"varint,3,opt,name=parent_span_id,json=parentSpanId,proto3" json:"parent_span_id,omitempty"`
	// Operation name.
	Operation string `protobuf:"bytes,4,opt,name=operation,proto3" json:"operation,omitempty"`
	// Baggage items get passed from parent to child spans (even through gRPC).
	// Notably, snowball tracing uses a special `sb` baggage item.
	Baggage map[string]string `protobuf:"bytes,5,rep,name=baggage" json:"baggage,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Tags associated with the span.
	Tags map[string]string `protobuf:"bytes,6,rep,name=tags" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Time when the span was started.
	StartTime time.Time `protobuf:"bytes,7,opt,name=start_time,json=startTime,stdtime" json:"start_time"`
	// Duration in nanoseconds; 0 if the span is not finished.
	Duration time.Duration `protobuf:"bytes,8,opt,name=duration,stdduration" json:"duration"`
	// Events logged in the span.
	Logs []RecordedSpan_LogRecord `protobuf:"bytes,9,rep,name=logs" json:"logs"`
}

func (m *RecordedSpan) Reset()                    { *m = RecordedSpan{} }
func (m *RecordedSpan) String() string            { return proto.CompactTextString(m) }
func (*RecordedSpan) ProtoMessage()               {}
func (*RecordedSpan) Descriptor() ([]byte, []int) { return fileDescriptorRecordedSpan, []int{0} }

func (m *RecordedSpan) GetTraceID() uint64 {
	if m != nil {
		return m.TraceID
	}
	return 0
}

func (m *RecordedSpan) GetSpanID() uint64 {
	if m != nil {
		return m.SpanID
	}
	return 0
}

func (m *RecordedSpan) GetParentSpanID() uint64 {
	if m != nil {
		return m.ParentSpanID
	}
	return 0
}

func (m *RecordedSpan) GetOperation() string {
	if m != nil {
		return m.Operation
	}
	return ""
}

func (m *RecordedSpan) GetBaggage() map[string]string {
	if m != nil {
		return m.Baggage
	}
	return nil
}

func (m *RecordedSpan) GetTags() map[string]string {
	if m != nil {
		return m.Tags
	}
	return nil
}

func (m *RecordedSpan) GetStartTime() time.Time {
	if m != nil {
		return m.StartTime
	}
	return time.Time{}
}

func (m *RecordedSpan) GetDuration() time.Duration {
	if m != nil {
		return m.Duration
	}
	return 0
}

func (m *RecordedSpan) GetLogs() []RecordedSpan_LogRecord {
	if m != nil {
		return m.Logs
	}
	return nil
}

type RecordedSpan_LogRecord struct {
	// Time of the log record.
	Time time.Time `protobuf:"bytes,1,opt,name=time,stdtime" json:"time"`
	// Fields with values converted to strings.
	Fields []RecordedSpan_LogRecord_Field `protobuf:"bytes,2,rep,name=fields" json:"fields"`
}

func (m *RecordedSpan_LogRecord) Reset()         { *m = RecordedSpan_LogRecord{} }
func (m *RecordedSpan_LogRecord) String() string { return proto.CompactTextString(m) }
func (*RecordedSpan_LogRecord) ProtoMessage()    {}
func (*RecordedSpan_LogRecord) Descriptor() ([]byte, []int) {
	return fileDescriptorRecordedSpan, []int{0, 2}
}

func (m *RecordedSpan_LogRecord) GetTime() time.Time {
	if m != nil {
		return m.Time
	}
	return time.Time{}
}

func (m *RecordedSpan_LogRecord) GetFields() []RecordedSpan_LogRecord_Field {
	if m != nil {
		return m.Fields
	}
	return nil
}

type RecordedSpan_LogRecord_Field struct {
	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *RecordedSpan_LogRecord_Field) Reset()         { *m = RecordedSpan_LogRecord_Field{} }
func (m *RecordedSpan_LogRecord_Field) String() string { return proto.CompactTextString(m) }
func (*RecordedSpan_LogRecord_Field) ProtoMessage()    {}
func (*RecordedSpan_LogRecord_Field) Descriptor() ([]byte, []int) {
	return fileDescriptorRecordedSpan, []int{0, 2, 0}
}

func (m *RecordedSpan_LogRecord_Field) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *RecordedSpan_LogRecord_Field) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterType((*RecordedSpan)(nil), "github.com.journeymidnight.nentropy.util.tracing.RecordedSpan")
	proto.RegisterType((*RecordedSpan_LogRecord)(nil), "github.com.journeymidnight.nentropy.util.tracing.RecordedSpan.LogRecord")
	proto.RegisterType((*RecordedSpan_LogRecord_Field)(nil), "github.com.journeymidnight.nentropy.util.tracing.RecordedSpan.LogRecord.Field")
}
func (m *RecordedSpan) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RecordedSpan) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.TraceID != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(m.TraceID))
	}
	if m.SpanID != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(m.SpanID))
	}
	if m.ParentSpanID != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(m.ParentSpanID))
	}
	if len(m.Operation) > 0 {
		dAtA[i] = 0x22
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(len(m.Operation)))
		i += copy(dAtA[i:], m.Operation)
	}
	if len(m.Baggage) > 0 {
		for k, _ := range m.Baggage {
			dAtA[i] = 0x2a
			i++
			v := m.Baggage[k]
			mapSize := 1 + len(k) + sovRecordedSpan(uint64(len(k))) + 1 + len(v) + sovRecordedSpan(uint64(len(v)))
			i = encodeVarintRecordedSpan(dAtA, i, uint64(mapSize))
			dAtA[i] = 0xa
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(len(k)))
			i += copy(dAtA[i:], k)
			dAtA[i] = 0x12
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(len(v)))
			i += copy(dAtA[i:], v)
		}
	}
	if len(m.Tags) > 0 {
		for k, _ := range m.Tags {
			dAtA[i] = 0x32
			i++
			v := m.Tags[k]
			mapSize := 1 + len(k) + sovRecordedSpan(uint64(len(k))) + 1 + len(v) + sovRecordedSpan(uint64(len(v)))
			i = encodeVarintRecordedSpan(dAtA, i, uint64(mapSize))
			dAtA[i] = 0xa
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(len(k)))
			i += copy(dAtA[i:], k)
			dAtA[i] = 0x12
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(len(v)))
			i += copy(dAtA[i:], v)
		}
	}
	dAtA[i] = 0x3a
	i++
	i = encodeVarintRecordedSpan(dAtA, i, uint64(github_com_gogo_protobuf_types.SizeOfStdTime(m.StartTime)))
	n1, err := github_com_gogo_protobuf_types.StdTimeMarshalTo(m.StartTime, dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n1
	dAtA[i] = 0x42
	i++
	i = encodeVarintRecordedSpan(dAtA, i, uint64(github_com_gogo_protobuf_types.SizeOfStdDuration(m.Duration)))
	n2, err := github_com_gogo_protobuf_types.StdDurationMarshalTo(m.Duration, dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n2
	if len(m.Logs) > 0 {
		for _, msg := range m.Logs {
			dAtA[i] = 0x4a
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *RecordedSpan_LogRecord) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RecordedSpan_LogRecord) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0xa
	i++
	i = encodeVarintRecordedSpan(dAtA, i, uint64(github_com_gogo_protobuf_types.SizeOfStdTime(m.Time)))
	n3, err := github_com_gogo_protobuf_types.StdTimeMarshalTo(m.Time, dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n3
	if len(m.Fields) > 0 {
		for _, msg := range m.Fields {
			dAtA[i] = 0x12
			i++
			i = encodeVarintRecordedSpan(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *RecordedSpan_LogRecord_Field) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RecordedSpan_LogRecord_Field) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Key) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(len(m.Key)))
		i += copy(dAtA[i:], m.Key)
	}
	if len(m.Value) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintRecordedSpan(dAtA, i, uint64(len(m.Value)))
		i += copy(dAtA[i:], m.Value)
	}
	return i, nil
}

func encodeFixed64RecordedSpan(dAtA []byte, offset int, v uint64) int {
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
func encodeFixed32RecordedSpan(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintRecordedSpan(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *RecordedSpan) Size() (n int) {
	var l int
	_ = l
	if m.TraceID != 0 {
		n += 1 + sovRecordedSpan(uint64(m.TraceID))
	}
	if m.SpanID != 0 {
		n += 1 + sovRecordedSpan(uint64(m.SpanID))
	}
	if m.ParentSpanID != 0 {
		n += 1 + sovRecordedSpan(uint64(m.ParentSpanID))
	}
	l = len(m.Operation)
	if l > 0 {
		n += 1 + l + sovRecordedSpan(uint64(l))
	}
	if len(m.Baggage) > 0 {
		for k, v := range m.Baggage {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovRecordedSpan(uint64(len(k))) + 1 + len(v) + sovRecordedSpan(uint64(len(v)))
			n += mapEntrySize + 1 + sovRecordedSpan(uint64(mapEntrySize))
		}
	}
	if len(m.Tags) > 0 {
		for k, v := range m.Tags {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovRecordedSpan(uint64(len(k))) + 1 + len(v) + sovRecordedSpan(uint64(len(v)))
			n += mapEntrySize + 1 + sovRecordedSpan(uint64(mapEntrySize))
		}
	}
	l = github_com_gogo_protobuf_types.SizeOfStdTime(m.StartTime)
	n += 1 + l + sovRecordedSpan(uint64(l))
	l = github_com_gogo_protobuf_types.SizeOfStdDuration(m.Duration)
	n += 1 + l + sovRecordedSpan(uint64(l))
	if len(m.Logs) > 0 {
		for _, e := range m.Logs {
			l = e.Size()
			n += 1 + l + sovRecordedSpan(uint64(l))
		}
	}
	return n
}

func (m *RecordedSpan_LogRecord) Size() (n int) {
	var l int
	_ = l
	l = github_com_gogo_protobuf_types.SizeOfStdTime(m.Time)
	n += 1 + l + sovRecordedSpan(uint64(l))
	if len(m.Fields) > 0 {
		for _, e := range m.Fields {
			l = e.Size()
			n += 1 + l + sovRecordedSpan(uint64(l))
		}
	}
	return n
}

func (m *RecordedSpan_LogRecord_Field) Size() (n int) {
	var l int
	_ = l
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovRecordedSpan(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovRecordedSpan(uint64(l))
	}
	return n
}

func sovRecordedSpan(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozRecordedSpan(x uint64) (n int) {
	return sovRecordedSpan(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RecordedSpan) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRecordedSpan
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
			return fmt.Errorf("proto: RecordedSpan: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RecordedSpan: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field TraceID", wireType)
			}
			m.TraceID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.TraceID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SpanID", wireType)
			}
			m.SpanID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SpanID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ParentSpanID", wireType)
			}
			m.ParentSpanID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ParentSpanID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Operation", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Operation = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Baggage", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Baggage == nil {
				m.Baggage = make(map[string]string)
			}
			var mapkey string
			var mapvalue string
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowRecordedSpan
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
				if fieldNum == 1 {
					var stringLenmapkey uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowRecordedSpan
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapkey |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapkey := int(stringLenmapkey)
					if intStringLenmapkey < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var stringLenmapvalue uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowRecordedSpan
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapvalue |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapvalue := int(stringLenmapvalue)
					if intStringLenmapvalue < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					postStringIndexmapvalue := iNdEx + intStringLenmapvalue
					if postStringIndexmapvalue > l {
						return io.ErrUnexpectedEOF
					}
					mapvalue = string(dAtA[iNdEx:postStringIndexmapvalue])
					iNdEx = postStringIndexmapvalue
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipRecordedSpan(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if skippy < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Baggage[mapkey] = mapvalue
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tags", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Tags == nil {
				m.Tags = make(map[string]string)
			}
			var mapkey string
			var mapvalue string
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowRecordedSpan
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
				if fieldNum == 1 {
					var stringLenmapkey uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowRecordedSpan
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapkey |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapkey := int(stringLenmapkey)
					if intStringLenmapkey < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var stringLenmapvalue uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowRecordedSpan
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapvalue |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapvalue := int(stringLenmapvalue)
					if intStringLenmapvalue < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					postStringIndexmapvalue := iNdEx + intStringLenmapvalue
					if postStringIndexmapvalue > l {
						return io.ErrUnexpectedEOF
					}
					mapvalue = string(dAtA[iNdEx:postStringIndexmapvalue])
					iNdEx = postStringIndexmapvalue
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipRecordedSpan(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if skippy < 0 {
						return ErrInvalidLengthRecordedSpan
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Tags[mapkey] = mapvalue
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartTime", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := github_com_gogo_protobuf_types.StdTimeUnmarshal(&m.StartTime, dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Duration", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := github_com_gogo_protobuf_types.StdDurationUnmarshal(&m.Duration, dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Logs", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Logs = append(m.Logs, RecordedSpan_LogRecord{})
			if err := m.Logs[len(m.Logs)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRecordedSpan(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRecordedSpan
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
func (m *RecordedSpan_LogRecord) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRecordedSpan
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
			return fmt.Errorf("proto: LogRecord: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: LogRecord: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Time", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := github_com_gogo_protobuf_types.StdTimeUnmarshal(&m.Time, dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Fields", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
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
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Fields = append(m.Fields, RecordedSpan_LogRecord_Field{})
			if err := m.Fields[len(m.Fields)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRecordedSpan(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRecordedSpan
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
func (m *RecordedSpan_LogRecord_Field) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRecordedSpan
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
			return fmt.Errorf("proto: Field: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Field: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecordedSpan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRecordedSpan
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRecordedSpan(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRecordedSpan
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
func skipRecordedSpan(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRecordedSpan
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
					return 0, ErrIntOverflowRecordedSpan
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
					return 0, ErrIntOverflowRecordedSpan
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
				return 0, ErrInvalidLengthRecordedSpan
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowRecordedSpan
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
				next, err := skipRecordedSpan(dAtA[start:])
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
	ErrInvalidLengthRecordedSpan = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRecordedSpan   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("recorded_span.proto", fileDescriptorRecordedSpan) }

var fileDescriptorRecordedSpan = []byte{
	// 527 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x92, 0xdd, 0x8a, 0xd3, 0x40,
	0x14, 0xc7, 0x77, 0xda, 0xb4, 0x69, 0x4e, 0x8b, 0x2c, 0xe3, 0x5e, 0x64, 0x83, 0xb4, 0x45, 0x41,
	0x7a, 0x35, 0x95, 0x15, 0x74, 0xd9, 0x1b, 0xb5, 0x56, 0xb1, 0x28, 0x22, 0xb1, 0x57, 0x22, 0x94,
	0x69, 0x33, 0x3b, 0x1b, 0x4d, 0x33, 0x61, 0x32, 0x11, 0xfa, 0x16, 0x5e, 0xee, 0xc3, 0xf8, 0x00,
	0x7b, 0xe9, 0x13, 0x54, 0x89, 0x3e, 0x88, 0xcc, 0x24, 0x69, 0x16, 0xbc, 0xb1, 0xb8, 0x77, 0x73,
	0xbe, 0x7e, 0xe7, 0x9c, 0xff, 0x19, 0xb8, 0x2d, 0xd9, 0x4a, 0xc8, 0x80, 0x05, 0x8b, 0x34, 0xa1,
	0x31, 0x49, 0xa4, 0x50, 0x02, 0x3f, 0xe0, 0xa1, 0xba, 0xc8, 0x96, 0x64, 0x25, 0xd6, 0xe4, 0x93,
	0xc8, 0x64, 0xcc, 0x36, 0xeb, 0x30, 0x88, 0x43, 0x7e, 0xa1, 0x48, 0xcc, 0x62, 0x25, 0x45, 0xb2,
	0x21, 0x99, 0x0a, 0x23, 0xa2, 0x24, 0x5d, 0x85, 0x31, 0xf7, 0x8e, 0xb8, 0xe0, 0xc2, 0x14, 0x8f,
	0xf5, 0xab, 0xe0, 0x78, 0xcf, 0x6a, 0x8e, 0x71, 0x8f, 0x8d, 0x7b, 0x99, 0x9d, 0xd7, 0x0f, 0x2e,
	0x04, 0x8f, 0x58, 0x6d, 0xab, 0x70, 0xcd, 0x52, 0x45, 0xd7, 0x49, 0x89, 0x78, 0xba, 0x3f, 0x22,
	0xc8, 0x24, 0x55, 0xa1, 0x28, 0x97, 0xb9, 0xfb, 0xcd, 0x86, 0x9e, 0x5f, 0x2e, 0xf9, 0x3e, 0xa1,
	0x31, 0xbe, 0x0f, 0x1d, 0x3d, 0x36, 0x5b, 0x84, 0x81, 0x8b, 0x86, 0x68, 0x64, 0x4d, 0xba, 0xf9,
	0x76, 0x60, 0xcf, 0xb5, 0x6f, 0x36, 0xf5, 0x6d, 0x13, 0x9c, 0x05, 0xf8, 0x1e, 0xd8, 0x5a, 0x13,
	0x9d, 0xd6, 0x30, 0x69, 0x90, 0x6f, 0x07, 0x6d, 0x8d, 0x98, 0x4d, 0xfd, 0xb6, 0x0e, 0xcd, 0x02,
	0xfc, 0x08, 0x6e, 0x25, 0x54, 0xb2, 0x58, 0x2d, 0xaa, 0xdc, 0xa6, 0xc9, 0x3d, 0xcc, 0xb7, 0x83,
	0xde, 0x3b, 0x13, 0x29, 0x2b, 0x7a, 0x49, 0x6d, 0x05, 0xf8, 0x0e, 0x38, 0x22, 0x61, 0xc5, 0xa0,
	0xae, 0x35, 0x44, 0x23, 0xc7, 0xaf, 0x1d, 0x98, 0x81, 0xbd, 0xa4, 0x9c, 0x53, 0xce, 0xdc, 0xd6,
	0xb0, 0x39, 0xea, 0x9e, 0xbc, 0x26, 0xfb, 0x9e, 0x84, 0x5c, 0xdf, 0x99, 0x4c, 0x0a, 0xda, 0x8b,
	0x58, 0xc9, 0x8d, 0x5f, 0xb1, 0xf1, 0x47, 0xb0, 0x14, 0xe5, 0xa9, 0xdb, 0x36, 0x3d, 0x5e, 0xfd,
	0x67, 0x8f, 0x39, 0xe5, 0x69, 0xd1, 0xc0, 0x50, 0xf1, 0x73, 0x80, 0x54, 0x51, 0xa9, 0x16, 0xfa,
	0xa6, 0xae, 0x3d, 0x44, 0xa3, 0xee, 0x89, 0x47, 0x8a, 0x6b, 0x91, 0xea, 0x5a, 0x64, 0x5e, 0x1d,
	0x7c, 0xd2, 0xb9, 0xda, 0x0e, 0x0e, 0xbe, 0xfe, 0x18, 0x20, 0xdf, 0x31, 0x75, 0x3a, 0x82, 0x9f,
	0x40, 0xa7, 0xba, 0xa7, 0xdb, 0x31, 0x88, 0xe3, 0xbf, 0x10, 0xd3, 0x32, 0xa1, 0x20, 0x5c, 0x6a,
	0xc2, 0xae, 0x08, 0x2f, 0xc1, 0x8a, 0x04, 0x4f, 0x5d, 0xe7, 0x46, 0x76, 0x7c, 0x23, 0x78, 0x61,
	0x4f, 0x2c, 0xdd, 0xcb, 0x37, 0x6c, 0xef, 0x0c, 0x7a, 0xd7, 0x05, 0xc6, 0x87, 0xd0, 0xfc, 0xcc,
	0x36, 0xe6, 0x73, 0x39, 0xbe, 0x7e, 0xe2, 0x23, 0x68, 0x7d, 0xa1, 0x51, 0xc6, 0xcc, 0x4f, 0x72,
	0xfc, 0xc2, 0x38, 0x6b, 0x9c, 0x22, 0xef, 0x31, 0x38, 0x3b, 0xe1, 0xf6, 0x2a, 0xfc, 0x8d, 0xc0,
	0xd9, 0x8d, 0x83, 0x4f, 0xc1, 0x32, 0x32, 0xa3, 0x3d, 0x64, 0x36, 0x15, 0x38, 0x82, 0xf6, 0x79,
	0xc8, 0xa2, 0x20, 0x75, 0x1b, 0x46, 0xa2, 0xb7, 0x37, 0x25, 0x11, 0x79, 0xa9, 0xb1, 0xa5, 0x50,
	0x65, 0x0f, 0x6f, 0x0c, 0x2d, 0xe3, 0xfe, 0xd7, 0x55, 0x27, 0xc7, 0x57, 0x79, 0x1f, 0x7d, 0xcf,
	0xfb, 0xe8, 0x67, 0xde, 0x47, 0x97, 0xbf, 0xfa, 0x07, 0x1f, 0xec, 0xb2, 0xed, 0xb2, 0x6d, 0xb6,
	0x7b, 0xf8, 0x27, 0x00, 0x00, 0xff, 0xff, 0x70, 0xdb, 0xcd, 0x4e, 0xc4, 0x04, 0x00, 0x00,
}
