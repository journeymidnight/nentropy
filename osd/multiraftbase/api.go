package multiraftbase

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
)

type Method int

//go:generate stringer -type=Method
const (
	// Get fetches the value for a key from the KV map, respecting a
	// possibly historical timestamp. If the timestamp is 0, returns
	// the most recent value.
	Get Method = iota
	// Put sets the value for a key at the specified timestamp. If the
	// timestamp is 0, the value is set with the current time as timestamp.
	Put
	// TruncateLog discards a prefix of the raft log.
	TruncateLog
	// HasKey check that the key exist
	HasKey
	// Delete removes the value for the specified key.
	Delete
	// change raft conf
	ChangeConf
)

// Request is an interface for RPC requests.
type Request interface {
	proto.Message
	// Method returns the request method.
	Method() Method
	// ShallowCopy returns a shallow copy of the receiver.
	ShallowCopy() Request
	flags() int
}

// Method implements the Request interface.
func (*GetRequest) Method() Method { return Get }

// ShallowCopy implements the Request interface.
func (gr *GetRequest) ShallowCopy() Request {
	shallowCopy := *gr
	return &shallowCopy
}

func (*GetRequest) flags() int { return 0 }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

// ShallowCopy implements the Request interface.
func (pr *PutRequest) ShallowCopy() Request {
	shallowCopy := *pr
	return &shallowCopy
}

func (*PutRequest) flags() int { return 0 }

// Method implements the Request interface.
func (*DeleteRequest) Method() Method { return Delete }

// ShallowCopy implements the Request interface.
func (gr *DeleteRequest) ShallowCopy() Request {
	shallowCopy := *gr
	return &shallowCopy
}

func (*DeleteRequest) flags() int { return 0 }

// Method implements the Request interface.
func (*TruncateLogRequest) Method() Method { return TruncateLog }

// ShallowCopy implements the Request interface.
func (tr *TruncateLogRequest) ShallowCopy() Request {
	shallowCopy := *tr
	return &shallowCopy
}

func (*TruncateLogRequest) flags() int { return 0 }

// Method implements the Request interface.
func (*ChangeConfRequest) Method() Method { return ChangeConf }

// ShallowCopy implements the Request interface.
func (tr *ChangeConfRequest) ShallowCopy() Request {
	shallowCopy := *tr
	return &shallowCopy
}

func (*ChangeConfRequest) flags() int { return 0 }

func (*HasKeyRequest) flags() int { return 0 }

// Method implements the Request interface.
func (*HasKeyRequest) Method() Method { return HasKey }

// ShallowCopy implements the Request interface.
func (tr *HasKeyRequest) ShallowCopy() Request {
	shallowCopy := *tr
	return &shallowCopy
}

// NewGet returns a Request initialized to get the value at key.
func NewGet(key Key, offset int64, len uint64) Request {
	return &GetRequest{
		Key:   key,
		Value: Value{Offset: offset, Len: len},
	}
}

// NewPut returns a Request initialized to put the value at key.
func NewPut(key Key, value Value) Request {
	return &PutRequest{
		Key:   key,
		Value: value,
	}
}

// GetInner returns the Request contained in the union.
func (ru RequestUnion) GetInner() Request {
	return ru.GetValue().(Request)
}

// GetInner returns the Response contained in the union.
func (ru ResponseUnion) GetInner() Response {
	return ru.GetValue().(Response)
}

// MustSetInner sets the Request contained in the union. It panics if the
// request is not recognized by the union type. The RequestUnion is reset
// before being repopulated.
func (ru *ResponseUnion) MustSetInner(args Response) {
	ru.Reset()
	if !ru.SetValue(args) {
		panic(fmt.Sprintf("%T excludes %T", ru, args))
	}
}

// MustSetInner sets the Request contained in the union. It panics if the
// request is not recognized by the union type. The RequestUnion is reset
// before being repopulated.
func (ru *RequestUnion) MustSetInner(args Request) {
	ru.Reset()
	if !ru.SetValue(args) {
		panic(fmt.Sprintf("%T excludes %T", ru, args))
	}
}

// Response is an interface for RPC result.
type Response interface {
	proto.Message
	// Method returns the request method.
	Method() Method
	// Header returns the response header.
	Header() ResponseHeader
	// SetHeader sets the response header.
	SetHeader(ResponseHeader)
}

// Method implements the Request interface.
func (*GetResponse) Method() Method { return Get }

// Method implements the Request interface.
func (*PutResponse) Method() Method { return Put }

// Method implements the Request interface.
func (*HasKeyResponse) Method() Method { return HasKey }

// Method implements the Request interface.
func (*DeleteResponse) Method() Method { return Delete }

// Method implements the Request interface.
func (*TruncateLogResponse) Method() Method { return TruncateLog }

// Method implements the Request interface.
func (*ChangeConfResponse) Method() Method { return ChangeConf }

// Header implements the Response interface for ResponseHeader.
func (rh ResponseHeader) Header() ResponseHeader {
	return rh
}

// SetHeader implements the Response interface.
func (rh *ResponseHeader) SetHeader(other ResponseHeader) {
	*rh = other
}
