package multiraftbase

import "fmt"

type ErrorDetailInterface interface {
	error
	// message returns an error message.
	message(*Error) string
}

type internalError Error

func (e *internalError) Error() string {
	return (*Error)(e).String()
}

func (e *internalError) message(_ *Error) string {
	return (*Error)(e).String()
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// NewError creates an Error from the given error.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{}
	if intErr, ok := err.(*internalError); ok {
		*e = *(*Error)(intErr)
	} else {
		e.setGoError(err)
	}

	return e
}

// setGoError sets Error using err.
func (e *Error) setGoError(err error) {
	if e.Message != "" {
		panic("cannot re-use roachpb.Error")
	}
	if sErr, ok := err.(ErrorDetailInterface); ok {
		e.Message = sErr.message(e)
	} else {
		e.Message = err.Error()
	}

	// If the specific error type exists in the detail union, set it.
	detail := &ErrorDetail{}
	if detail.SetValue(err) {
		e.Detail = detail
	}
	return
}

func NewNodeNotReadyError(nodeID NodeID) *NodeNotReadyError {
	return &NodeNotReadyError{
		NodeID: nodeID,
	}
}

func (e *NodeNotReadyError) Error() string {
	return e.message(nil)
}

func (e *NodeNotReadyError) message(_ *Error) string {
	return fmt.Sprintf("node %s was not ready", e.NodeID)
}

// NewGroupNotFoundError initializes a new GroupNotFoundError.
func NewGroupNotFoundError(groupID GroupID) *GroupNotFoundError {
	return &GroupNotFoundError{
		GroupID: groupID,
	}
}

func (e *GroupNotFoundError) Error() string {
	return e.message(nil)
}

func (e *GroupNotFoundError) message(_ *Error) string {
	return fmt.Sprintf("r%s was not found", e.GroupID)
}

// NewAmbiguousResultError initializes a new AmbiguousResultError with
// an explanatory message.
func NewAmbiguousResultError(msg string) *AmbiguousResultError {
	return &AmbiguousResultError{Message: msg}
}

func (e *AmbiguousResultError) Error() string {
	return e.message(nil)
}

func (e *AmbiguousResultError) message(_ *Error) string {
	return fmt.Sprintf("result is ambiguous (%s)", e.Message)
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// NewStoreNotFoundError initializes a new StoreNotFoundError.
func NewStoreNotFoundError(storeID StoreID) *StoreNotFoundError {
	return &StoreNotFoundError{
		StoreID: storeID,
	}
}

func (e *StoreNotFoundError) Error() string {
	return e.message(nil)
}

func (e *StoreNotFoundError) message(_ *Error) string {
	return fmt.Sprintf("store %d was not found", e.StoreID)
}

// NewReplicaTooOldError initializes a new ReplicaTooOldError.
func NewReplicaTooOldError(replicaID ReplicaID) *ReplicaTooOldError {
	return &ReplicaTooOldError{
		ReplicaID: replicaID,
	}
}

func (e *ReplicaTooOldError) Error() string {
	return e.message(nil)
}

func (*ReplicaTooOldError) message(_ *Error) string {
	return "sender replica too old, discarding message"
}
