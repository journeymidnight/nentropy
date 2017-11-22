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

func (this *ErrorDetail) SetValue(value interface{}) bool {
	switch vt := value.(type) {
	case *NodeUnavailableError:
		this.NodeUnavailable = vt
	case *NodeNotReadyError:
		this.NodeNotReady = vt
	case *SendError:
		this.Send = vt
	case *RaftGroupDeletedError:
		this.RaftGroupDeleted = vt
	case *ReplicaCorruptionError:
		this.ReplicaCorruption = vt
	case *ReplicaTooOldError:
		this.ReplicaTooOld = vt
	case *AmbiguousResultError:
		this.AmbiguousResult = vt
	default:
		return false
	}
	return true
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

func NewNodeNotReadyError(nodeID string) *NodeNotReadyError {
	return &NodeNotReadyError{
		NodeId: nodeID,
	}
}

func (e *NodeNotReadyError) Error() string {
	return e.message(nil)
}

func (e *NodeNotReadyError) message(_ *Error) string {
	return fmt.Sprintf("node %s was not ready", e.NodeId)
}

// NewGroupNotFoundError initializes a new GroupNotFoundError.
func NewGroupNotFoundError(groupID GroupID) *GroupNotFoundError {
	return &GroupNotFoundError{
		GroupID: groupID,
	}
}

// NewAmbiguousResultError initializes a new AmbiguousResultError with
// an explanatory message.
func NewAmbiguousResultError(msg string) *AmbiguousResultError {
	return &AmbiguousResultError{Message: msg}
}
