package multiraft

import (
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"golang.org/x/net/context"
)

// proposalResult indicates the result of a proposal. Exactly one of
// Reply, Err and ProposalRetry is set, and it represents the result of
// the proposal.
type proposalResult struct {
	Reply *multiraftbase.BatchResponse
	Err   *multiraftbase.Error
}

// ProposalData is data about a command which allows it to be
// evaluated, proposed to raft, and for the result of the command to
// be returned to the caller.
type ProposalData struct {
	// The caller's context, used for logging proposals and reproposals.
	ctx context.Context

	// idKey uniquely identifies this proposal.
	// TODO(andreimatei): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, various test facilities depend on the
	// command ID for e.g. replay protection.
	idKey multiraftbase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// command is serialized and proposed to raft. In the event of
	// reproposals its MaxLeaseIndex field is mutated.
	command multiraftbase.RaftCommand

	// endCmds.finish is called after command execution to update the timestamp cache &
	// command queue.
	endCmds *endCmds

	// doneCh is used to signal the waiting RPC handler (the contents of
	// proposalResult come from LocalEvalResult).
	//
	// Attention: this channel is not to be signaled directly downstream of Raft.
	// Always use ProposalData.finishRaftApplication().
	doneCh chan proposalResult

	// Request is the client's original BatchRequest.
	// TODO(tschottdorf): tests which use TestingCommandFilter use this.
	// Decide how that will work in the future, presumably the
	// CommandFilter would run at proposal time or we allow an opaque
	// struct to be attached to a proposal which is then available as it
	// applies. Other than tests, we only need a few bits of the request
	// here; this could be replaced with isLease and isChangeReplicas
	// booleans.
	Request *multiraftbase.BatchRequest
}

// finishRaftApplication is called downstream of Raft when a command application
// has finished. proposal.doneCh is signaled with pr so that the proposer is
// unblocked.
//
// It first invokes the endCmds function and then sends the specified
// proposalResult on the proposal's done channel. endCmds is invoked here in
// order to allow the original client to be cancelled and possibly no longer
// listening to this done channel, and so can't be counted on to invoke endCmds
// itself.
//
// Note: this should not be called upstream of Raft because, in case pr.Err is
// set, it clears the intents from pr before sending it on the channel. This
// clearing should not be done upstream of Raft because, in cases of errors
// encountered upstream of Raft, we might still want to resolve intents:
// upstream of Raft, pr.intents represent intents encountered by a request, not
// the current txn's intents.
func (proposal *ProposalData) finishRaftApplication(pr proposalResult) {
	if proposal.endCmds != nil {
		proposal.endCmds.done(pr.Reply, pr.Err)
		proposal.endCmds = nil
	}
	proposal.doneCh <- pr
	close(proposal.doneCh)
}
