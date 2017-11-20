package multiraft

import (
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// endCmds holds necessary information to end a batch after Raft
// command processing.
type endCmds struct {
	repl *Replica
	ba   multiraftbase.BatchRequest
}

// removeCmdsFromCommandQueue removes a batch's set of commands for the
// replica's command queue.
func (r *Replica) removeCmdsFromCommandQueue() {
	r.cmdQMu.Lock()
	r.cmdQMu.Unlock()
}

// done removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (ec *endCmds) done(br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {
	ec.repl.removeCmdsFromCommandQueue(ec.cmds)
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	helper.AmbientContext

	store *Store

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// The state of the Raft state machine.
		state multiraftbase.ReplicaState
		// Last index/term persisted to the raft log (not necessarily
		// committed). Note that lastTerm may be 0 (and thus invalid) even when
		// lastIndex is known, in which case the term will have to be retrieved
		// from the Raft log entry. Use the invalidLastTerm constant for this
		// case.
		lastIndex, lastTerm uint64
		// proposals stores the Raft in-flight commands which
		// originated at this Replica, i.e. all commands for which
		// propose has been called, but which have not yet
		// applied.
		//
		// The *ProposalData in the map are "owned" by it. Elements from the
		// map must only be referenced while Replica.mu is held, except if the
		// element is removed from the map first. The notable exception is the
		// contained RaftCommand, which we treat as immutable.
		proposals         map[CmdIDKey]*ProposalData
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID ReplicaID
		// The minimum allowed ID for this replica. Initialized from
		// RaftTombstone.NextReplicaID.
		minReplicaID ReplicaID
		// The ID of the leader replica within the Raft group. Used to determine
		// when the leadership changes.
		leaderID ReplicaID

		// Counts calls to Replica.tick()
		ticks int
		// Note that there are two replicaStateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader replicaStateLoader

		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool
	}

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	//
	// TODO(peter): evaluate runtime overhead of the timed mutex.
	raftMu struct {
		timedMutex

		// Note that there are two replicaStateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader replicaStateLoader
	}

	cmdQMu struct {
		// Protects all fields in the cmdQMu struct.
		//
		// Locking notes: Replica.mu < Replica.cmdQMu
		syncutil.Mutex
		// Enforces at most one command is running per key(s) within each span
		// scope. The globally-scoped component tracks user writes (i.e. all
		// keys for which keys.Addr is the identity), the locally-scoped component
		// the rest (e.g. RangeDescriptor, transaction record, Lease, ...).
		// Commands with different accesses but the same scope are stored in the
		// same component.
		queues [numSpanScope]*CommandQueue
	}
	unreachablesMu struct {
		syncutil.Mutex
		remotes map[ReplicaID]struct{}
	}
}

// tick the Raft group, returning any error and true if the raft group exists
// and false otherwise.
func (r *Replica) tick() (bool, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the raft group is uninitialized, do not initialize raft groups on
	// tick.
	if r.mu.internalRaftGroup == nil {
		return false, nil
	}

	r.mu.ticks++
	r.mu.internalRaftGroup.Tick()
	return true, nil
}

type handleRaftReadyStats struct {
	processed int
}

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReady(inSnap IncomingSnapshot) (handleRaftReadyStats, string, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
//
// The returned string is nonzero whenever an error is returned to give a
// non-sensitive cue as to what happened.
func (r *Replica) handleRaftReadyRaftMuLocked(
	inSnap IncomingSnapshot,
) (handleRaftReadyStats, string, error) {
	var stats handleRaftReadyStats

	ctx := r.AnnotateCtx(context.TODO())
	var hasReady bool
	var rd raft.Ready
	r.mu.Lock()

	lastIndex := r.mu.lastIndex // used for append below
	lastTerm := r.mu.lastTerm
	raftLogSize := r.mu.raftLogSize
	leaderID := r.mu.leaderID
	lastLeaderID := leaderID

	err := r.withRaftGroupLocked(false, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	r.mu.Unlock()
	if err != nil {
		const expl = "while checking raft group for Ready"
		return stats, expl, errors.Wrap(err, expl)
	}

	if !hasReady {
		return stats, "", nil
	}

	refreshReason := noReason
	if rd.SoftState != nil && leaderID != ReplicaID(rd.SoftState.Lead) {
		leaderID = ReplicaID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		//
	}

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	prevLastIndex := lastIndex
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		if lastIndex, lastTerm, raftLogSize, err = r.append(
			ctx, writer, lastIndex, lastTerm, raftLogSize, thinEntries,
		); err != nil {
			const expl = "during append"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.raftMu.stateLoader.setHardState(ctx, writer, rd.HardState); err != nil {
			const expl = "during setHardState"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	writer.Close()
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before Raft messages are sent and before the call to RawNode.Advance. Our
	// regular locking is sufficient for this and if other goroutines can see the
	// data early, that's fine. In particular, snapshots are not a problem (I
	// think they're the only thing that might access log entries or HardState
	// from other goroutines). Snapshots do not include either the HardState or
	// uncommitted log entries, and even if they did include log entries that
	// were not persisted to disk, it wouldn't be a problem because raft does not
	// infer the that entries are persisted on the node that sends a snapshot.
	start := timeutil.Now()
	if err := batch.Commit(syncRaftLog.Get(&r.store.cfg.Settings.SV) && rd.MustSync); err != nil {
		const expl = "while committing batch"
		return stats, expl, errors.Wrap(err, expl)
	}

	if len(rd.Entries) > 0 {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := rd.Entries[0].Index // first new entry written
		purgeTerm := rd.Entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
	}

	// Update protected state (last index, last term, raft log size and raft
	// leader ID) and set raft log entry cache. We clear any older, uncommitted
	// log entries and cache the latest ones.
	//
	// Note also that we're likely to send messages related to the Entries we
	// just appended, and these entries need to be inlined when sending them to
	// followers - populating the cache here saves a lot of that work.
	r.mu.Lock()
	r.store.raftEntryCache.addEntries(r.RangeID, rd.Entries)
	r.mu.lastIndex = lastIndex
	r.mu.lastTerm = lastTerm
	r.mu.raftLogSize = raftLogSize
	r.mu.leaderID = leaderID
	r.mu.Unlock()

	for _, message := range rd.Messages {
		r.sendRaftMessage(ctx, message)
	}

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:
			var commandID CmdIDKey
			var command protos.RaftCommand

			// Process committed entries. etcd raft occasionally adds a nil entry
			// (our own commands are never empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				refreshReason = reasonNewLeaderOrConfigChange
				commandID = "" // special-cased value, command isn't used
			} else {
				var encodedCommand []byte
				commandID, encodedCommand = DecodeRaftCommand(e.Data)
				// An empty command is used to unquiesce a range and wake the
				// leader. Clear commandID so it's ignored for processing.
				if len(encodedCommand) == 0 {
					commandID = ""
				} else if err := command.Unmarshal(encodedCommand); err != nil {
					const expl = "while unmarshalling entry"
					return stats, expl, errors.Wrap(err, expl)
				}
			}

			if changedRepl := r.processRaftCommand(ctx, commandID, e.Term, e.Index, command); changedRepl {
				helper.Logger.Fatalf(5, "unexpected replication change from command %s", &command)
			}
			stats.processed++

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				const expl = "while unmarshaling ConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			var ccCtx ConfChangeContext
			if err := ccCtx.Unmarshal(cc.Context); err != nil {
				const expl = "while unmarshaling ConfChangeContext"
				return stats, expl, errors.Wrap(err, expl)
			}
			var command protos.RaftCommand
			if err := command.Unmarshal(ccCtx.Payload); err != nil {
				const expl = "while unmarshaling RaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			}
			commandID := CmdIDKey(ccCtx.CommandID)
			if changedRepl := r.processRaftCommand(
				ctx, commandID, e.Term, e.Index, command,
			); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			stats.processed++

			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				const expl = "during ApplyConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
		default:
			helper.Logger.Fatalf(5, "unexpected Raft entry: %v", e)
		}
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	const expl = "during advance"
	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.Advance(rd)
		return true, nil
	}); err != nil {
		return stats, expl, errors.Wrap(err, expl)
	}
	return stats, "", nil
}

// processRaftCommand processes a raft command by unpacking the
// command struct to get args and reply and then applying the command
// to the state machine via applyRaftCommand(). The result is sent on
// the command's done channel, if available. As a special case, the
// zero idKey signifies an empty Raft command, which will apply as a
// no-op (without accessing raftCmd), updating only the applied index.
//
// This method returns true if the command successfully applied a
// replica change.
//
// TODO(tschottdorf): once we properly check leases and lease requests etc,
// make sure that the error returned from this method is always populated in
// those cases, as one of the callers uses it to abort replica changes.
func (r *Replica) processRaftCommand(
	ctx context.Context,
	idKey CmdIDKey,
	term, index uint64,
	raftCmd protos.RaftCommand,
) bool {
	if index == 0 {
		helper.Logger.Fatalf(5, "processRaftCommand requires a non-zero index")
	}

	r.mu.Lock()
	proposal, proposedLocally := r.mu.proposals[idKey]

	// TODO(tschottdorf): consider the Trace situation here.
	if proposedLocally {
		// We initiated this command, so use the caller-supplied context.
		ctx = proposal.ctx
		proposal.ctx = nil // avoid confusion
		delete(r.mu.proposals, idKey)
	}

	r.mu.Unlock()

	var response proposalResult
	var writeBatch *protos.WriteBatch
	{
		var pErr *protos.Error
		if raftCmd.WriteBatch != nil {
			writeBatch = raftCmd.WriteBatch
		}

		r.applyRaftCommand(ctx, idKey, writeBatch)
	}

	if proposedLocally {
		proposal.finishRaftApplication(response)
	} else if response.Err != nil {
		helper.Logger.Printf(5, "applying raft command resulted in error: %s", response.Err)
	}

	return true
}

// NewReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func NewReplicaCorruptionError(err error) *protos.ReplicaCorruptionError {
	return &protos.ReplicaCorruptionError{ErrorMsg: err.Error()}
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine). When the state machine can not be
// updated, an error (which is likely a ReplicaCorruptionError) is returned and
// must be handled by the caller.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey CmdIDKey,
	writeBatch *protos.WriteBatch,
) *Error {
	return nil
}

func (r *Replica) getReplicaDescriptorByIDRLocked(
	replicaID ReplicaID, fallback protos.ReplicaDescriptor,
) (protos.ReplicaDescriptor, error) {
	if repDesc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(replicaID); ok {
		return repDesc, nil
	}
	if fallback.ReplicaID == replicaID {
		return fallback, nil
	}
	return protos.ReplicaDescriptor{},
		errors.Errorf("replica %d not present in %v, %v", replicaID, fallback, r.mu.state.Desc.Replicas)
}

// sendRaftMessage sends a Raft message.
func (r *Replica) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	r.mu.Lock()
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(ReplicaID(msg.To), r.mu.lastFromReplica)
	r.mu.Unlock()

	if fromErr != nil {
		helper.Logger.Printf(5, "failed to look up sender replica %d in r%d while sending %s: %s",
			msg.From, r.RangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		helper.Logger.Printf(5, "failed to look up recipient replica %d in r%d while sending %s: %s",
			msg.To, r.RangeID, msg.Type, toErr)
		return
	}

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false) {
		return
	}

	if !r.sendRaftMessageRequest(ctx, &protos.RaftMessageRequest{
		RangeID:     r.RangeID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	}) {
		if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
			r.mu.droppedMessages++
			raftGroup.ReportUnreachable(msg.To)
			return true, nil
		}); err != nil {
			helper.Logger.Fatalln(5, err)
		}
	}
}

// maybeCoalesceHeartbeat returns true if the heartbeat was coalesced and added
// to the appropriate queue.
func (r *Replica) maybeCoalesceHeartbeat(
	ctx context.Context,
	msg raftpb.Message,
	toReplica, fromReplica protos.ReplicaDescriptor,
	quiesce bool,
) bool {
	var hbMap map[protos.StoreIdent][]protos.RaftHeartbeat
	switch msg.Type {
	case raftpb.MsgHeartbeat:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeatResponses
	default:
		return false
	}
	beat := protos.RaftHeartbeat{
		RangeID:       r.RangeID,
		ToReplicaID:   toReplica.ReplicaID,
		FromReplicaID: fromReplica.ReplicaID,
		Term:          msg.Term,
		Commit:        msg.Commit,
		Quiesce:       quiesce,
	}

	toStore := protos.StoreIdent{
		StoreID: toReplica.StoreID,
		NodeID:  toReplica.NodeID,
	}
	hbMap[toStore] = append(hbMap[toStore], beat)
	r.store.coalescedMu.Unlock()
	return true
}

func (r *Replica) isSoloReplicaRLocked() bool {
	return len(r.mu.state.Desc.Replicas) == 1 &&
		r.mu.state.Desc.Replicas[0].ReplicaID == r.mu.replicaID
}

// withRaftGroupLocked calls the supplied function with the (lazily
// initialized) Raft group. The supplied function should return true for the
// unquiesceAndWakeLeader argument if the replica should be unquiesced (and the
// leader awoken). See handleRaftReady for an instance of where this value
// varies. The shouldCampaignOnCreation argument indicates whether a new raft group
// should be campaigned upon creation and is used to eagerly campaign idle
// replicas.
//
// Requires that both Replica.mu and Replica.raftMu are held.
func (r *Replica) withRaftGroupLocked(
	shouldCampaignOnCreation bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	if r.mu.replicaID == 0 {
		// The replica's raft group has not yet been configured (i.e. the replica
		// was created from a preemptive snapshot).
		return nil
	}

	if shouldCampaignOnCreation {
		// Special handling of idle replicas: we campaign their Raft group upon
		// creation if we gossiped our store descriptor more than the election
		// timeout in the past.
		shouldCampaignOnCreation = (r.mu.internalRaftGroup == nil) && r.store.canCampaignIdleReplica()
	}

	ctx := r.AnnotateCtx(context.TODO())

	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			helper.Logger,
		), nil)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup

		if !shouldCampaignOnCreation {
			// Automatically campaign and elect a leader for this group if there's
			// exactly one known node for this group.
			//
			// A grey area for this being correct happens in the case when we're
			// currently in the process of adding a second node to the group, with
			// the change committed but not applied.
			//
			// Upon restarting, the first node would immediately elect itself and
			// only then apply the config change, where really it should be applying
			// first and then waiting for the majority (which would now require two
			// votes, not only its own).
			//
			// However, in that special case, the second node has no chance to be
			// elected leader while the first node restarts (as it's aware of the
			// configuration and knows it needs two votes), so the worst that could
			// happen is both nodes ending up in candidate state, timing out and then
			// voting again. This is expected to be an extremely rare event.
			//
			// TODO(peter): It would be more natural for this campaigning to only be
			// done when proposing a command (see defaultProposeRaftCommandLocked).
			// Unfortunately, we enqueue the right hand side of a split for Raft
			// ready processing if the range only has a single replica (see
			// splitPostApply). Doing so implies we need to be campaigning
			// that right hand side range when raft ready processing is
			// performed. Perhaps we should move the logic for campaigning single
			// replica ranges there so that normally we only eagerly campaign when
			// proposing.
			shouldCampaignOnCreation = r.isSoloReplicaRLocked()
		}
		if shouldCampaignOnCreation {
			helper.Logger.Printf(3, "campaigning")
			if err := raftGroup.Campaign(); err != nil {
				return err
			}
		}
	}

	unquiesce, err := f(r.mu.internalRaftGroup)
	if unquiesce {
		r.unquiesceAndWakeLeaderLocked()
	}
	return err
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent {
		helper.Logger.Printf(5, "unquiescing: waking leader")
		r.mu.quiescent = false
		// Propose an empty command which will wake the leader.
		_ = r.mu.internalRaftGroup.Propose(encodeRaftCommandV1(makeIDKey(), nil))
	}
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
//
// Requires that Replica.raftMu is held.
func (r *Replica) withRaftGroup(
	f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(false, f)
}

// sendRaftMessageRequest sends a raft message, returning false if the message
// was dropped. It is the caller's responsibility to call ReportUnreachable on
// the Raft group.
func (r *Replica) sendRaftMessageRequest(ctx context.Context, req *protos.RaftMessageRequest) bool {

	helper.Logger.Printf(5, "sending raft request %+v", req)

	ok := r.store.cfg.Transport.SendAsync(req)
	return ok
}

// IsInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInitializedRLocked()
}

// isInitializedRLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedRLocked() bool {
	return r.mu.state.Desc.IsInitialized()
}

// RaftStatus returns the current raft status of the replica. It returns nil
// if the Raft group has not been initialized yet.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftStatusRLocked()
}

func (r *Replica) raftStatusRLocked() *raft.Status {
	if rg := r.mu.internalRaftGroup; rg != nil {
		return rg.Status()
	}
	return nil
}

// maybeTickQuiesced attempts to tick a quiesced or dormant replica, returning
// true on success and false if the regular tick path must be taken
// (i.e. Replica.tick).
func (r *Replica) maybeTickQuiesced() bool {
	var done bool
	r.mu.Lock()
	if r.mu.internalRaftGroup == nil {
		done = true
	} else if r.mu.quiescent {
		done = true
		if !enablePreVote {
			// NB: It is safe to call TickQuiesced without holding Replica.raftMu
			// because that method simply increments a counter without performing any
			// other logic.
			r.mu.internalRaftGroup.TickQuiesced()
		}
	}
	r.mu.Unlock()
	return done
}

// addUnreachableRemoteReplica adds the given remote ReplicaID to be reported
// as unreachable on the next tick.
func (r *Replica) addUnreachableRemoteReplica(remoteReplica ReplicaID) {
	r.unreachablesMu.Lock()
	if r.unreachablesMu.remotes == nil {
		r.unreachablesMu.remotes = make(map[ReplicaID]struct{})
	}
	r.unreachablesMu.remotes[remoteReplica] = struct{}{}
	r.unreachablesMu.Unlock()
}