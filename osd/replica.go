package main

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/badger"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	//"github.com/journeymidnight/nentropy/multiraft/keys"
	"github.com/journeymidnight/nentropy/osd/keys"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/idutil"
	"github.com/journeymidnight/nentropy/util/protoutil"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
	"github.com/journeymidnight/nentropy/util/tracing"
	"github.com/journeymidnight/nentropy/util/uuid"
	"github.com/journeymidnight/nentropy/util/wait"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"math/rand"
	"strconv"
	"time"
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
}

// done removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (ec *endCmds) done(br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	log.AmbientContext

	// TODO(tschottdorf): Duplicates r.mu.state.desc.RangeID; revisit that.
	GroupID multiraftbase.GroupID // Should only be set by the constructor.

	store  *Store
	engine engine.Engine

	creatingReplica *multiraftbase.ReplicaDescriptor

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// Has the replica been destroyed.
		destroyed error
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
		proposals         map[multiraftbase.CmdIDKey]*ProposalData
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID multiraftbase.ReplicaID
		// The minimum allowed ID for this replica. Initialized from
		// RaftTombstone.NextReplicaID.
		minReplicaID multiraftbase.ReplicaID
		// The ID of the leader replica within the Raft group. Used to determine
		// when the leadership changes.
		leaderID multiraftbase.ReplicaID

		// Counts calls to Replica.tick()
		ticks int
		// Note that there are two replicaStateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader replicaStateLoader

		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool

		lastToReplica, lastFromReplica multiraftbase.ReplicaDescriptor
		// The most recent commit index seen in a message from the leader. Used by
		// the follower to estimate the number of Raft log entries it is
		// behind. This field is only valid when the Replica is a follower.
		estimatedCommitIndex uint64
		// raftLogSize is the approximate size in bytes of the persisted raft log.
		// On server restart, this value is assumed to be zero to avoid costly scans
		// of the raft log. This will be correct when all log entries predating this
		// process have been truncated.
		raftLogSize int64

		// The raft log index of a pending preemptive snapshot. Used to prohibit
		// raft log truncation while a preemptive snapshot is in flight. A value of
		// 0 indicates that there is no pending snapshot.
		pendingSnapshotIndex uint64
		// Max bytes before split.
		maxBytes int64
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
	unreachablesMu struct {
		syncutil.Mutex
		remotes map[multiraftbase.ReplicaID]struct{}
	}

	reqIDGen  *idutil.Generator
	readwaitc chan struct{}
	// stopping is closed by run goroutine on shutdown.
	stopping chan struct{}

	readMu syncutil.RWMutex
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	readNotifier *helper.Notifier

	// a chan to send out readState
	readStateC chan raft.ReadState
	applyWait  wait.WaitTime
	// done is closed when all goroutines from start() complete.
	done chan struct{}
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
		helper.Printf(5, "Replica %d has no raft group to tick.", r.mu.replicaID)
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

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeader
	reasonNewLeaderOrConfigChange
	reasonSnapshotApplied
	reasonReplicaIDChanged
	reasonTicks

	defaultReplicaRaftMuWarnThreshold = 500 * time.Millisecond
)

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
	//lastLeaderID := leaderID

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

	if rd.SoftState != nil {
		leader := r.mu.leaderID == r.mu.replicaID
		if rd.RaftState == raft.StateFollower && leader {
			ReplicaStateChangeCallback(string(r.GroupID), rd.SoftState.RaftState.String())
		} else if rd.RaftState == raft.StateLeader && !leader {
			helper.Println(5, "ID ", r.mu.replicaID, " became leader in ", r.GroupID)
			ReplicaStateChangeCallback(string(r.GroupID), rd.SoftState.RaftState.String())
		}
	}

	//refreshReason := noReason
	if rd.SoftState != nil && leaderID != multiraftbase.ReplicaID(rd.SoftState.Lead) {
		leaderID = multiraftbase.ReplicaID(rd.SoftState.Lead)

	}
	//eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)
	eng := r.engine
	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		if err != nil {
			const expl = "invalid snapshot id"
			return stats, expl, errors.Wrap(err, expl)
		}
		if inSnap.SnapUUID == (uuid.UUID{}) {
			helper.Fatalf("programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
		}
		if snapUUID != inSnap.SnapUUID {
			helper.Fatalf("incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
		}

		if err := r.applySnapshot(ctx, inSnap, rd.Snapshot, rd.HardState); err != nil {
			const expl = "while applying snapshot"
			helper.Println(5, "Error applying snapshot. err:", err)
			return stats, expl, errors.Wrap(err, expl)
		}

		// r.mu.lastIndex and r.mu.lastTerm were updated in applySnapshot, but
		// we also want to make sure we reflect these changes in the local
		// variables we're tracking here. We could pull these values from
		// r.mu itself, but that would require us to grab a lock.
		if lastIndex, err = r.raftMu.stateLoader.loadLastIndex(ctx, eng); err != nil {
			const expl = "loading last index"
			return stats, expl, errors.Wrap(err, expl)
		}
		lastTerm = invalidLastTerm

		for _, rep := range inSnap.State.Desc.Replicas {
			helper.Println(5, "-----inSnap: replica id:", rep.ReplicaID, " osdid:", rep.NodeID)
		}

	}

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := eng.NewBatch(true)
	defer batch.Close()

	prevLastIndex := lastIndex
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, sideLoadedEntriesSize, err := r.maybeSideloadEntriesRaftMuLocked(ctx, rd.Entries)
		if err != nil {
			const expl = "during sideloading"
			return stats, expl, errors.Wrap(err, expl)
		}

		raftLogSize += sideLoadedEntriesSize
		if lastIndex, lastTerm, raftLogSize, err = r.append(
			ctx, batch, lastIndex, lastTerm, raftLogSize, thinEntries,
		); err != nil {
			helper.Println(10, "Failed to append entries! err:", err)
			const expl = "during append"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.raftMu.stateLoader.setHardState(ctx, batch, rd.HardState); err != nil {
			helper.Println(10, "Failed to set hard state! err:", err)
			const expl = "during setHardState"
			return stats, expl, errors.Wrap(err, expl)
		}
	}

	internalTimeout := time.Second
	if len(rd.ReadStates) != 0 {
		helper.Println(20, "handle readstate message.")
		select {
		case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
		case <-time.After(internalTimeout):
			helper.Println(5, "timed out sending read state")
		}
	}

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
	//start := timeutil.Now()
	if err := batch.Commit(); err != nil {
		helper.Println(10, "Failed to commit! err:", err)
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
		for i := firstPurge; i <= lastPurge; i++ {
			key := keys.DataKey(purgeTerm, i)
			err := r.engine.Clear(key)
			if err != nil {
				const expl = "while purging index %d"
				return stats, expl, errors.Wrapf(err, expl, i)
			}
		}
	}

	// Update protected state (last index, last term, raft log size and raft
	// leader ID) and set raft log entry cache. We clear any older, uncommitted
	// log entries and cache the latest ones.
	//
	// Note also that we're likely to send messages related to the Entries we
	// just appended, and these entries need to be inlined when sending them to
	// followers - populating the cache here saves a lot of that work.
	r.mu.Lock()
	r.store.raftEntryCache.addEntries(r.GroupID, rd.Entries)
	r.mu.lastIndex = lastIndex
	r.mu.lastTerm = lastTerm
	r.mu.raftLogSize = raftLogSize
	r.mu.leaderID = leaderID
	r.mu.Unlock()

	//helper.Printf(10, "=====raftLogSize:%d", raftLogSize)
	for _, message := range rd.Messages {
		helper.Println(20, "sent message:", "To:", message.To,
			"From:", message.From,
			"Type:", message.Type,
			"Term:", message.Term,
			"Index:", message.Index,
			"LogTerm:", message.LogTerm,
			"context:", message.Context,
		)
		r.sendRaftMessage(ctx, message)
	}
	var lastAppliedIndex uint64
	for _, e := range rd.CommittedEntries {
		helper.Println(5, "raft commit index:", e.Index, " term:", e.Term)
		switch e.Type {
		case raftpb.EntryNormal:
			if newEnt, err := maybeInlineSideloadedRaftCommand(
				ctx, r.GroupID, e, r.engine, r.store.raftEntryCache,
			); err != nil {
				const expl = "maybeInlineSideloadedRaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			} else if newEnt != nil {
				e = *newEnt
			}
			var commandID multiraftbase.CmdIDKey
			var command multiraftbase.RaftCommand

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
				//refreshReason := reasonNewLeaderOrConfigChange
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
			helper.Println(5, "commit entries. EntryNormal: ")
			if changedRepl := r.processRaftCommand(ctx, commandID, e.Term, e.Index, command); !changedRepl {
				helper.Fatalf("unexpected replication change from command %s", &command)
			}
			lastAppliedIndex = e.Index
			stats.processed++

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				const expl = "while unmarshaling ConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			var ccCtx multiraftbase.ConfChangeContext
			if err := ccCtx.Unmarshal(cc.Context); err != nil {
				const expl = "while unmarshaling ConfChangeContext"
				return stats, expl, errors.Wrap(err, expl)
			}
			var command multiraftbase.RaftCommand
			if err := command.Unmarshal(ccCtx.Payload); err != nil {
				const expl = "while unmarshaling RaftCommand"
				return stats, expl, errors.Wrap(err, expl)
			}
			commandID := multiraftbase.CmdIDKey(ccCtx.CommandID)
			if changedRepl := r.processRaftCommand(
				ctx, commandID, e.Term, e.Index, command,
			); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}

			if cc.Type == raftpb.ConfChangeAddNode {
				r.mu.Lock()
				var exist bool
				for _, rep := range r.mu.state.Desc.Replicas {
					if rep.ReplicaID == ccCtx.Replica.ReplicaID &&
						rep.NodeID == ccCtx.Replica.NodeID {
						exist = true
					}
				}
				if !exist {
					r.mu.state.Desc.Replicas = append(r.mu.state.Desc.Replicas, multiraftbase.ReplicaDescriptor{
						NodeID:    ccCtx.Replica.NodeID,
						ReplicaID: ccCtx.Replica.ReplicaID,
					})
				}
				r.mu.Unlock()
			} else {
				r.mu.Lock()
				var replicas []multiraftbase.ReplicaDescriptor
				for _, rep := range r.mu.state.Desc.Replicas {
					if rep.ReplicaID != ccCtx.Replica.ReplicaID ||
						rep.NodeID != ccCtx.Replica.NodeID {
						replicas = append(replicas, rep)
					}
				}
				r.mu.state.Desc.Replicas = replicas
				if r.mu.replicaID == ccCtx.Replica.ReplicaID {
					r.mu.destroyed = errors.New("Exit...")
				}
				helper.Println(5, "received msg ConfChangeRemoveNode, group:", r.GroupID, " total members:")
				for _, rep := range r.mu.state.Desc.Replicas {
					helper.Println(5, "rep.ReplicaID:", rep.ReplicaID, " NodeID:", rep.NodeID)
				}
				var reps []protos.PgReplica
				var osdId int32
				fmt.Sscanf(string(ccCtx.Replica.NodeID), "osd.%d", &osdId)
				reps = append(reps, protos.PgReplica{
					OsdId:        osdId,
					ReplicaIndex: int32(ccCtx.Replica.ReplicaID),
				})
				r.mu.Unlock()
				ReplicaDelReplicaCallback(reps, r.GroupID)
			}

			stats.processed++
			helper.Println(5, "-------conf change node group", r.GroupID, ccCtx.Replica.NodeID, " replica id:", ccCtx.Replica.ReplicaID)
			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				const expl = "during ApplyConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			lastAppliedIndex = e.Index
		default:
			helper.Fatalf("unexpected Raft entry: %v", e)
		}
	}

	rsl := makeReplicaStateLoader(r.GroupID)
	r.mu.Lock()
	err = rsl.save(ctx, eng, r.mu.state)
	if err != nil {
		helper.Println(5, "Failed to write replica state! err:", err)
	}
	if lastAppliedIndex != 0 {
		r.mu.state.RaftAppliedIndex = lastAppliedIndex
	}
	r.applyWait.Trigger(r.mu.state.RaftAppliedIndex)
	r.mu.Unlock()
	if len(rd.CommittedEntries) != 0 {
		helper.Println(5, "Group:", r.GroupID, "Raft lastIndex:", r.mu.lastIndex,
			" RaftAppliedIndex:", r.mu.state.RaftAppliedIndex,
			" TruncatedState.index:", r.mu.state.TruncatedState.Index,
			" TruncatedState.term:", r.mu.state.TruncatedState.Term)
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
	idKey multiraftbase.CmdIDKey,
	term, index uint64,
	raftCmd multiraftbase.RaftCommand,
) bool {
	if index == 0 {
		helper.Fatalf("processRaftCommand requires a non-zero index")
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
	var writeBatch *multiraftbase.WriteBatch
	if idKey != "" {
		var _ *multiraftbase.Error
		if raftCmd.WriteBatch != nil {
			writeBatch = raftCmd.WriteBatch
		}
		response.Reply = &multiraftbase.BatchResponse{}
		resp, err := r.applyRaftCommand(ctx, idKey, term, index, raftCmd.Method, writeBatch)
		response.Reply.Responses.MustSetInner(resp)
		response.Err = err
	}

	helper.Println(5, "GroupID:", r.GroupID, "processRaftCommand(): RaftAppliedIndex:", index)

	if proposedLocally {
		proposal.finishRaftApplication(response)
	} else if response.Err != nil {
		helper.Printf(5, "applying raft command resulted in error: %s", response.Err)
	}

	return true
}

// NewReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func NewReplicaCorruptionError(err error) *multiraftbase.ReplicaCorruptionError {
	return &multiraftbase.ReplicaCorruptionError{ErrorMsg: err.Error()}
}

func NewReplica(
	desc *multiraftbase.GroupDescriptor, store *Store, replicaID multiraftbase.ReplicaID,
) (*Replica, error) {
	r := newReplica(desc.GroupID, store)
	return r, r.init(desc, replicaID)
}

func newReplica(groupID multiraftbase.GroupID, store *Store) *Replica {
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		GroupID:        groupID,
		store:          store,
	}
	r.mu.stateLoader = makeReplicaStateLoader(groupID)

	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTagStr("@", fmt.Sprintf("%x", unsafe.Pointer(r)))

	raftMuLogger := thresholdLogger(
		r.AnnotateCtx(context.Background()),
		500*time.Millisecond,
		func(ctx context.Context, msg string, args ...interface{}) {
			helper.Printf(5, "raftMu: "+msg, args...)
		},
	)
	r.raftMu.timedMutex = makeTimedMutex(raftMuLogger)
	r.raftMu.stateLoader = makeReplicaStateLoader(groupID)
	return r
}

func (r *Replica) init(
	desc *multiraftbase.GroupDescriptor, replicaID multiraftbase.ReplicaID,
) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.initRaftMuLockedReplicaMuLocked(desc, replicaID)
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine). When the state machine can not be
// updated, an error (which is likely a ReplicaCorruptionError) is returned and
// must be handled by the caller.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey multiraftbase.CmdIDKey,
	term, index uint64,
	method multiraftbase.Method,
	writeBatch *multiraftbase.WriteBatch,
) (resp multiraftbase.Response, Err *multiraftbase.Error) {

	if writeBatch == nil || writeBatch.Data == nil {
		return nil, nil
	}

	if method == multiraftbase.Put {
		resp = &multiraftbase.PutResponse{}
		putReq := multiraftbase.PutRequest{}
		err := putReq.Unmarshal(writeBatch.Data)
		if err != nil {
			Err = multiraftbase.NewError(err)
			helper.Printf(5, "Cannot unmarshal data to kv")
			return
		}
		data, err := r.engine.Get(putReq.Key)
		onode := multiraftbase.Onode{}
		if err != nil && err != badger.ErrKeyNotFound {
			Err = multiraftbase.NewError(err)
		} else if err == badger.ErrKeyNotFound {
			onode.Key = keys.DataKey(term, index)
			onode.Size_ = int32(len(putReq.Value))
			data, err = onode.Marshal()
			err = r.engine.Put(putReq.Key, data)
			helper.Println(5, "write kv onode ***************", r.GroupID, putReq.Key, onode.Key, onode.Size_, term, index)
			if err != nil {
				Err = multiraftbase.NewError(err)
				helper.Println(5, "Error putting data to db, err:", err)
			}
		} else {
			err = onode.Unmarshal(data)
			if err != nil {
				Err = multiraftbase.NewError(err)
				return
			}
			err = r.engine.Clear(onode.Key)
			if err != nil && err != badger.ErrKeyNotFound {
				Err = multiraftbase.NewError(err)
				helper.Println(5, "Error clear expired data, err:", err)
			} else {
				onode.Key = keys.DataKey(term, index)
				onode.Size_ = int32(len(putReq.Value))
				data, err = onode.Marshal()
				err = r.engine.Put(putReq.Key, data)
				helper.Println(5, "update kv onode ***************", r.GroupID, putReq.Key, onode.Key, onode.Size_)
				if err != nil {
					Err = multiraftbase.NewError(err)
					helper.Println(5, "Error putting data to db, err:", err)
				}
			}
		}
	} else if method == multiraftbase.TruncateLog {
		truncateReq := multiraftbase.TruncateLogRequest{}
		resp = &multiraftbase.TruncateLogResponse{}
		err := truncateReq.Unmarshal(writeBatch.Data)
		if err != nil {
			Err = multiraftbase.NewError(err)
			helper.Printf(5, "Cannot unmarshal data to kv")
		}
		eng := r.store.LoadGroupEngine(r.Desc().GroupID)
		deltSize := int32(0)
		r.mu.Lock()
		for i := uint64(r.mu.state.TruncatedState.Index); i < truncateReq.Index; i++ {
			raftLogKey := keys.RaftLogKey(truncateReq.GroupID, i)
			value, err := eng.Get(raftLogKey)
			if err != nil && err != badger.ErrKeyNotFound {
				Err = multiraftbase.NewError(err)
				helper.Println(5, "Failed to remove raft log, index:", i, err)
				continue
			} else if err == badger.ErrKeyNotFound {
				helper.Println(5, "Failed to remove raft log, index:", i, err)
				continue
			} else {
				var entry raftpb.Entry
				err = entry.Unmarshal(value)
				if err != nil {
					helper.Println(5, "Failed to remove raft log, Entry Unmarshal failed index:", i, err)
					continue
				}
				if sniffSideloadedRaftCommand(entry.Data) {
					_, data := DecodeRaftCommand(entry.Data)
					var strippedCmd multiraftbase.RaftCommand
					if err := protoutil.Unmarshal(data, &strippedCmd); err != nil {
						helper.Println(5, "Failed to remove raft log, RaftCommand Unmarshal failed index:", i, err)
						continue
					}
					if strippedCmd.Method != multiraftbase.Put {
						err = helper.Errorf("Failed to remove raft log, side load raft log must be a put request", i, err)
						continue
					}
					putReq := multiraftbase.PutRequest{}
					err = putReq.Unmarshal(strippedCmd.WriteBatch.Data)
					if err != nil {
						helper.Printf(5, "Failed to remove raft log, PutRequest unmarshal failed", i, err)
						continue
					}
					err := eng.Clear(raftLogKey)
					if err != nil {
						Err = multiraftbase.NewError(err)
						helper.Println(5, "Failed to remove raft log, index:", i)
						continue
					}
					deltSize += int32(putReq.Size_) + int32(len(raftLogKey)) + int32(len(value))
				} else {
					err := eng.Clear(raftLogKey)
					if err != nil {
						Err = multiraftbase.NewError(err)
						helper.Println(5, "Failed to remove raft log, index:", i)
						continue
					}
					deltSize += int32(len(raftLogKey)) + int32(len(value))
				}
			}
		}
		r.mu.state.TruncatedState.Index = truncateReq.Index
		r.mu.state.TruncatedState.Term = truncateReq.Term
		r.mu.raftLogSize = r.mu.raftLogSize - int64(deltSize)
		r.mu.Unlock()

		helper.Println(5, "Finished to truncate log! GroupID:", truncateReq.GroupID, " index:", truncateReq.Index)

	} else if method == multiraftbase.Delete {
		deleteReq := multiraftbase.DeleteRequest{}
		resp = &multiraftbase.DeleteResponse{}
		err := deleteReq.Unmarshal(writeBatch.Data)
		if err != nil {
			Err = multiraftbase.NewError(err)
			helper.Printf(5, "Cannot unmarshal data to kv")
		}
		eng := r.store.LoadGroupEngine(r.Desc().GroupID)
		onode := multiraftbase.Onode{}
		value, err := eng.Get(deleteReq.Key)
		if err != nil {
			Err = multiraftbase.NewError(err)
			return
		}
		err = onode.Unmarshal(value)
		if err != nil {
			Err = multiraftbase.NewError(err)
			return
		}
		err = eng.Clear(onode.Key)
		if err != nil {
			Err = multiraftbase.NewError(err)
			helper.Println(5, "Failed to remove data:", onode.Key, deleteReq.Key, err)
		}
		err = eng.Clear(deleteReq.Key)
		if err != nil {
			Err = multiraftbase.NewError(err)
			helper.Println(5, "Failed to remove key:", deleteReq.Key, err)
		}
		helper.Println(5, "Finished to delete key! key:", deleteReq.Key)
		resp = &multiraftbase.DeleteResponse{}

	} else if method == multiraftbase.ChangeConf {
		resp = &multiraftbase.ChangeConfResponse{}
	} else {
		helper.Fatalln("Unexpected raft command method.")
	}

	return
}

func (r *Replica) getReplicaDescriptorByIDRLocked(
	replicaID multiraftbase.ReplicaID, fallback multiraftbase.ReplicaDescriptor,
) (multiraftbase.ReplicaDescriptor, error) {
	if repDesc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(replicaID); ok {
		return repDesc, nil
	}
	if fallback.ReplicaID == replicaID {
		return fallback, nil
	}
	return multiraftbase.ReplicaDescriptor{},
		errors.Errorf("replica %d not present in %v, %v", replicaID, fallback, r.mu.state.Desc.Replicas)
}

// sendRaftMessage sends a Raft message.
func (r *Replica) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	r.mu.Lock()
	helper.Printf(20, "sendRaftMessage: from: %d, to: %d", msg.From, msg.To)
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(multiraftbase.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(multiraftbase.ReplicaID(msg.To), r.mu.lastFromReplica)
	r.mu.Unlock()

	if fromErr != nil {
		helper.Printf(20, "failed to look up sender replica %d in r%d while sending %s: %s",
			msg.From, r.GroupID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		helper.Printf(20, "failed to look up recipient replica %d in r%d while sending %s: %s",
			msg.To, r.GroupID, msg.Type, toErr)
		return
	}

	// Raft-initiated snapshots are handled by the Raft snapshot queue.
	if msg.Type == raftpb.MsgSnap {
		if _, err := r.store.raftSnapshotQueue.Add(r, raftSnapshotPriority); err != nil {
			helper.Printf(5, "unable to add replica to Raft repair queue: %s", err)
		}
		return
	}

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false) {
		helper.Printf(20, "maybeCoalesceHeartbeat return for heartbeat msg ")
		return
	}

	if !r.sendRaftMessageRequest(ctx, &multiraftbase.RaftMessageRequest{
		GroupID:     r.GroupID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	}) {
		if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
			raftGroup.ReportUnreachable(msg.To)
			return true, nil
		}); err != nil {
			helper.Fatalln(err)
		}
	}
}

// maybeCoalesceHeartbeat returns true if the heartbeat was coalesced and added
// to the appropriate queue.
func (r *Replica) maybeCoalesceHeartbeat(
	ctx context.Context,
	msg raftpb.Message,
	toReplica, fromReplica multiraftbase.ReplicaDescriptor,
	quiesce bool,
) bool {
	var hbMap map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat
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
	beat := multiraftbase.RaftHeartbeat{
		GroupID:       r.GroupID,
		ToReplicaID:   toReplica.ReplicaID,
		FromReplicaID: fromReplica.ReplicaID,
		Term:          msg.Term,
		Commit:        msg.Commit,
		Quiesce:       quiesce,
		Context:       msg.Context,
	}

	toStore := multiraftbase.StoreIdent{
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

	//_ := r.AnnotateCtx(context.TODO())

	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			log.NewRaftLogger(helper.Logger),
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
			helper.Printf(3, "campaigning")
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

func makeIDKey() multiraftbase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = helper.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return multiraftbase.CmdIDKey(idKeyBuf)
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent {
		helper.Printf(5, "unquiescing: waking leader")
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
func (r *Replica) sendRaftMessageRequest(ctx context.Context, req *multiraftbase.RaftMessageRequest) bool {

	//helper.Printf(5, "sending raft request %+v", req)

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

func (r *Replica) cancelPendingCommandsLocked() {
	r.mu.AssertHeld()
	for _, p := range r.mu.proposals {
		resp := proposalResult{
			Reply: &multiraftbase.BatchResponse{},
			Err:   multiraftbase.NewError(multiraftbase.NewAmbiguousResultError("removing replica")),
		}
		p.finishRaftApplication(resp)
	}
	r.mu.proposals = map[multiraftbase.CmdIDKey]*ProposalData{}
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

func (r *Replica) setReplicaIDRaftMuLockedMuLocked(replicaID multiraftbase.ReplicaID) error {
	if r.mu.replicaID == replicaID {
		// The common case: the replica ID is unchanged.
		return nil
	}
	if replicaID == 0 {
		// If the incoming message does not have a new replica ID it is a
		// preemptive snapshot. We'll update minReplicaID if the snapshot is
		// accepted.
		return nil
	}

	//	previousReplicaID := r.mu.replicaID
	r.mu.replicaID = replicaID

	if replicaID >= r.mu.minReplicaID {
		r.mu.minReplicaID = replicaID + 1
	}

	r.mu.internalRaftGroup = nil
	// If there was a previous replica, repropose its pending commands under
	// this new incarnation.
	//if previousReplicaID != 0 {
	//	if log.V(1) {
	//		log.Infof(r.AnnotateCtx(context.TODO()), "changed replica ID from %d to %d",
	//			previousReplicaID, replicaID)
	//	}
	//	// repropose all pending commands under new replicaID.
	//	r.refreshProposalsLocked(0, reasonReplicaIDChanged)
	//}
	return nil
}

// addUnreachableRemoteReplica adds the given remote ReplicaID to be reported
// as unreachable on the next tick.
func (r *Replica) addUnreachableRemoteReplica(remoteReplica multiraftbase.ReplicaID) {
	r.unreachablesMu.Lock()
	if r.unreachablesMu.remotes == nil {
		r.unreachablesMu.remotes = make(map[multiraftbase.ReplicaID]struct{})
	}
	r.unreachablesMu.remotes[remoteReplica] = struct{}{}
	r.unreachablesMu.Unlock()
}

func (r *Replica) initRaftMuLockedReplicaMuLocked(
	desc *multiraftbase.GroupDescriptor, replicaID multiraftbase.ReplicaID,
) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		helper.Fatalf("r%d: cannot reinitialize an initialized replica", desc.GroupID)
	}
	helper.Printf(10, "initRaftMuLockedReplicaMuLocked() GroupID: %s, replicaID: %s", r.GroupID, replicaID)
	if desc.IsInitialized() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}

	r.mu.proposals = map[multiraftbase.CmdIDKey]*ProposalData{}
	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	var err error

	eng := r.store.LoadGroupEngine(desc.GroupID)
	if r.mu.state, err = r.mu.stateLoader.load(ctx, eng, desc); err != nil {
		return err
	}
	r.engine = eng
	r.mu.lastIndex, err = r.mu.stateLoader.loadLastIndex(ctx, eng)
	if err != nil {
		return err
	}
	r.mu.lastTerm = invalidLastTerm

	if replicaID == 0 {
		repDesc, ok := desc.GetReplicaDescriptor(r.store.nodeDesc.NodeID)
		if !ok {
			// This is intentionally not an error and is the code path exercised
			// during preemptive snapshots. The replica ID will be sent when the
			// actual raft replica change occurs.
			return nil
		}
		replicaID = repDesc.ReplicaID
	}

	if err := r.setReplicaIDRaftMuLockedMuLocked(replicaID); err != nil {
		return err
	}

	r.reqIDGen = idutil.NewGenerator(0, time.Now())
	r.applyWait = wait.NewTimeList()
	r.readStateC = make(chan raft.ReadState, 1)
	r.readNotifier = helper.NewNotifier()
	r.readwaitc = make(chan struct{}, 1)

	go r.linearizableReadLoop()

	return nil
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *multiraftbase.GroupDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

// setLastReplicaDescriptors sets the the most recently seen replica
// descriptors to those contained in the *RaftMessageRequest, acquiring r.mu
// to do so.
func (r *Replica) setLastReplicaDescriptors(req *multiraftbase.RaftMessageRequest) {
	r.mu.Lock()
	r.mu.lastFromReplica = req.FromReplica
	r.mu.lastToReplica = req.ToReplica
	r.mu.Unlock()
}

// mark the replica as quiesced. Returns true if the Replica is successfully
// quiesced and false otherwise.
func (r *Replica) quiesce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.quiesceLocked()
}

func (r *Replica) amLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.replicaID == r.mu.leaderID
}

func (r *Replica) quiesceLocked() bool {
	//_ := r.AnnotateCtx(context.TODO())
	if len(r.mu.proposals) != 0 {
		helper.Printf(5, "not quiescing: %d pending commands", len(r.mu.proposals))
		return false
	}
	if !r.mu.quiescent {
		helper.Printf(5, "quiescing")
		r.mu.quiescent = true
	}
	return true
}

func (r *Replica) unquiesceLocked() {
	if r.mu.quiescent {
		r.mu.quiescent = false
	}
}

func (r *Replica) setEstimatedCommitIndexLocked(commit uint64) {
	// The estimated commit index only ratchets up to account for Raft messages
	// arriving out of order.
	if r.mu.estimatedCommitIndex < commit {
		r.mu.estimatedCommitIndex = commit
	}
}

func (r *Replica) executeWriteBatch(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, *multiraftbase.Error) {
	br, pErr := r.tryExecuteWriteBatch(ctx, ba)
	return br, pErr
}

// maybeInitializeRaftGroup check whether the internal Raft group has
// not yet been initialized. If not, it is created and set to campaign
// if this replica is the most recent owner of the range lease.
func (r *Replica) maybeInitializeRaftGroup(ctx context.Context) {
	r.mu.RLock()
	// If this replica hasn't initialized the Raft group, create it and
	// unquiesce and wake the leader to ensure the replica comes up to date.
	initialized := r.mu.internalRaftGroup != nil
	r.mu.RUnlock()
	if initialized {
		return
	}

	// Acquire raftMu, but need to maintain lock ordering (raftMu then mu).
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	// Campaign if this replica is the current lease holder to avoid
	// an election storm after a recent split. If no replica is the
	// lease holder, all replicas must campaign to avoid waiting for
	// an election timeout to acquire the lease. In the latter case,
	// there's less chance of an election storm because this replica
	// will only campaign if it's been idle for >= election timeout,
	// so there's most likely been no traffic to the range.
	shouldCampaignOnCreation := true
	if err := r.withRaftGroupLocked(shouldCampaignOnCreation, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil {
		helper.Printf(5, "unable to initialize raft group: %s", err)
	}
}

func (r *Replica) maybeCanBeReadFromParent(oid []byte, offset, length uint64) ([]byte, error) {
	state, err := r.engine.Get([]byte("system_pg_state"))
	if err != nil {
		return nil, err
	}
	s, err := strconv.Atoi(string(state))
	if err != nil {
		return nil, err
	}
	if s&protos.PG_STATE_MIGRATING == 0 {
		return nil, errors.New("not in MIGRATE state")
	}
	return nil, nil
}

func (r *Replica) readKV(key []byte) (value []byte, pErr *multiraftbase.Error) {
	data, err := r.engine.Get(key)
	if err != nil {
		helper.Println(5, "Error getting onode from db. err ", err, key)
		if err == badger.ErrKeyNotFound {
			pErr = multiraftbase.NewError(multiraftbase.NewKeyNonExistent(key))
			return nil, pErr
		}
	}
	onode := multiraftbase.Onode{}
	err = onode.Unmarshal(data)
	if err != nil {
		helper.Println(5, "Error Unmarshal onode", err, key)
		pErr = multiraftbase.NewError(multiraftbase.NewOnodeBroken(key))
		return nil, pErr
	}

	data, err = r.engine.Get(onode.Key)
	if err != nil || int32(len(data)) != onode.Size_ {
		helper.Println(5, "Error getting data from db. err ", err, key, onode.Key)
		if err == badger.ErrKeyNotFound {
			pErr = multiraftbase.NewError(multiraftbase.NewDataLost(key))
			return nil, pErr
		}
	}
	return data, nil
}

// executeReadOnlyBatch updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the command queue.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {

	res := multiraftbase.BatchResponse{}
	req := ba.Request.GetValue().(multiraftbase.Request)
	if req.Method() == multiraftbase.Get {
		getReq := req.(*multiraftbase.GetRequest)
		r.store.enqueueRaftUpdateCheck(r.GroupID)
		r.linearizableReadNotify(ctx)
		//eng := r.store.LoadGroupEngine(r.Desc().GroupID)
		//data, err := eng.Get(getReq.Key)
		//if err != nil {
		//	helper.Println(5, "Error getting onode from db. err ", err, getReq.Key)
		//	if err == badger.ErrKeyNotFound {
		//		pErr = multiraftbase.NewError(multiraftbase.NewKeyNonExistent(getReq.Key))
		//	}
		//}
		//onode := multiraftbase.Onode{}
		//err = onode.Unmarshal(data)
		//if err != nil {
		//	helper.Println(5, "Error Unmarshal onode", err, getReq.Key)
		//	pErr = multiraftbase.NewError(multiraftbase.NewOnodeBroken(getReq.Key))
		//}
		//
		//data, err = eng.Get(onode.Key)
		//if err != nil {
		//	helper.Println(5, "Error getting onode from db. err ", err, getReq.Key)
		//	if err == badger.ErrKeyNotFound {
		//		pErr = multiraftbase.NewError(multiraftbase.NewDataLost(getReq.Key))
		//	}
		//}
		data := []byte{}
		data, pErr = r.readKV(getReq.Key)
		getRes := multiraftbase.GetResponse{}
		getRes.Value = data
		res.Responses = multiraftbase.ResponseUnion{Get: &getRes}
	} else {
		helper.Panicln(0, "Unsupported req type.")
	}

	return &res, pErr
}

func (r *Replica) ExistCheck(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (bool, *multiraftbase.Error) {
	ctx = r.AnnotateCtx(ctx)

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)
	//var pErr *multiraftbase.Error
	req := ba.Request.GetValue().(multiraftbase.Request)
	oid := []byte{}
	if req.Method() == multiraftbase.Get {
		getReq := req.(*multiraftbase.GetRequest)
		oid = getReq.Key
	} else if req.Method() == multiraftbase.Put {
		putReq := req.(*multiraftbase.PutRequest)
		oid = putReq.Key
	} else {
		return true, nil
	}
	r.store.enqueueRaftUpdateCheck(r.GroupID)
	r.linearizableReadNotify(ctx)
	//eng := r.store.LoadGroupEngine(r.Desc().GroupID)
	_, err := r.engine.Get(oid)
	if err != nil {
		return false, nil
	} else {
		return true, nil
	}

}

func (r *Replica) Send(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, *multiraftbase.Error) {
	var br *multiraftbase.BatchResponse

	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureContext(ctx, r.AmbientContext.Tracer, "replica send")
	defer cleanup()

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	var pErr *multiraftbase.Error
	req := ba.Request.GetValue().(multiraftbase.Request)
	switch req.Method() {
	case multiraftbase.Get:
		br, pErr = r.executeReadOnlyBatch(ctx, ba)
	case multiraftbase.Put, multiraftbase.TruncateLog, multiraftbase.Delete, multiraftbase.ChangeConf:
		br, pErr = r.executeWriteBatch(ctx, ba)
	default:
		helper.Fatalf("don't know how to handle command %s", ba)
	}

	if pErr != nil {
		helper.Printf(5, "replica.Send got error: %s", pErr)
	}

	return br, pErr
}

// insertProposalLocked assigns a MaxLeaseIndex to a proposal and adds
// it to the pending map.
func (r *Replica) insertProposalLocked(
	proposal *ProposalData, proposerReplica multiraftbase.ReplicaDescriptor,
) {
	proposal.command.ProposerReplica = proposerReplica

	if _, ok := r.mu.proposals[proposal.idKey]; ok {
		helper.Fatal("pending command already exists for %s", proposal.idKey)
	}
	r.mu.proposals[proposal.idKey] = proposal
}

func defaultSubmitProposalLocked(r *Replica, p *ProposalData) error {
	data, err := protoutil.Marshal(&p.command)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.GroupID)

	const largeProposalEventThresholdBytes = 2 << 19 // 512kb

	// Log an event if this is a large proposal. These are more likely to cause
	// blips or worse, and it's good to be able to pick them from traces.
	//
	// TODO(tschottdorf): can we mark them so lightstep can group them?

	if p.ChangeReplica != 0 {
		var confType raftpb.ConfChangeType
		if p.ChangeRepType == multiraftbase.ConfType_ADD_REPLICA {
			confType = raftpb.ConfChangeAddNode
		} else {
			confType = raftpb.ConfChangeRemoveNode
		}
		confChangeCtx := multiraftbase.ConfChangeContext{
			CommandID: string(p.idKey),
			Payload:   data,
			Replica:   p.RepDes,
		}
		encodedCtx, err := protoutil.Marshal(&confChangeCtx)
		if err != nil {
			return err
		}

		return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {

			return false, /* !unquiesceAndWakeLeader */
				raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    confType,
					NodeID:  p.ChangeReplica,
					Context: encodedCtx,
				})
		})
	}

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		var encode func(multiraftbase.CmdIDKey, []byte) []byte
		if p.command.Method == multiraftbase.Put {
			encode = encodeRaftCommandV2
		} else {
			encode = encodeRaftCommandV1
		}
		// We're proposing a command so there is no need to wake the leader if we
		// were quiesced.
		r.unquiesceLocked()
		return false /* !unquiesceAndWakeLeader */, raftGroup.Propose(encode(p.idKey, data))
	})
}

// submitProposalLocked proposes or re-proposes a command in r.mu.proposals.
// The replica lock must be held.
func (r *Replica) submitProposalLocked(p *ProposalData) error {
	p.proposedAtTicks = r.mu.ticks

	return defaultSubmitProposalLocked(r, p)
}

// requestToProposal converts a BatchRequest into a ProposalData, by
// evaluating it. The returned ProposalData is partially valid even
// on a non-nil *roachpb.Error.
func (r *Replica) requestToProposal(
	ctx context.Context,
	idKey multiraftbase.CmdIDKey,
	ba multiraftbase.BatchRequest,
) (*ProposalData, *multiraftbase.Error) {
	proposal := &ProposalData{
		ctx:     ctx,
		idKey:   idKey,
		doneCh:  make(chan proposalResult, 1),
		Request: &ba,
	}
	//var pErr *multiraftbase.Error

	// Fill out the results even if pErr != nil; we'll return the error below.
	var data []byte
	var err error
	req := ba.Request.GetValue().(multiraftbase.Request)
	if req.Method() == multiraftbase.Put {
		putReq := req.(*multiraftbase.PutRequest)
		data, err = putReq.Marshal()
		if err != nil {
			helper.Printf(5, "Error marshal put request.")
		}
	} else if req.Method() == multiraftbase.TruncateLog {
		truncateLogReq := req.(*multiraftbase.TruncateLogRequest)
		data, err = truncateLogReq.Marshal()
		if err != nil {
			helper.Printf(5, "Error marshal truncate request.")
		}
	} else if req.Method() == multiraftbase.Delete {
		deleteReq := req.(*multiraftbase.DeleteRequest)
		data, err = deleteReq.Marshal()
		if err != nil {
			helper.Printf(5, "Error marshal delete request.")
		}
	} else if req.Method() == multiraftbase.ChangeConf {
		changeConfReq := req.(*multiraftbase.ChangeConfRequest)
		data, err = changeConfReq.Marshal()
		if err != nil {
			helper.Printf(5, "Error marshal delete request.")
		}
		proposal.ChangeReplica = uint64(changeConfReq.ReplicaId)
		proposal.ChangeRepType = changeConfReq.ConfType
		proposal.RepDes = multiraftbase.ReplicaDescriptor{
			NodeID:    multiraftbase.NodeID(fmt.Sprintf("osd.%d", changeConfReq.OsdId)),
			ReplicaID: multiraftbase.ReplicaID(changeConfReq.ReplicaId),
		}
	} else {
		helper.Panicln(0, "Unsupported req type.")
	}
	proposal.command = multiraftbase.RaftCommand{
		Method:     req.Method(),
		WriteBatch: &multiraftbase.WriteBatch{Data: data},
	}

	return proposal, nil
}

// getReplicaDescriptorRLocked is like getReplicaDescriptor, but assumes that
// r.mu is held for either reading or writing.
func (r *Replica) getReplicaDescriptorRLocked() (multiraftbase.ReplicaDescriptor, error) {
	repDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(r.store.NodeID())
	if ok {
		return repDesc, nil
	}
	return multiraftbase.ReplicaDescriptor{}, nil
}

// IsDestroyed returns a non-nil error if the replica has been destroyed.
func (r *Replica) IsDestroyed() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.destroyed
}

func (r *Replica) propose(
	ctx context.Context,
	ba multiraftbase.BatchRequest,
) (chan proposalResult, *multiraftbase.Error) {
	if err := r.IsDestroyed(); err != nil {
		return nil, multiraftbase.NewError(err)
	}

	idKey := makeIDKey()
	proposal, _ := r.requestToProposal(ctx, idKey, ba)

	// submitProposalLocked calls withRaftGroupLocked which requires that
	// raftMu is held. In order to maintain our lock ordering we need to lock
	// Replica.raftMu here before locking Replica.mu.
	//
	// TODO(peter): It appears that we only need to hold Replica.raftMu when
	// calling raft.NewRawNode. We could get fancier with the locking here to
	// optimize for the common case where Replica.mu.internalRaftGroup is
	// non-nil, but that doesn't currently seem worth it. Always locking raftMu
	// has a tiny (~1%) performance hit for single-node block_writer testing.
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// NB: We need to check Replica.mu.destroyed again in case the Replica has
	// been destroyed between the initial check at the beginning of this method
	// and the acquisition of Replica.mu. Failure to do so will leave pending
	// proposals that never get cleared.
	if err := r.mu.destroyed; err != nil {
		return nil, multiraftbase.NewError(err)
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return nil, multiraftbase.NewError(err)
	}
	r.insertProposalLocked(proposal, repDesc)

	if err := r.submitProposalLocked(proposal); err != nil {
		delete(r.mu.proposals, proposal.idKey)
		return nil, multiraftbase.NewError(err)
	}

	return proposal.doneCh, nil
}

const SlowRequestThreshold = 60 * time.Second

func (r *Replica) tryExecuteWriteBatch(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {

	ch, pErr := r.propose(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(SlowRequestThreshold)
	for {
		select {
		case propResult := <-ch:
			helper.Printf(5, "successfully to propose data. ")
			return propResult.Reply, propResult.Err
		case <-slowTimer.C:
			slowTimer.Read = true
			helper.Printf(5, "have been waiting %s for proposing command %s",
				SlowRequestThreshold, ba)

		case <-ctxDone:
			ctxDone = nil
		}
	}
}

func (r *Replica) reportSnapshotStatus(to uint64, snapErr error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportSnapshot(to, snapStatus)
		return true, nil
	}); err != nil {
		//ctx := r.AnnotateCtx(context.TODO())
		helper.Fatal(err)
	}
}

// GetReplicaDescriptor returns the replica for this range from the range
// descriptor. Returns a *RangeNotFoundError if the replica is not found.
// No other errors are returned.
func (r *Replica) GetReplicaDescriptor() (multiraftbase.ReplicaDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getReplicaDescriptorRLocked()
}

const (
	snapTypeRaft       = "Raft"
	snapTypePreemptive = "preemptive"
)

func (r *Replica) setPendingSnapshotIndex(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We allow the pendingSnapshotIndex to change from 0 to 1 and then from 1 to
	// a value greater than 1. Any other change indicates 2 current preemptive
	// snapshots on the same replica which is disallowed.
	if (index == 1 && r.mu.pendingSnapshotIndex != 0) ||
		(index > 1 && r.mu.pendingSnapshotIndex != 1) {
		return errors.Errorf(
			"%s: can't set pending snapshot index to %d; pending snapshot already present: %d",
			r, index, r.mu.pendingSnapshotIndex)
	}
	r.mu.pendingSnapshotIndex = index
	return nil
}

type snapshotError struct {
	cause error
}

func (s *snapshotError) Error() string {
	return fmt.Sprintf("snapshot failed: %s", s.cause.Error())
}

// sendSnapshot sends a snapshot of the replica state to the specified
// replica. This is used for both preemptive snapshots that are performed
// before adding a replica to a range, and for Raft-initiated snapshots that
// are used to bring a replica up to date that has fallen too far
// behind. Currently only invoked from replicateQueue and raftSnapshotQueue. Be
// careful about adding additional calls as generating a snapshot is moderately
// expensive.
func (r *Replica) sendSnapshot(
	ctx context.Context,
	repDesc multiraftbase.ReplicaDescriptor,
	snapType string,
	priority multiraftbase.SnapshotRequest_Priority,
) error {
	helper.Println(5, "Send replica groupId:", r.GroupID, " snapshot", " r.Desc().GroupID:", r.Desc().GroupID)
	snap, err := r.GetSnapshot(ctx, snapType)
	if err != nil {
		return errors.Wrapf(err, "%s: failed to generate %s snapshot", r, snapType)
	}
	defer snap.Close()

	fromRepDesc, err := r.GetReplicaDescriptor()
	if err != nil {
		return errors.Wrapf(err, "%s: change replicas failed", r)
	}

	if snapType == snapTypePreemptive {
		if err := r.setPendingSnapshotIndex(snap.RaftSnap.Metadata.Index); err != nil {
			return err
		}
	}

	status := r.RaftStatus()
	if status == nil {
		return errors.New("raft status not initialized")
	}

	req := multiraftbase.SnapshotRequest_Header{
		State: snap.State,
		RaftMessageRequest: multiraftbase.RaftMessageRequest{
			GroupID:     r.GroupID,
			FromReplica: fromRepDesc,
			ToReplica:   repDesc,
			Message: raftpb.Message{
				Type:     raftpb.MsgSnap,
				To:       uint64(repDesc.ReplicaID),
				From:     uint64(fromRepDesc.ReplicaID),
				Term:     status.Term,
				Snapshot: snap.RaftSnap,
			},
		},
		// Recipients can choose to decline preemptive snapshots.
		CanDecline: snapType == snapTypePreemptive,
		Priority:   priority,
	}
	sent := func() {

	}
	if err := r.store.cfg.Transport.SendSnapshot(
		ctx, req, snap, sent); err != nil {
		return &snapshotError{err}
	}
	return nil
}
