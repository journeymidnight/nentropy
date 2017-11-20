package multiraft

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
	"github.com/journeymidnight/nentropy/multiraft/keys"
	"golang.org/x/net/context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const ()

// Engines is a container of engines, allowing convenient closing.
type Engines map[string]engine.Engine

var schedulerConcurrency = envutil.EnvOrDefaultInt(
	"NENTROPY_SCHEDULER_CONCURRENCY", 8*runtime.NumCPU())

type StoreConfig struct {
	RaftConfig
	Transport                  *RaftTransport
	RPCContext                 *rpc.Context
	RaftHeartbeatIntervalTicks int
}

type raftRequestInfo struct {
	req        *multiraftbase.RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos []raftRequestInfo
}

type Store struct {
	cfg StoreConfig
	mu  struct {
		sync.Mutex
		replicas       sync.Map //map[multiraftbase.GroupID]*Replica
		uninitReplicas map[multiraftbase.GroupID]*Replica
	}
	engines       Engines
	started       int32
	stopper       *stop.Stopper
	replicaQueues sync.Map // map[multiraftbase.GroupID]*raftRequestQueue
	//	raftRequestQueues map[multiraftbase.GroupID]*raftRequestQueue
	scheduler *raftScheduler
}

func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()
}

func NewStore(cfg StoreConfig) *Store {
	cfg.SetDefaults()
	s := &Store{
		cfg: cfg,
	}
	s.scheduler = newRaftScheduler(s, schedulerConcurrency)
	s.mu.Lock()
	//init in mu
	s.mu.Unlock()
	return s
}

func (s *Store) processReady(ctx context.Context, id multiraftbase.GroupID) {
	value, ok := s.mu.replicas.Load(id)
	if !ok {
		return
	}

	start := timeutil.Now()
	r := (*Replica)(value)
	stats, expl, err := r.handleRaftReady(IncomingSnapshot{})
	if err != nil {
		helper.Logger.Fatalf(5, "%s", err) // TODO(bdarnell)
	}
	if !r.IsInitialized() {
	}
}

func (s *Store) processRequestQueue(ctx context.Context, id multiraftbase.GroupID) {
	value, ok := s.replicaQueues.Load(id)
	if !ok {
		return
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	infos := q.infos
	q.infos = nil
	q.Unlock()

	for _, info := range infos {
		if pErr := s.processRaftRequest(info.respStream.Context(), info.req, IncomingSnapshot{}); pErr != nil {
			// If we're unable to process the request, clear the request queue. This
			// only happens if we couldn't create the replica because the request was
			// targeted to a removed range. This is also racy and could cause us to
			// drop messages to the deleted range occasionally (#18355), but raft
			// will just retry.
			q.Lock()
			if len(q.infos) == 0 {
				s.replicaQueues.Delete(id)
			}
			q.Unlock()
			if err := info.respStream.Send(newRaftMessageResponse(info.req, pErr)); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				helper.Logger.Printf(5, "error sending error: %s", err)
			}
		}
	}
}

func (s *Store) processTick(ctx context.Context, id multiraftbase.GroupID) bool {
	value, ok := s.mu.replicas.Load(id))
	if !ok {
		return false
	}

	start := timeutil.Now()
	r := (*Replica)(value)
	exists, err := r.tick()
	if err != nil {
		helper.Logger.Println(5, err)
	}
	return exists // ready
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	s.cfg.Transport.Listen(s.StoreID(), s)
	s.processRaft(ctx)
}

func (s *Store) processRaft(ctx context.Context) {
	if s.cfg.TestingKnobs.DisableProcessRaft {
		return
	}

	s.scheduler.Start(ctx, s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	s.stopper.RunWorker(ctx, s.scheduler.Wait)

	s.stopper.RunWorker(ctx, s.raftTickLoop)
	s.stopper.RunWorker(ctx, s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.Stop(s.StoreID())
	}))
}

func (s *Store) raftTickLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()

	var rangeIDs []RangeID

	for {
		select {
		case <-ticker.C:
			rangeIDs = rangeIDs[:0]

			s.mu.replicas.Range(func(k int64, v unsafe.Pointer) bool {
				// Fast-path handling of quiesced replicas. This avoids the overhead of
				// queueing the replica on the Raft scheduler. This overhead is
				// significant and there is overhead to filling the Raft scheduler with
				// replicas to tick. A node with 3TB of disk might contain 50k+
				// replicas. Filling the Raft scheduler with all of those replicas
				// every tick interval can starve other Raft processing of cycles.
				//
				// Why do we bother to ever queue a Replica on the Raft scheduler for
				// tick processing? Couldn't we just call Replica.tick() here? Yes, but
				// then a single bad/slow Replica can disrupt tick processing for every
				// Replica on the store which cascades into Raft elections and more
				// disruption. Replica.maybeTickQuiesced only grabs short-duration
				// locks and not locks that are held during disk I/O.
				if !(*Replica)(v).maybeTickQuiesced() {
					rangeIDs = append(rangeIDs, protos.RangeID(k))
				}
				return true
			})

			s.scheduler.EnqueueRaftTick(rangeIDs...)

		case <-s.stopper.ShouldStop():
			return
		}
	}
}

func (s *Store) coalescedHeartbeatsLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CoalescedHeartbeatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendQueuedHeartbeats(ctx)
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

func (s *Store) sendQueuedHeartbeats(ctx context.Context) {
	s.coalescedMu.Lock()
	heartbeats := s.coalescedMu.heartbeats
	heartbeatResponses := s.coalescedMu.heartbeatResponses
	s.coalescedMu.heartbeats = map[protos.StoreIdent][]protos.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[protos.StoreIdent][]protos.RaftHeartbeat{}
	s.coalescedMu.Unlock()

	var beatsSent int

	for to, beats := range heartbeats {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, beats, nil, to)
	}
	for to, resps := range heartbeatResponses {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, nil, resps, to)
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (s *Store) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []multiraftbase.RaftHeartbeat, to multiraftbase.StoreIdent,
) int {
	var msgType raftpb.MessageType

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	} else if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	} else if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	} else {
		helper.Logger.Fatalf(5, "cannot coalesce both heartbeats and responses")
	}

	chReq := &multiraftbase.RaftMessageRequest{
		RangeID: 0,
		ToReplica: multiraftbase.ReplicaDescriptor{
			NodeID:    to.NodeID,
			StoreID:   to.StoreID,
			ReplicaID: 0,
		},
		FromReplica: multiraftbase.ReplicaDescriptor{
			NodeID:  s.Ident.NodeID,
			StoreID: s.Ident.StoreID,
		},
		Message: raftpb.Message{
			Type: msgType,
		},
		Heartbeats:     beats,
		HeartbeatResps: resps,
	}

	if !s.cfg.Transport.SendAsync(chReq) {
		for _, beat := range beats {
			if value, ok := s.mu.replicas.Load(int64(beat.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if value, ok := s.mu.replicas.Load(int64(resp.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(resp.ToReplicaID)
			}
		}
		return 0
	}
	return len(beats) + len(resps)
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, respStream RaftMessageResponseStream,
) *multiraftbase.Error {
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		if req.RangeID != 0 {
			helper.Logger.Fatalf(5, "coalesced heartbeats must have rangeID == 0")
		}
		s.uncoalesceBeats(ctx, req.Heartbeats, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeat, respStream)
		s.uncoalesceBeats(ctx, req.HeartbeatResps, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeatResp, respStream)
		return nil
	}
	return s.HandleRaftUncoalescedRequest(ctx, req, respStream)
}

func (s *Store) uncoalesceBeats(
	ctx context.Context,
	beats []multiraftbase.RaftHeartbeat,
	fromReplica, toReplica multiraftbase.ReplicaDescriptor,
	msgT raftpb.MessageType,
	respStream RaftMessageResponseStream,
) {
	if len(beats) == 0 {
		return
	}

	helper.Logger.Printf(5, "uncoalescing %d beats of type %v: %+v", len(beats), msgT, beats)

	beatReqs := make([]multiraftbase.RaftMessageRequest, len(beats))
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   uint64(beat.FromReplicaID),
			To:     uint64(beat.ToReplicaID),
			Term:   beat.Term,
			Commit: beat.Commit,
		}
		beatReqs[i] = multiraftbase.RaftMessageRequest{
			RangeID: beat.RangeID,
			FromReplica: multiraftbase.ReplicaDescriptor{
				NodeID:    fromReplica.NodeID,
				StoreID:   fromReplica.StoreID,
				ReplicaID: beat.FromReplicaID,
			},
			ToReplica: multiraftbase.ReplicaDescriptor{
				NodeID:    toReplica.NodeID,
				StoreID:   toReplica.StoreID,
				ReplicaID: beat.ToReplicaID,
			},
			Message: msg,
			Quiesce: beat.Quiesce,
		}

		helper.Logger.Printf(5, "uncoalesced beat: %+v", beatReqs[i])

		if err := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream); err != nil {
			helper.Logger.Printf(5, "could not handle uncoalesced heartbeat %s", err)
		}
	}
}

// getOrCreateReplica returns a replica for the given RangeID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
func (s *Store) getOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	for {
		r, created, err := s.tryGetOrCreateReplica(
			ctx,
			rangeID,
			replicaID,
			creatingReplica,
		)
		if err == errRetry {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		return r, created, err
	}
}

// tryGetOrCreateReplica performs a single attempt at trying to lookup or
// create a replica. It will fail with errRetry if it finds a Replica that has
// been destroyed (and is no longer in Store.mu.replicas) or if during creation
// another goroutine gets there first. In either case, a subsequent call to
// tryGetOrCreateReplica will likely succeed, hence the loop in
// getOrCreateReplica.
func (s *Store) tryGetOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing (initialized) replica.
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		repl := (*Replica)(value)
		if creatingReplica != nil {
			// Drop messages that come from a node that we believe was once a member of
			// the group but has been removed.
			desc := repl.Desc()
			_, found := desc.GetReplicaDescriptorByID(creatingReplica.ReplicaID)
			// It's not a current member of the group. Is it from the past?
			if !found && creatingReplica.ReplicaID < desc.NextReplicaID {
				return nil, false, roachpb.NewReplicaTooOldError(creatingReplica.ReplicaID)
			}
		}

		repl.raftMu.Lock()
		repl.mu.RLock()
		destroyed, corrupted := repl.mu.destroyed, repl.mu.corrupted
		repl.mu.RUnlock()
		if destroyed != nil {
			repl.raftMu.Unlock()
			if corrupted {
				return nil, false, destroyed
			}
			return nil, false, errRetry
		}
		repl.mu.Lock()
		if err := repl.setReplicaIDRaftMuLockedMuLocked(replicaID); err != nil {
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			return nil, false, err
		}
		repl.mu.Unlock()
		return repl, false, nil
	}

	// No replica currently exists, so we'll try to create one. Before creating
	// the replica, see if there is a tombstone which would indicate that this is
	// a stale message.
	tombstoneKey := keys.RaftTombstoneKey(rangeID)
	var tombstone roachpb.RaftTombstone
	if ok, err := engine.MVCCGetProto(
		ctx, s.Engine(), tombstoneKey, hlc.Timestamp{}, true, nil, &tombstone,
	); err != nil {
		return nil, false, err
	} else if ok {
		if replicaID != 0 && replicaID < tombstone.NextReplicaID {
			return nil, false, &roachpb.RaftGroupDeletedError{}
		}
	}

	// Create a new replica and lock it for raft processing.
	repl := newReplica(rangeID, s)
	repl.creatingReplica = creatingReplica
	repl.raftMu.Lock()

	// Install the replica in the store's replica map. The replica is in an
	// inconsistent state, but nobody will be accessing it while we hold its
	// locks.
	s.mu.Lock()
	// Grab the internal Replica state lock to ensure nobody mucks with our
	// replica even outside of raft processing. Have to do this after grabbing
	// Store.mu to maintain lock ordering invariant.
	repl.mu.Lock()
	repl.mu.minReplicaID = tombstone.NextReplicaID
	// Add the range to range map, but not replicasByKey since the range's start
	// key is unknown. The range will be added to replicasByKey later when a
	// snapshot is applied. After unlocking Store.mu above, another goroutine
	// might have snuck in and created the replica, so we retry on error.
	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		repl.mu.Unlock()
		s.mu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, errRetry
	}
	s.mu.uninitReplicas[repl.RangeID] = repl
	s.mu.Unlock()

	desc := &multiraftbase.RangeDescriptor{
		RangeID: rangeID,
		// TODO(bdarnell): other fields are unknown; need to populate them from
		// snapshot.
	}
	if err := repl.initRaftMuLockedReplicaMuLocked(desc, s.Clock(), replicaID); err != nil {
		// Mark the replica as destroyed and remove it from the replicas maps to
		// ensure nobody tries to use it
		repl.mu.destroyed = errors.Wrapf(err, "%s: failed to initialize", repl)
		repl.mu.Unlock()
		s.mu.Lock()
		s.mu.replicas.Delete(int64(rangeID))
		delete(s.mu.uninitReplicas, rangeID)
		s.replicaQueues.Delete(int64(rangeID))
		s.mu.Unlock()
		repl.raftMu.Unlock()
		return nil, false, err
	}
	repl.mu.Unlock()
	return repl, true, nil
}

func (s *Store) processRaftRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, inSnap IncomingSnapshot,
) (pErr *multiraftbase.Error) {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.RangeID,
		req.ToReplica.ReplicaID,
		&req.FromReplica,
	)
	if err != nil {
		return multiraftbase.NewError(err)
	}
	ctx = r.AnnotateCtx(ctx)
	defer r.raftMu.Unlock()
	r.setLastReplicaDescriptors(req)

	if req.Quiesce {
		if req.Message.Type != raftpb.MsgHeartbeat {
			log.Fatalf(ctx, "unexpected quiesce: %+v", req)
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			if r.quiesce() {
				return
			}
		}

		helper.Logger.Printf(5, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)

	}

	// Snapshots addressed to replica ID 0 are permitted; this is the
	// mechanism by which preemptive snapshots work. No other requests to
	// replica ID 0 are allowed.
	//
	// Note that just because the ToReplica's ID is 0 it does not necessarily
	// mean that the replica's current ID is 0. We allow for preemptive snaphots
	// to be applied to initialized replicas as of #8613.
	if req.ToReplica.ReplicaID == 0 {
		if req.Message.Type != raftpb.MsgSnap {
			helper.Logger.Printf(5, "refusing incoming Raft message %s from %+v to %+v",
				req.Message.Type, req.FromReplica, req.ToReplica)
			return multiraftbase.NewErrorf(
				"cannot recreate replica that is not a member of its range (StoreID %s not found in r%d)",
				r.store.StoreID(), req.RangeID,
			)
		}

		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			// We need to remove the placeholder regardless of whether the snapshot
			// applied successfully or not.
			if addedPlaceholder {
				// Clear the replica placeholder; we are about to swap it with a real replica.
				if !s.removePlaceholderLocked(ctx, req.RangeID) {
					log.Fatalf(ctx, "could not remove placeholder after preemptive snapshot")
				}
				if pErr == nil {
					atomic.AddInt32(&s.counts.filledPlaceholders, 1)
				} else {
					atomic.AddInt32(&s.counts.removedPlaceholders, 1)
				}
				removePlaceholder = false
			}

			if pErr == nil {
				// If the snapshot succeeded, process the range descriptor update.
				if err := s.processRangeDescriptorUpdateLocked(ctx, r); err != nil {
					pErr = multiraftbase.NewError(err)
				}
			}
		}()

		// Requiring that the Term is set in a message makes sure that we
		// get all of Raft's internal safety checks (it confuses messages
		// at term zero for internal messages). The sending side uses the
		// term from the snapshot itself, but we'll just check nonzero.
		if req.Message.Term == 0 {
			return multiraftbase.NewErrorf(
				"preemptive snapshot from term %d received with zero term",
				req.Message.Snapshot.Metadata.Term,
			)
		}
		// TODO(tschottdorf): A lot of locking of the individual Replica
		// going on below as well. I think that's more easily refactored
		// away; what really matters is that the Store doesn't do anything
		// else with that same Replica (or one that might conflict with us
		// while we still run). In effect, we'd want something like:
		//
		// 1. look up the snapshot's key range
		// 2. get an exclusive lock for operations on that key range from
		//    the store (or discard the snapshot)
		//    (at the time of writing, we have checked the key range in
		//    canApplySnapshotLocked above, but there are concerns about two
		//    conflicting operations passing that check simultaneously,
		//    see #7830)
		// 3. do everything below (apply the snapshot through temp Raft group)
		// 4. release the exclusive lock on the snapshot's key range
		//
		// There are two future outcomes: Either we begin receiving
		// legitimate Raft traffic for this Range (hence learning the
		// ReplicaID and becoming a real Replica), or the Replica GC queue
		// decides that the ChangeReplicas as a part of which the
		// preemptive snapshot was sent has likely failed and removes both
		// in-memory and on-disk state.
		r.mu.Lock()
		r.mu.internalRaftGroup = nil
		needTombstone := r.mu.state.Desc.NextReplicaID != 0
		r.mu.Unlock()

		appliedIndex, _, err := r.raftMu.stateLoader.loadAppliedIndex(ctx, r.store.Engine())
		if err != nil {
			return multiraftbase.NewError(err)
		}
		raftGroup, err := raft.NewRawNode(
			newRaftConfig(
				raft.Storage((*replicaRaftStorage)(r)),
				preemptiveSnapshotRaftGroupID,
				// We pass the "real" applied index here due to subtleties
				// arising in the case in which Raft discards the snapshot:
				// It would instruct us to apply entries, which would have
				// crashing potential for any choice of dummy value below.
				appliedIndex,
				r.store.cfg,
				helper.Logger,
			), nil)
		if err != nil {
			return multiraftbase.NewError(err)
		}
		// We have a Raft group; feed it the message.
		if err := raftGroup.Step(req.Message); err != nil {
			return multiraftbase.NewError(errors.Wrap(err, "unable to process preemptive snapshot"))
		}
		// In the normal case, the group should ask us to apply a snapshot.
		// If it doesn't, our snapshot was probably stale. In that case we
		// still go ahead and apply a noop because we want that case to be
		// counted by stats as a successful application.
		var ready raft.Ready
		if raftGroup.HasReady() {
			ready = raftGroup.Ready()
		}

		if needTombstone {
			// Bump the min replica ID, but don't write the tombstone key. The
			// tombstone key is not expected to be present when normal replica data
			// is present and applySnapshot would delete the key in most cases. If
			// Raft has decided the snapshot shouldn't be applied we would be
			// writing the tombstone key incorrectly.
			r.mu.Lock()
			r.mu.minReplicaID = r.nextReplicaIDLocked(nil)
			r.mu.Unlock()
		}

		// Apply the snapshot, as Raft told us to.
		if err := r.applySnapshot(ctx, inSnap, ready.Snapshot, ready.HardState); err != nil {
			return multiraftbase.NewError(err)
		}

		// At this point, the Replica has data but no ReplicaID. We hope
		// that it turns into a "real" Replica by means of receiving Raft
		// messages addressed to it with a ReplicaID, but if that doesn't
		// happen, at some point the Replica GC queue will have to grab it.
		//
		// NB: See the defer at the start of this block for the removal of the
		// placeholder and processing of the range descriptor update.
		return nil
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// We're processing a message from another replica which means that the
		// other replica is not quiesced, so we don't need to wake the leader.
		r.unquiesceLocked()
		if req.Message.Type == raftpb.MsgApp {
			r.setEstimatedCommitIndexLocked(req.Message.Commit)
		}
		return false, /* !unquiesceAndWakeLeader */
			raftGroup.Step(req.Message)
	}); err != nil {
		return multiraftbase.NewError(err)
	}

	if _, expl, err := r.handleRaftReadyRaftMuLocked(inSnap); err != nil {
		// Mimic the behavior in processRaft.
		helper.Logger.Fatalf(5, "%s: %s", log.Safe(expl), err) // TODO(bdarnell)
	}
	removePlaceholder = false
	return nil
}

// HandleRaftUncoalescedRequest dispatches a raft message to the appropriate
// Replica. It requires that s.mu is not held.
func (s *Store) HandleRaftUncoalescedRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, respStream RaftMessageResponseStream,
) *multiraftbase.Error {

	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		helper.Logger.Fatalf(5, "HandleRaftUncoalescedRequest cannot be given coalesced heartbeats or heartbeat responses, received %s", req)
	}
	// HandleRaftRequest is called on locally uncoalesced heartbeats (which are
	// not sent over the network if the environment variable is set) so do not
	// count them.

	if respStream == nil {
		return s.processRaftRequest(ctx, req, IncomingSnapshot{})
	}

	value, ok := s.replicaQueues.Load(int64(req.RangeID))
	if !ok {
		value, _ = s.replicaQueues.LoadOrStore(int64(req.RangeID), unsafe.Pointer(&raftRequestQueue{}))
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	if len(q.infos) >= replicaRequestQueueSize {
		q.Unlock()
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		return nil
	}
	q.infos = append(q.infos, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})
	q.Unlock()

	s.scheduler.EnqueueRaftRequest(req.RangeID)
	return nil
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(rangeID protos.RangeID) (*Replica, error) {
	if value, ok := s.mu.replicas.Load(int64(rangeID)); ok {
		return (*Replica)(value), nil
	}
	return nil, protos.NewRangeNotFoundError(rangeID)
}

// HandleRaftResponse implements the RaftMessageHandler interface.
// It requires that s.mu is not held.
func (s *Store) HandleRaftResponse(ctx context.Context, resp *multiraftbase.RaftMessageResponse) error {
	ctx = s.AnnotateCtx(ctx)
	switch val := resp.Union.GetValue().(type) {
	case *multiraftbase.Error:
		switch tErr := val.GetDetail().(type) {
		case *multiraftbase.ReplicaTooOldError:
			repl, err := s.GetReplica(resp.RangeID)
			if err != nil {
				// RangeNotFoundErrors are expected here; nothing else is.
				if _, ok := err.(*protos.RangeNotFoundError); !ok {
					helper.Logger.Printfln(5, err)
				}
				return nil
			}
			repl.mu.Lock()
			// If the replica ID in the error matches (which is the usual
			// case; the exception is when a replica has been removed and
			// re-added rapidly), we know the replica will be removed and we
			// can cancel any pending commands. This is sometimes necessary
			// to unblock PushTxn operations that are necessary for the
			// replica GC to succeed.
			if tErr.ReplicaID == repl.mu.replicaID {
				repl.cancelPendingCommandsLocked()
			}
			repl.mu.Unlock()
			replCtx := repl.AnnotateCtx(ctx)
			added, err := s.replicaGCQueue.Add(
				repl, replicaGCPriorityRemoved,
			)
			if err != nil {
				helper.Logger.Printf(5, "unable to add to replica GC queue: %s", err)
			} else if added {
				helper.Logger.Printf(5, "added to replica GC queue (peer suggestion)")
			}
		case *multiraftbase.StoreNotFoundError:
			helper.Logger.Printf(5, "raft error: node %d claims to not contain store %d for replica %s: %s",
				resp.FromReplica.NodeID, resp.FromReplica.StoreID, resp.FromReplica, val)
			return val.GetDetail()
		default:
			helper.Logger.Printf(5, "got error from r%d, replica %s: %s",
				resp.RangeID, resp.FromReplica, val)
		}

	default:
		helper.Logger.Printf(5, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
	}
	return nil
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg StoreConfig, logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:            id,
		Applied:       appliedIndex,
		ElectionTick:  storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick: storeCfg.RaftHeartbeatIntervalTicks,
		Storage:       strg,
		Logger:        logger,

		// TODO(bdarnell): PreVote and CheckQuorum are two ways of
		// achieving the same thing. PreVote is more compatible with
		// quiesced ranges, so we want to switch to it once we've worked
		// out the bugs.
		PreVote:     enablePreVote,
		CheckQuorum: !enablePreVote,

		// MaxSizePerMsg controls how many Raft log entries the leader will send to
		// followers in a single MsgApp.
		MaxSizePerMsg: uint64(raftMaxSizePerMsg),
		// MaxInflightMsgs controls how many "inflight" messages Raft will send to
		// a follower without hearing a response. The total number of Raft log
		// entries is a combination of this setting and MaxSizePerMsg. The current
		// settings provide for up to 1 MB of raft log to be sent without
		// acknowledgement. With an average entry size of 1 KB that translates to
		// ~1024 commands that might be executed in the handling of a single
		// raft.Ready operation.
		MaxInflightMsgs: raftMaxInflightMsgs,
	}
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	s.metrics.raftRcvdMessages[raftpb.MsgSnap].Inc(1)

	if s.IsDraining() {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_DECLINED,
			Message: storeDrainingMsg,
		})
	}

	ctx := s.AnnotateCtx(stream.Context())
	cleanup, rejectionMsg, err := s.reserveSnapshot(ctx, header)
	if err != nil {
		return err
	}
	if cleanup == nil {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_DECLINED,
			Message: rejectionMsg,
		})
	}
	defer cleanup()

	sendSnapError := func(err error) error {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_ERROR,
			Message: err.Error(),
		})
	}

	// Check to see if the snapshot can be applied but don't attempt to add
	// a placeholder here, because we're not holding the replica's raftMu.
	// We'll perform this check again later after receiving the rest of the
	// snapshot data - this is purely an optimization to prevent downloading
	// a snapshot that we know we won't be able to apply.
	if _, err := s.canApplySnapshot(ctx, header.State.Desc); err != nil {
		return sendSnapError(
			errors.Wrapf(err, "%s,r%d: cannot apply snapshot", s, header.State.Desc.RangeID),
		)
	}

	if err := stream.Send(&SnapshotResponse{Status: SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	var batches [][]byte
	var logEntries [][]byte
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if req.Header != nil {
			return sendSnapError(errors.New("client error: provided a header mid-stream"))
		}

		if req.KVBatch != nil {
			batches = append(batches, req.KVBatch)
		}
		if req.LogEntries != nil {
			logEntries = append(logEntries, req.LogEntries...)
		}
		if req.Final {
			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				return sendSnapError(errors.Wrap(err, "invalid snapshot"))
			}

			inSnap := IncomingSnapshot{
				SnapUUID:   snapUUID,
				Batches:    batches,
				LogEntries: logEntries,
				State:      &header.State,
				snapType:   snapTypeRaft,
			}
			if header.RaftMessageRequest.ToReplica.ReplicaID == 0 {
				inSnap.snapType = snapTypePreemptive
			}

			if err := s.processRaftSnapshotRequest(ctx, &header.RaftMessageRequest, inSnap); err != nil {
				return sendSnapError(errors.Wrap(err.GoError(), "failed to apply snapshot"))
			}
			return stream.Send(&SnapshotResponse{Status: SnapshotResponse_APPLIED})
		}
	}
}

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engine }

// StoreID accessor.
func (s *Store) StoreID() StoreID { return s.Ident.StoreID }