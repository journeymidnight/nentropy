package main

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/badger"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/envutil"
	"github.com/journeymidnight/nentropy/util/shuffle"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
	"github.com/journeymidnight/nentropy/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/journeymidnight/nentropy/protos"
	"golang.org/x/time/rate"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	storeDrainingMsg = "store is draining"
	SNAP_CHUNK_SIZE  = 1 << 22
)

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"NENTROPY_SCHEDULER_CONCURRENCY", 8)

var enablePreVote = envutil.EnvOrDefaultBool(
	"NENTROPY_ENABLE_PREVOTE", false)

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreConfig struct {
	AmbientCtx log.AmbientContext
	BaseDir    string
	NodeID     int
	helper.RaftConfig
	Transport                   *RaftTransport
	RPCContext                  *rpc.Context
	RaftHeartbeatIntervalTicks  int
	CoalescedHeartbeatsInterval time.Duration
	// ScanInterval is the default value for the scan interval
	ScanInterval time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	ScanMaxIdleTime time.Duration
}

type raftRequestInfo struct {
	req        *multiraftbase.RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos  []raftRequestInfo
	idxMap map[string]int
}

//type ReplicaStateChangeCallback func(pgId string, state string)

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident multiraftbase.StoreIdent
	cfg   StoreConfig
	Db    *client.DB
	mu    struct {
		sync.Mutex
		replicas       sync.Map //map[multiraftbase.GroupID]*Replica
		uninitReplicas map[multiraftbase.GroupID]*Replica
	}

	engines        sync.Map //map[multiraftbase.GroupID]engine.Engine
	sysEng         engine.Engine
	raftEntryCache *raftEntryCache
	started        int32
	stopper        *stop.Stopper
	nodeDesc       *multiraftbase.NodeDescriptor
	//	replicaGCQueue     *replicaGCQueue
	replicaQueues sync.Map // map[multiraftbase.GroupID]*raftRequestQueue
	//	raftRequestQueues map[multiraftbase.GroupID]*raftRequestQueue
	scheduler *raftScheduler

	raftLogQueue        *raftLogQueue        // Raft log truncation queue
	raftSnapshotQueue   *raftSnapshotQueue   // Raft repair queue
	transferLeaderQueue *transferLeaderQueue // transfer leader queue
	scanner             *replicaScanner      // Replica scanner
	coalescedMu         struct {
		syncutil.Mutex
		heartbeats         map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat
		heartbeatResponses map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat
	}

	// draining holds a bool which indicates whether this store is draining. See
	// SetDraining() for a more detailed explanation of behavior changes.
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`, which at
	// the time of its creation was riddled with deadlock (but that situation
	// has likely improved).
	draining atomic.Value
}

func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()
}

func (s *Store) LoadGroupEngine(groupID multiraftbase.GroupID) engine.Engine {
	value, ok := s.engines.Load(groupID)
	if !ok {
		helper.Fatal("Cannot find db handle when write data. Group:", string(groupID))
	}
	eng, ok := value.(engine.Engine)
	if !ok {
		helper.Fatal("Cannot convert to db handle when write data. Group:", string(groupID))
	}
	return eng
}

func (s *Store) ShutdownGroupEngine(groupID multiraftbase.GroupID) {
	value, ok := s.engines.Load(groupID)
	if !ok {
		helper.Fatal("Cannot find db handle when write data. Group:", string(groupID))
	}
	eng, ok := value.(engine.Engine)
	if !ok {
		helper.Fatal("Cannot convert to db handle when write data. Group:", string(groupID))
	}
	eng.Close()
	s.engines.Delete(groupID)
	return
}

func (s *Store) processReady(ctx context.Context, id multiraftbase.GroupID) {
	value, ok := s.mu.replicas.Load(id)
	if !ok {
		return
	}

	start := timeutil.Now()
	r, ok := value.(*Replica)
	if !ok {
		return
	}
	stats, expl, err := r.handleRaftReady(IncomingSnapshot{})

	if err != nil {
		helper.Fatalf("%v, %s", expl, err) // TODO(bdarnell)
	}

	elapsed := timeutil.Since(start)
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		helper.Printf(5, "handle raft ready: %.1fs [processed=%d]",
			elapsed.Seconds(), stats.processed)
	}
	if !r.IsInitialized() {
	}
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Store) enqueueRaftUpdateCheck(groupID multiraftbase.GroupID) {
	s.scheduler.EnqueueRaftReady(groupID)
}

func (s *Store) processRequestQueue(ctx context.Context, id multiraftbase.GroupID) {
	helper.Println(20, "Enter processRequestQueue(). id:", id)
	value, ok := s.replicaQueues.Load(id)
	if !ok {
		helper.Println(5, "Cannot load replicaQueues. id:", id)
		return
	}
	q, ok := value.(*raftRequestQueue)
	if !ok {
		helper.Println(5, "Cannot convert to raftRequestQueue type")
		return
	}
	q.Lock()
	infos := q.infos
	q.infos = nil
	q.idxMap = nil
	q.Unlock()

	for _, info := range infos {
		if pErr := s.processRaftRequest(info.respStream.Context(), info.req, IncomingSnapshot{}); pErr != nil {
			// If we're unable to process the request, clear the request queue. This
			// only happens if we couldn't create the replica because the request was
			// targeted to a removed range. This is also racy and could cause us to
			// drop messages to the deleted range occasionally (#18355), but raft
			// will just retry.
			helper.Println(5, "Failed to call processRaftRequest, err:", pErr)
			q.Lock()
			if len(q.infos) == 0 {
				s.replicaQueues.Delete(id)
			}
			q.Unlock()
			if err := info.respStream.Send(newRaftMessageResponse(info.req, pErr)); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				helper.Printf(5, "error sending error: %s", err)
			}
		}
	}
}

func (s *Store) ExistCheck(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (exist bool, pErr *multiraftbase.Error) {
	ctx = s.AnnotateCtx(ctx)

	// Add the command to the range for execution; exit retry loop on success.
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return false, multiraftbase.NewError(err)
		}

		// Get range and add command to the range for execution.
		repl, err := s.GetReplica(ba.GroupID)
		if err != nil {
			return false, multiraftbase.NewError(err)
		}
		if !repl.IsInitialized() {
			repl.mu.RLock()
			repl.mu.RUnlock()

			// If we have an uninitialized copy of the range, then we are
			// probably a valid member of the range, we're just in the
			// process of getting our snapshot. If we returned
			// RangeNotFoundError, the client would invalidate its cache,
			// but we can be smarter: the replica that caused our
			// uninitialized replica to be created is most likely the
			// leader.
			err = errors.New("Replica is not initialized!")
			return false, multiraftbase.NewError(err)
		}

		exist, pErr := repl.ExistCheck(ctx, ba)
		return exist, pErr
	}
	return
}

func (s *Store) Send(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {
	// repl.Send()
	// Attach any log tags from the store to the context (which normally
	// comes from gRPC).
	ctx = s.AnnotateCtx(ctx)

	// Add the command to the range for execution; exit retry loop on success.
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return nil, multiraftbase.NewError(err)
		}

		// Get range and add command to the range for execution.
		repl, err := s.GetReplica(ba.GroupID)
		if err != nil {
			return nil, multiraftbase.NewError(err)
		}
		if !repl.IsInitialized() {
			repl.mu.RLock()
			repl.mu.RUnlock()

			// If we have an uninitialized copy of the range, then we are
			// probably a valid member of the range, we're just in the
			// process of getting our snapshot. If we returned
			// RangeNotFoundError, the client would invalidate its cache,
			// but we can be smarter: the replica that caused our
			// uninitialized replica to be created is most likely the
			// leader.
			err = errors.New("Replica is not initialized!")
			return nil, multiraftbase.NewError(err)
		}

		br, pErr = repl.Send(ctx, ba)
		if pErr == nil {
			return br, nil
		}

		// Handle push txn failures and write intent conflicts locally and
		// retry. Other errors are returned to caller.
		switch pErr.GetDetailType().(type) {
		case *multiraftbase.RaftGroupDeletedError:
		case *multiraftbase.GroupNotFoundError:
		}

		if pErr != nil {
			return nil, pErr
		}
	}
	return
}

func (s *Store) processTick(ctx context.Context, id multiraftbase.GroupID) bool {
	value, ok := s.mu.replicas.Load(id)
	if !ok {
		return false
	}

	//start := timeutil.Now()
	r, ok := value.(*Replica)
	if !ok {
		return false
	}
	exists, err := r.tick()
	if err != nil {
		helper.Println(5, err)
	}
	return exists // ready
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	s.cfg.Transport.Listen(s)
	s.processRaft(ctx)

	s.scanner.Start(s.stopper)
	return nil
}

func (s *Store) processRaft(ctx context.Context) {

	s.scheduler.Start(ctx, s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	s.stopper.RunWorker(ctx, s.scheduler.Wait)

	s.stopper.RunWorker(ctx, s.raftTickLoop)
	s.stopper.RunWorker(ctx, s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.Stop()
	}))
}

func (s *Store) raftTickLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()

	var groupIDs []multiraftbase.GroupID

	for {
		select {
		case <-ticker.C:
			groupIDs = groupIDs[:0]

			s.mu.replicas.Range(func(k, v interface{}) bool {
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
				replica, ok := v.(*Replica)
				if !ok {
					return false
				}
				if !replica.maybeTickQuiesced() {
					val, ok := k.(multiraftbase.GroupID)
					if !ok {
						return false
					}
					if replica.mu.destroyed == nil {
						groupIDs = append(groupIDs, val)
					} else {
						// TODO: queue the replica to delete
					}
				}
				return true
			})
			s.scheduler.EnqueueRaftTick(groupIDs...)

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
	s.coalescedMu.heartbeats = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
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
		helper.Fatalf("cannot coalesce both heartbeats and responses")
	}

	chReq := &multiraftbase.RaftMessageRequest{
		GroupID: "",
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
			if value, ok := s.mu.replicas.Load(beat.GroupID); ok {
				replica, ok := value.(*Replica)
				if !ok {
					// TODO:
					return 0
				}
				replica.addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if value, ok := s.mu.replicas.Load(resp.GroupID); ok {
				replica, ok := value.(*Replica)
				if !ok {
					// TODO:
					return 0
				}
				replica.addUnreachableRemoteReplica(resp.ToReplicaID)
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
	helper.Printf(20, "HandleRaftRequest handle %d req", len(req.Heartbeats)+len(req.HeartbeatResps))
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		if req.GroupID != "" {
			helper.Fatalf("coalesced heartbeats must have groupID == 0")
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

	helper.Printf(20, "uncoalescing %d beats of type %v: %+v", len(beats), msgT, beats)

	beatReqs := make([]multiraftbase.RaftMessageRequest, len(beats))
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:    msgT,
			From:    uint64(beat.FromReplicaID),
			To:      uint64(beat.ToReplicaID),
			Term:    beat.Term,
			Commit:  beat.Commit,
			Context: beat.Context,
		}
		beatReqs[i] = multiraftbase.RaftMessageRequest{
			GroupID: beat.GroupID,
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

		helper.Printf(20, "uncoalesced beat: %+v", beatReqs[i])

		if err := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream); err != nil {
			helper.Printf(5, "could not handle uncoalesced heartbeat %s", err)
		}
	}
}

var errRetry = errors.New("retry: orphaned replica")

// getOrCreateReplica returns a replica for the given GroupID, creating an
// uninitialized replica if necessary. The caller must not hold the store's
// lock. The returned replica has Replica.raftMu locked and it is the caller's
// responsibility to unlock it.
func (s *Store) getOrCreateReplica(
	ctx context.Context,
	groupID multiraftbase.GroupID,
	replicaID multiraftbase.ReplicaID,
	creatingReplica *multiraftbase.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	for {
		r, created, err := s.tryGetOrCreateReplica(
			ctx,
			groupID,
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

// addReplicaToRangeMapLocked adds the replica to the replicas map.
// addReplicaToRangeMapLocked requires that the store lock is held.
func (s *Store) addReplicaToGroupMapLocked(repl *Replica) error {
	if _, loaded := s.mu.replicas.LoadOrStore(repl.GroupID, repl); loaded {
		return errors.New("replica already exists")
	}
	return nil
}

// tryGetOrCreateReplica performs a single attempt at trying to lookup or
// create a replica. It will fail with errRetry if it finds a Replica that has
// been destroyed (and is no longer in Store.mu.replicas) or if during creation
// another goroutine gets there first. In either case, a subsequent call to
// tryGetOrCreateReplica will likely succeed, hence the loop in
// getOrCreateReplica.
func (s *Store) tryGetOrCreateReplica(
	ctx context.Context,
	groupID multiraftbase.GroupID,
	replicaID multiraftbase.ReplicaID,
	creatingReplica *multiraftbase.ReplicaDescriptor,
) (_ *Replica, created bool, _ error) {
	// The common case: look up an existing (initialized) replica.
	if value, ok := s.mu.replicas.Load(groupID); ok {
		helper.Printf(20, "Load an exist replica.")
		repl, ok := value.(*Replica)
		if !ok {
			return nil, false, multiraftbase.NewReplicaTooOldError(creatingReplica.ReplicaID)
		}

		if creatingReplica != nil {
			// Drop messages that come from a node that we believe was once a member of
			// the group but has been removed.
			desc := repl.Desc()
			_, found := desc.GetReplicaDescriptorByID(creatingReplica.ReplicaID)
			// It's not a current member of the group. Is it from the past?
			if !found && creatingReplica.ReplicaID < desc.NextReplicaID {
				return nil, false, multiraftbase.NewReplicaTooOldError(creatingReplica.ReplicaID)
			}
		}

		repl.raftMu.Lock()
		repl.mu.Lock()
		if err := repl.setReplicaIDRaftMuLockedMuLocked(replicaID); err != nil {
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			return nil, false, err
		}
		repl.mu.Unlock()
		return repl, false, nil
	}

	return nil, false, errors.New("Not found replica")
	/*
		helper.Println(10, "Create a replica for group ", groupID)
		// Create a new replica and lock it for raft processing.
		repl := newReplica(groupID, s)
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
		// Add the range to range map, but not replicasByKey since the range's start
		// key is unknown. The range will be added to replicasByKey later when a
		// snapshot is applied. After unlocking Store.mu above, another goroutine
		// might have snuck in and created the replica, so we retry on error.
		if err := s.addReplicaToGroupMapLocked(repl); err != nil {
			repl.mu.Unlock()
			s.mu.Unlock()
			repl.raftMu.Unlock()
			return nil, false, errRetry
		}
		s.mu.uninitReplicas[repl.GroupID] = repl
		s.mu.Unlock()

		desc := &multiraftbase.GroupDescriptor{
			GroupID: groupID,
			// TODO(bdarnell): other fields are unknown; need to populate them from
			// snapshot.
		}
		if err := repl.initRaftMuLockedReplicaMuLocked(desc, replicaID); err != nil {
			// Mark the replica as destroyed and remove it from the replicas maps to
			// ensure nobody tries to use it
			repl.mu.destroyed = errors.New("failed to initialize")
			repl.mu.Unlock()
			s.mu.Lock()
			s.mu.replicas.Delete(groupID)
			delete(s.mu.uninitReplicas, groupID)
			s.replicaQueues.Delete(groupID)
			s.mu.Unlock()
			repl.raftMu.Unlock()
			helper.Printf(0, "Error initRaftMuLockedReplicaMuLocked(), err:", err)
			return nil, false, err
		}
		repl.mu.Unlock()
		return repl, true, nil
	*/
}

func (s *Store) processRaftRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, inSnap IncomingSnapshot,
) (pErr *multiraftbase.Error) {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.GroupID,
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
			helper.Fatalf("unexpected quiesce: %+v", req)
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			if r.quiesce() {
				return
			}
		}

		helper.Printf(5, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// We're processing a message from another replica which means that the
		// other replica is not quiesced, so we don't need to wake the leader.
		r.unquiesceLocked()
		if req.Message.Type == raftpb.MsgApp {
			r.setEstimatedCommitIndexLocked(req.Message.Commit)
		}
		helper.Println(20, r.GroupID, "received message:", "To:", req.Message.To,
			"From:", req.Message.From,
			"Type:", req.Message.Type,
			"Term:", req.Message.Term,
			"Index:", req.Message.Index,
			"LogTerm:", req.Message.LogTerm,
			"Context:", req.Message.Context,
		)
		return false, /* !unquiesceAndWakeLeader */
			raftGroup.Step(req.Message)
	}); err != nil {
		return multiraftbase.NewError(err)
	}

	if _, expl, err := r.handleRaftReadyRaftMuLocked(inSnap); err != nil {
		// Mimic the behavior in processRaft.
		helper.Fatalf("%v: %s", expl, err) // TODO(bdarnell)
	}
	return nil
}

// HandleRaftUncoalescedRequest dispatches a raft message to the appropriate
// Replica. It requires that s.mu is not held.
func (s *Store) HandleRaftUncoalescedRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, respStream RaftMessageResponseStream,
) *multiraftbase.Error {

	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		helper.Fatalf("HandleRaftUncoalescedRequest cannot be given coalesced heartbeats or heartbeat responses, received %s", req)
	}
	// HandleRaftRequest is called on locally uncoalesced heartbeats (which are
	// not sent over the network if the environment variable is set) so do not
	// count them.

	if respStream == nil {
		helper.Printf(5, " call processRaftRequest")
		return s.processRaftRequest(ctx, req, IncomingSnapshot{})
	}
	helper.Printf(20, "HandleRaftUncoalescedRequest() groupID:", req.GroupID)
	value, ok := s.replicaQueues.Load(req.GroupID)
	if !ok {
		value, _ = s.replicaQueues.LoadOrStore(req.GroupID, &raftRequestQueue{})
	}
	q, ok := value.(*raftRequestQueue)
	if !ok {
		return nil
	}
	q.Lock()
	if q.idxMap == nil {
		q.idxMap = make(map[string]int)
	}
	idx := fmt.Sprintf("%d.%d", req.Message.Term, req.Message.Index)
	seq, ok := q.idxMap[idx]
	if ok {
		q.infos[seq] = raftRequestInfo{
			req:        req,
			respStream: respStream,
		}
	} else {
		if len(q.infos) >= 100 {
			q.Unlock()
			// TODO(peter): Return an error indicating the request was dropped. Note
			// that dropping the request is safe. Raft will retry.
			helper.Fatalf("drop message because of queue too full")
			return nil
		}
		q.infos = append(q.infos, raftRequestInfo{
			req:        req,
			respStream: respStream,
		})
		q.idxMap[idx] = len(q.infos) - 1
	}

	q.Unlock()
	s.scheduler.EnqueueRaftRequest(req.GroupID)
	return nil
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(groupID multiraftbase.GroupID) (*Replica, error) {
	if value, ok := s.mu.replicas.Load(groupID); ok {
		replica, ok := value.(*Replica)
		if !ok {
			return nil, errors.New("Error converting type!")
		}
		return replica, nil
	}
	return nil, multiraftbase.NewGroupNotFoundError(groupID)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Store) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// HandleRaftResponse implements the RaftMessageHandler interface.
// It requires that s.mu is not held.
func (s *Store) HandleRaftResponse(ctx context.Context, resp *multiraftbase.RaftMessageResponse) error {
	ctx = s.AnnotateCtx(ctx)
	switch val := resp.Union.GetValue().(type) {
	case *multiraftbase.Error:
		switch tErr := val.GetDetailType().(type) {
		case *multiraftbase.ReplicaTooOldError:
			repl, err := s.GetReplica(resp.GroupID)
			if err != nil {
				// RangeNotFoundErrors are expected here; nothing else is.
				if _, ok := err.(*multiraftbase.GroupNotFoundError); !ok {
					helper.Println(5, err)
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
		default:
			helper.Printf(5, "got error from r%d, replica %s: %s",
				resp.GroupID, resp.FromReplica, val)
		}

	default:
		helper.Printf(5, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
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
		PreVote:     false,
		CheckQuorum: true,

		// MaxSizePerMsg controls how many Raft log entries the leader will send to
		// followers in a single MsgApp.
		MaxSizePerMsg: math.MaxUint64,
		// MaxInflightMsgs controls how many "inflight" messages Raft will send to
		// a follower without hearing a response. The total number of Raft log
		// entries is a combination of this setting and MaxSizePerMsg. The current
		// settings provide for up to 1 MB of raft log to be sent without
		// acknowledgement. With an average entry size of 1 KB that translates to
		// ~1024 commands that might be executed in the handling of a single
		// raft.Ready operation.
		MaxInflightMsgs: 64,
		ReadOnlyOption:  raft.ReadOnlyLeaseBased,
	}
}

// StoreID accessor.
func (s *Store) StoreID() multiraftbase.StoreID { return s.Ident.StoreID }

func (s *Store) NodeID() multiraftbase.NodeID { return s.nodeDesc.NodeID }

// A storeReplicaVisitor calls a visitor function for each of a store's
// initialized Replicas (in unspecified order).
type storeReplicaVisitor struct {
	store   *Store
	repls   []*Replica // Replicas to be visited.
	visited int        // Number of visited ranges, -1 before first call to Visit()
}

// Len implements shuffle.Interface.
func (rs storeReplicaVisitor) Len() int { return len(rs.repls) }

// Swap implements shuffle.Interface.
func (rs storeReplicaVisitor) Swap(i, j int) { rs.repls[i], rs.repls[j] = rs.repls[j], rs.repls[i] }

// newStoreReplicaVisitor constructs a storeReplicaVisitor.
func newStoreReplicaVisitor(store *Store) *storeReplicaVisitor {
	return &storeReplicaVisitor{
		store:   store,
		visited: -1,
	}
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeReplicaVisitor) Visit(visitor func(*Replica) bool) {
	// Copy the range IDs to a slice so that we iterate over some (possibly
	// stale) view of all Replicas without holding the Store lock. In particular,
	// no locks are acquired during the copy process.
	rs.repls = nil
	rs.store.mu.replicas.Range(func(key, value interface{}) bool {
		r, ok := value.(*Replica)
		if !ok {
			return false
		}
		rs.repls = append(rs.repls, r)
		return true
	})

	// The Replicas are already in "unspecified order" due to map iteration,
	// but we want to make sure it's completely random to prevent issues in
	// tests where stores are scanning replicas in lock-step and one store is
	// winning the race and getting a first crack at processing the replicas on
	// its queues.
	//
	// TODO(peter): Re-evaluate whether this is necessary after we allow
	// rebalancing away from the leaseholder. See TestRebalance_3To5Small.
	shuffle.Shuffle(rs)

	rs.visited = 0
	for _, repl := range rs.repls {
		// TODO(tschottdorf): let the visitor figure out if something's been
		// destroyed once we return errors from mutexes (#9190). After all, it
		// can still happen with this code.
		rs.visited++
		repl.mu.RLock()
		destroyed := repl.mu.destroyed
		initialized := repl.isInitializedRLocked()
		repl.mu.RUnlock()
		if initialized && destroyed == nil && !visitor(repl) {
			break
		}
	}
	rs.visited = 0
}

// ReplicaCount returns the number of replicas contained by this store. This
// method is O(n) in the number of replicas and should not be called from
// performance critical code.
func (s *Store) ReplicaCount() int {
	var count int
	s.mu.replicas.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// EstimatedCount returns an estimated count of the underlying store's
// replicas.
//
// TODO(tschottdorf): this method has highly doubtful semantics.
func (rs *storeReplicaVisitor) EstimatedCount() int {
	if rs.visited <= 0 {
		return rs.store.ReplicaCount()
	}
	return len(rs.repls) - rs.visited
}

// NewStore returns a new instance of a store.
func NewStore(cfg StoreConfig, eng engine.Engine, nodeDesc *multiraftbase.NodeDescriptor) *Store {
	cfg.SetDefaults()

	s := &Store{
		cfg: cfg,
	}

	s.raftEntryCache = newRaftEntryCache(16 * 1024 * 1024)
	s.scheduler = newRaftScheduler(s, storeSchedulerConcurrency)
	s.nodeDesc = nodeDesc
	s.sysEng = eng
	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
	s.coalescedMu.Unlock()
	s.Db = client.NewDB(s)
	s.draining.Store(false)

	//if s.cfg.Gossip != nil {
	// Add range scanner and configure with queues.
	cfg.ScanInterval = 10 * time.Minute
	cfg.ScanMaxIdleTime = 200 * time.Millisecond
	s.scanner = newReplicaScanner(
		s.cfg.AmbientCtx, cfg.ScanInterval, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
	)
	s.raftLogQueue = newRaftLogQueue(s, s.Db)
	s.raftSnapshotQueue = newRaftSnapshotQueue(s)
	s.transferLeaderQueue = newTransferLeaderQueue(s, s.Db)
	s.scanner.AddQueues(s.raftSnapshotQueue, s.raftLogQueue, s.transferLeaderQueue)
	//}

	s.mu.Lock()
	s.mu.uninitReplicas = map[multiraftbase.GroupID]*Replica{}
	s.mu.Unlock()
	return s
}

// addReplicaInternalLocked adds the replica to the replicas map and the
// replicasByKey btree. Returns an error if a replica with
// the same Range ID or a KeyRange that overlaps has already been added to
// this store. addReplicaInternalLocked requires that the store lock is held.
func (s *Store) addReplicaInternalLocked(repl *Replica) error {
	if !repl.IsInitialized() {
		return errors.Errorf("attempted to add uninitialized range %s", repl)
	}

	// TODO(spencer): will need to determine which range is
	// newer, and keep that one.
	if err := s.addReplicaToGroupMapLocked(repl); err != nil {
		return err
	}

	return nil
}

func (s *Store) isExistReplicaWorkDir(groupID multiraftbase.GroupID) bool {
	dir, err := helper.GetDataDir(s.cfg.BaseDir, uint64(s.cfg.NodeID), false, false)
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	_, err = os.Stat(dir)
	if err != nil && !os.IsNotExist(err) {
		helper.Check(err)
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (s *Store) getReplicaWorkDir(groupID multiraftbase.GroupID) (string, error) {
	dir, err := helper.GetDataDir(s.cfg.BaseDir, uint64(s.cfg.NodeID), false, true)
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	return dir + "/" + string(groupID), nil
}

func (s *Store) getReplicaSnapDir(groupID multiraftbase.GroupID) (string, error) {
	dir, err := helper.GetDataDir(s.cfg.BaseDir, uint64(s.cfg.NodeID), false, true)
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	return dir + "/" + string(groupID) + ".snap", nil
}

func (s *Store) GetGroupStore(groupId multiraftbase.GroupID) (engine.Engine, error) {
	value, exist := s.engines.Load(groupId)
	if exist {
		eng, ok := value.(engine.Engine)
		if ok {
			return eng, nil
		}
	}

	dir, err := s.getReplicaWorkDir(groupId)
	if err != nil {
		helper.Fatalln("Can not get replica data dir. group:", string(groupId))
	}
	helper.Println(10, "Create pg group data dir:", dir)
	opt := engine.KVOpt{Dir: dir}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		helper.Println(5, "Can not new a badger db.")
		return nil, err
	}
	s.engines.Store(groupId, eng)
	return eng, nil
}

func (s *Store) BootstrapGroup(join bool, group *multiraftbase.GroupDescriptor) error {
	desc := *group
	if err := desc.Validate(); err != nil {
		helper.Println(5, "BootstrapGroup quit 0 ", err.Error())
		return err
	}
	/*
		batch := s.Engine().NewBatch()
		defer batch.Close()
		encoded, _ := desc.Marshal()
		batch.Put(keys.GroupDescriptorKey(desc.GroupID), encoded)
		// Now add all passed-in default entries.
		for _, kv := range initialValues {
			// Initialize the checksums.
			if err := batch.Put(kv.Key, kv.Value.GetRawBytes()); err != nil {
				return err
			}
		}
		err := batch.Commit()
		if err != nil {
			return err
		}
	*/

	_, err := s.GetGroupStore(desc.GroupID)
	if err != nil {
		helper.Check(err)
	}
	_, found := group.GetReplicaDescriptor(s.nodeDesc.NodeID)
	if !found {
		helper.Println(5, "BootstrapGroup quit 1 ")
		return errors.New(fmt.Sprintf("send to wrong node %s", s.nodeDesc.NodeID))
	}
	r, err := NewReplica(&desc, s, 0)
	if err != nil {
		helper.Println(5, "BootstrapGroup quit 2 ", err.Error())
		return err
	}
	s.mu.Lock()
	err = s.addReplicaInternalLocked(r)
	s.mu.Unlock()
	if err != nil {
		helper.Println(5, "BootstrapGroup quit 3 ", err.Error())
		return err
	}
	if _, ok := desc.GetReplicaDescriptor(s.NodeID()); !ok {
		// We are no longer a member of the range, but we didn't GC the replica
		// before shutting down. Add the replica to the GC queue.
	}

	r.raftMu.Lock()
	r.mu.Lock()
	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			log.NewRaftLogger(string(r.GroupID), helper.Logger),
		), nil)
		if err != nil {
			r.mu.Unlock()
			r.raftMu.Unlock()
			helper.Println(5, "BootstrapGroup quit 4 ", err.Error())
			return err
		}
		r.mu.internalRaftGroup = raftGroup
	}
	r.mu.Unlock()
	r.raftMu.Unlock()
	helper.Println(5, "BootstrapGroup quit 5, groupId:", group.GroupID)
	return nil
}

func (s *Store) GetGroupIdsByLeader() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vector := make([]string, 0)
	s.mu.replicas.Range(func(key, value interface{}) bool {
		replica, _ := value.(*Replica)
		//		helper.Println(5, "check one replica*******************:", replica)
		if replica.amLeader() {
			vector = append(vector, string(replica.GroupID))
			//			helper.Println(5, "find one leader*******************:", string(replica.GroupID))
		}
		return true
	})
	return vector, nil
}

// IsDraining accessor.
func (s *Store) IsDraining() bool {
	return s.draining.Load().(bool)
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *multiraftbase.SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	if s.IsDraining() {
		return stream.Send(&multiraftbase.SnapshotResponse{
			Status:  multiraftbase.SnapshotResponse_DECLINED,
			Message: storeDrainingMsg,
		})
	}
	ctx := s.AnnotateCtx(stream.Context())
	sendSnapError := func(err error) error {
		return stream.Send(&multiraftbase.SnapshotResponse{
			Status:  multiraftbase.SnapshotResponse_ERROR,
			Message: err.Error(),
		})
	}

	if err := stream.Send(&multiraftbase.SnapshotResponse{Status: multiraftbase.SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}

	helper.Printf(5, "accepted snapshot reservation for r%s", header.State.Desc.GroupID)

	var batches [][]byte
	var logEntries [][]byte
	//eng := s.LoadGroupEngine(header.RaftMessageRequest.GroupID)
	snapDir, err := s.getReplicaSnapDir(header.RaftMessageRequest.GroupID)
	if err != nil {
		helper.Panicln(5, "Error get ReplicaSnapDir path, err:", err)
	}
	_, err = os.Stat(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(snapDir, os.ModePerm)
			if err != nil {
				helper.Panicln(0, "Cannot create snap dir! err:", err)
			}
		} else {
			helper.Panicln(5, "Error stat ReplicaSnapDir, err:", err)
		}
	}
	helper.Printf(5, "start receive kvs r%s", header.State.Desc.GroupID)
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

		//err = stripeWrite(eng, req.Key, req.Val, 0, uint64(len(req.Val)))
		//if err != nil {
		//	helper.Panicln(5, "Error putting data to db, err:", err)
		//}
		if req.Key != nil {
			filePath := snapDir + "/" + string(req.Key)
			fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			if err != nil {
				helper.Panicln(5, "Error open new badger file, err:", err, string(req.Key))
			}
			_, err = fd.Write(req.Val)
			if err != nil {
				helper.Panicln(5, "Error write badger file, err:", err, string(req.Key))
			}
			fd.Close()
		}

		if req.LogEntries != nil {
			helper.Printf(5, "receive log entrys r%s", header.State.Desc.GroupID)
			logEntries = append(logEntries, req.LogEntries...)
		}

		if req.Final {
			s.ShutdownGroupEngine(header.RaftMessageRequest.GroupID)
			replicaDir, err := s.getReplicaWorkDir(header.RaftMessageRequest.GroupID)
			if err != nil {
				helper.Panicln(5, "Error get ReplicaSnapDir path, err:", err)
			}
			err = os.RemoveAll(replicaDir)
			if err != nil {
				helper.Panicln(5, "Error RemoveAll replicaDir, err:", err, replicaDir)
			}
			os.Rename(snapDir, replicaDir)
			if err != nil {
				helper.Panicln(5, "Error Rename snapDir, err:", err, snapDir, replicaDir)
			}
			opt := engine.KVOpt{Dir: replicaDir}
			eng, err := engine.NewBadgerDB(&opt)
			if err != nil {
				helper.Println(5, "Can not new a badger db.")
				return err
			}
			s.engines.Store(header.RaftMessageRequest.GroupID, eng)

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
			if err := s.processRaftRequest(ctx, &header.RaftMessageRequest, inSnap); err != nil {
				return sendSnapError(errors.Wrap(err.GoError(), "failed to apply snapshot"))
			}

			return stream.Send(&multiraftbase.SnapshotResponse{Status: multiraftbase.SnapshotResponse_APPLIED})
		}
	}
}

// OutgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type OutgoingSnapshotStream interface {
	Send(*multiraftbase.SnapshotRequest) error
	Recv() (*multiraftbase.SnapshotResponse, error)
}

func snapshotRateLimit(priority multiraftbase.SnapshotRequest_Priority,
) (rate.Limit, error) {
	switch priority {
	case multiraftbase.SnapshotRequest_RECOVERY:
		return rate.Limit(8 << 20), nil
	case multiraftbase.SnapshotRequest_REBALANCE:
		return rate.Limit(2 << 20), nil
	default:
		return 0, errors.Errorf("unknown snapshot priority: %s", priority)
	}
}

func sendBatch(stream OutgoingSnapshotStream, key, val []byte, index int32) error {
	return stream.Send(&multiraftbase.SnapshotRequest{Key: key, Val: val, Index: index})
}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	stream OutgoingSnapshotStream,
	header multiraftbase.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	sent func(),
) error {
	//start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&multiraftbase.SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server.
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	switch resp.Status {
	case multiraftbase.SnapshotResponse_DECLINED:
		if header.CanDecline {
			declinedMsg := "reservation rejected"
			if len(resp.Message) > 0 {
				declinedMsg = resp.Message
			}
			return errors.Errorf("%s: remote declined snapshot: %s", to, declinedMsg)
		}
		return errors.Errorf("%s: programming error: remote declined required snapshot: %s",
			to, resp.Message)
	case multiraftbase.SnapshotResponse_ERROR:
		return errors.Errorf("%s: remote couldn't accept snapshot with error: %s",
			to, resp.Message)
	case multiraftbase.SnapshotResponse_ACCEPTED:
		// This is the response we're expecting. Continue with snapshot sending.
	default:
		return errors.Errorf("%s: server sent an invalid status during negotiation: %s",
			to, resp.Status)
	}
	// The size of batches to send. This is the granularity of rate limiting.
	//const batchSize = 256 << 10 // 256 KB
	//targetRate, err := snapshotRateLimit(header.Priority)
	//if err != nil {
	//return errors.Wrapf(err, "%s", to)
	//}

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	//limiter := rate.NewLimiter(targetRate/batchSize, 1 /* burst size */)

	// Determine the unreplicated key prefix so we can drop any
	// unreplicated keys from the snapshot.
	//unreplicatedPrefix := keys.MakeGroupIDUnreplicatedPrefix(header.State.Desc.GroupID)
	//var alloc bufalloc.ByteAllocator
	//n := 0

	db, ok := snap.Engine.(*engine.BadgerDB)
	if !ok {
		return errors.Wrapf(err, "Error assertion snap engine when snapshot.")
	}
	fds, err := db.QuickBackupPrepare()
	if err != nil {
		return errors.Wrapf(err, "Error Quick Backup Prepare when snapshot.")
	}
	helper.Printf(5, "QuickBackupPrepare returned, with opened fds %d", len(fds))
	for _, fd := range fds {
		path := fd.Name()
		key := filepath.Base(path)
		if key == badger.ManifestBackupFilename {
			key = badger.ManifestFilename
		}
		state, err := fd.Stat()
		if err != nil {
			return errors.Wrapf(err, "file stat failed when snapshot.")
		}
		length := state.Size()
		loop := int(length / SNAP_CHUNK_SIZE)
		buf := make([]byte, SNAP_CHUNK_SIZE)
		helper.Printf(5, "will send bach, key:%s, length:%d, looptime:%d", key, length, loop)
		for i := 0; i <= loop; i++ {
			offset := SNAP_CHUNK_SIZE * i
			if length >= SNAP_CHUNK_SIZE {
				buf = buf[:]
			} else {
				buf = buf[:length]
			}
			n, err := fd.ReadAt(buf, int64(offset))
			if err != io.EOF && err != nil {
				helper.Printf(5, "enter error case, key:%s, bytes:%d, offset:%d, err:%v", key, n, offset, err)
				return errors.Wrapf(err, "read file %s failed at offset %d when snapshot.", key, i*SNAP_CHUNK_SIZE)
			}
			helper.Printf(5, "real send bach, key:%s, bytes:%d, offset:%d", key, n, offset)
			if n <= 0 {
				break
			}
			if err := sendBatch(stream, []byte(key), buf, int32(i)); err != nil {
				return err
			}
			if err == io.EOF {
				break
			}
			length = length - SNAP_CHUNK_SIZE
		}

	}
	helper.Println(5, "exec QuickBackupDone")
	err = db.QuickBackupDone()
	if err != nil {
		errors.Wrapf(err, "Error Quick Backup Done when snapshot.")
	}
	helper.Println(5, "exec QuickBackupDone return")
	//for snap.Iter.Rewind(); ; snap.Iter.Next() {
	//	if ok := snap.Iter.Valid(); !ok {
	//		break
	//	}
	//	item := snap.Iter.Item()
	//	key := item.Key()
	//	if !bytes.HasPrefix(key, []byte{'\x02'}) {
	//		continue
	//	}
	//	oid := bytes.TrimPrefix(key, []byte{'\x02'})
	//	val, err := StripeRead(snap.EngineSnap, oid, 0, math.MaxUint32)
	//	if err != nil {
	//		helper.Println(5, "Error reading data when snapshot", oid, "err:", err)
	//		return errors.Wrapf(err, "Error reading data when  snapshot.")
	//	}
	//
	//	helper.Println(5, "Send snapshot key:", key)
	//	//helper.Println(5, "Send snapshot val:", val)
	//	if err := sendBatch(stream, key, val); err != nil {
	//		return err
	//	}
	//}

	firstIndex := header.State.TruncatedState.Index + 1
	endIndex := snap.RaftSnap.Metadata.Index + 1
	logEntries := make([][]byte, 0, endIndex-firstIndex)
	scanFunc := func(kv multiraftbase.KeyValue) (bool, error) {
		//bytes, err := kv.Value.GetBytes()
		bytes := kv.Value
		if err == nil {
			logEntries = append(logEntries, bytes)
		}
		return false, err
	}

	groupID := header.State.Desc.GroupID
	helper.Println(5, "exec iterateEntries")
	if err := iterateEntries(ctx, snap.EngineSnap, groupID, firstIndex, endIndex, scanFunc); err != nil {
		return err
	}

	helper.Println(5, "sendsnapshot final")
	req := &multiraftbase.SnapshotRequest{
		LogEntries: logEntries,
		Final:      true,
	}
	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(req); err != nil {
		return err
	}
	//helper.Printf(5, "streamed snapshot to %s: kv pairs: %d, log entries: %d, rate-limit: %s/sec, %0.0fms",
	//	to, n, len(logEntries), humanizeutil.IBytes(int64(targetRate)),
	//	timeutil.Since(start).Seconds()*1000)

	resp, err = stream.Recv()
	if err != nil {
		return errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		return errors.Errorf("%s: expected EOF, got resp=%v err=%v", to, unexpectedResp, err)
	}
	switch resp.Status {
	case multiraftbase.SnapshotResponse_ERROR:
		return errors.Errorf("%s: remote failed to apply snapshot for reason %s", to, resp.Message)
	case multiraftbase.SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status)
	}

	return nil
}

func (s *Store) GetMergedPGState() (*protos.MergedPGState, error) {
	return &protos.MergedPGState{}, nil
}

func (s *Store) SetMergedPGState(state protos.MergedPGState) error {
	return nil
}
