package multiraft

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/base"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/multiraft/keys"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/envutil"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"runtime"
	"sync"
	"time"
	"unsafe"
)

// Engines is a container of engines, allowing convenient closing.
type Engines map[string]engine.Engine

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"NENTROPY_SCHEDULER_CONCURRENCY", 8*runtime.NumCPU())

var enablePreVote = envutil.EnvOrDefaultBool(
	"COCKROACH_ENABLE_PREVOTE", false)

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreConfig struct {
	AmbientCtx helper.AmbientContext
	base.RaftConfig
	Transport                   *RaftTransport
	RPCContext                  *rpc.Context
	RaftHeartbeatIntervalTicks  int
	CoalescedHeartbeatsInterval time.Duration
}

type raftRequestInfo struct {
	req        *multiraftbase.RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos []raftRequestInfo
}

// A Store maintains a map of ranges by start key. A Store corresponds
// to one physical device.
type Store struct {
	Ident multiraftbase.StoreIdent
	cfg   StoreConfig
	mu    struct {
		sync.Mutex
		replicas       sync.Map //map[multiraftbase.GroupID]*Replica
		uninitReplicas map[multiraftbase.GroupID]*Replica
	}
	engines        Engines
	raftEntryCache *raftEntryCache
	started        int32
	stopper        *stop.Stopper
	nodeDesc       *multiraftbase.NodeDescriptor
	//	replicaGCQueue     *replicaGCQueue
	replicaQueues sync.Map // map[multiraftbase.GroupID]*raftRequestQueue
	//	raftRequestQueues map[multiraftbase.GroupID]*raftRequestQueue
	scheduler *raftScheduler

	coalescedMu struct {
		syncutil.Mutex
		heartbeats         map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat
		heartbeatResponses map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat
	}
}

func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()
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
		helper.Logger.Fatalf(5, "%v, %s", expl, err) // TODO(bdarnell)
	}

	elapsed := timeutil.Since(start)
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		helper.Logger.Printf(5, "handle raft ready: %.1fs [processed=%d]",
			elapsed.Seconds(), stats.processed)
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

func (s *Store) Send(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (br *multiraftbase.BatchResponse, pErr *multiraftbase.Error) {
	// repl.Send()
}

func (s *Store) processTick(ctx context.Context, id multiraftbase.GroupID) bool {
	value, ok := s.mu.replicas.Load(id)
	if !ok {
		return false
	}

	//start := timeutil.Now()
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

	s.cfg.Transport.Listen(s)
	s.processRaft(ctx)
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
				if !(*Replica)(v).maybeTickQuiesced() {
					groupIDs = append(groupIDs, multiraftbase.GroupID(k))
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
		helper.Logger.Fatalf(5, "cannot coalesce both heartbeats and responses")
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
				(*Replica)(value).addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if value, ok := s.mu.replicas.Load(int64(resp.GroupID)); ok {
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
		if req.GroupID != "" {
			helper.Logger.Fatalf(5, "coalesced heartbeats must have groupID == 0")
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

		helper.Logger.Printf(5, "uncoalesced beat: %+v", beatReqs[i])

		if err := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream); err != nil {
			helper.Logger.Printf(5, "could not handle uncoalesced heartbeat %s", err)
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
func (s *Store) addReplicaToRangeMapLocked(repl *Replica) error {
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
	if value, ok := s.mu.replicas.Load(int64(groupID)); ok {
		repl := (*Replica)(value)
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
	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
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
		s.mu.replicas.Delete(int64(groupID))
		delete(s.mu.uninitReplicas, groupID)
		s.replicaQueues.Delete(int64(groupID))
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
			helper.Logger.Fatalf(5, "unexpected quiesce: %+v", req)
		}
		status := r.RaftStatus()
		if status != nil && status.Term == req.Message.Term && status.Commit == req.Message.Commit {
			if r.quiesce() {
				return
			}
		}

		helper.Logger.Printf(5, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)

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
		helper.Logger.Fatalf(5, "%v: %s", expl, err) // TODO(bdarnell)
	}
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

	value, ok := s.replicaQueues.Load(int64(req.GroupID))
	if !ok {
		value, _ = s.replicaQueues.LoadOrStore(int64(req.GroupID), unsafe.Pointer(&raftRequestQueue{}))
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	if len(q.infos) >= 100 {
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

	s.scheduler.EnqueueRaftRequest(req.GroupID)
	return nil
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
func (s *Store) GetReplica(groupID multiraftbase.GroupID) (*Replica, error) {
	if value, ok := s.mu.replicas.Load(int64(groupID)); ok {
		return (*Replica)(value), nil
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
		switch tErr := val.GetDetail().(type) {
		case *multiraftbase.ReplicaTooOldError:
			repl, err := s.GetReplica(resp.GroupID)
			if err != nil {
				// RangeNotFoundErrors are expected here; nothing else is.
				if _, ok := err.(*multiraftbase.GroupNotFoundError); !ok {
					helper.Logger.Println(5, err)
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
			helper.Logger.Printf(5, "got error from r%d, replica %s: %s",
				resp.GroupID, resp.FromReplica, val)
		}

	default:
		helper.Logger.Printf(5, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
	}
	return nil
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg StoreConfig, logger log.RaftLogger,
) *raft.Config {
	return &raft.Config{
		ID:            id,
		Applied:       appliedIndex,
		ElectionTick:  storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick: storeCfg.RaftHeartbeatIntervalTicks,
		Storage:       strg,
		Logger:        &logger,

		// TODO(bdarnell): PreVote and CheckQuorum are two ways of
		// achieving the same thing. PreVote is more compatible with
		// quiesced ranges, so we want to switch to it once we've worked
		// out the bugs.
		PreVote:     true,
		CheckQuorum: false,

		// MaxSizePerMsg controls how many Raft log entries the leader will send to
		// followers in a single MsgApp.
		MaxSizePerMsg: uint64(16 * 1024),
		// MaxInflightMsgs controls how many "inflight" messages Raft will send to
		// a follower without hearing a response. The total number of Raft log
		// entries is a combination of this setting and MaxSizePerMsg. The current
		// settings provide for up to 1 MB of raft log to be sent without
		// acknowledgement. With an average entry size of 1 KB that translates to
		// ~1024 commands that might be executed in the handling of a single
		// raft.Ready operation.
		MaxInflightMsgs: 64,
	}
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *multiraftbase.SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	return nil
}

// Engine accessor.
func (s *Store) Engine() engine.Engine { return s.engines["system"] }

// StoreID accessor.
func (s *Store) StoreID() multiraftbase.StoreID { return s.Ident.StoreID }

func (s *Store) NodeID() multiraftbase.NodeID { return s.nodeDesc.NodeID }

// NewStore returns a new instance of a store.
func NewStore(cfg StoreConfig, eng engine.Engine, nodeDesc *multiraftbase.NodeDescriptor) *Store {
	// TODO(tschottdorf): find better place to set these defaults.
	cfg.SetDefaults()

	s := &Store{
		cfg: cfg,
	}

	s.raftEntryCache = newRaftEntryCache(16 * 1024 * 1024)
	s.scheduler = newRaftScheduler(s, storeSchedulerConcurrency)
	s.nodeDesc = nodeDesc
	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[multiraftbase.StoreIdent][]multiraftbase.RaftHeartbeat{}
	s.coalescedMu.Unlock()
	/*
		if s.cfg.Gossip != nil {
			// Add range scanner and configure with queues.
			s.scanner = newReplicaScanner(
				s.cfg.AmbientCtx, cfg.ScanInterval, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
			)
			s.replicateQueue = newReplicateQueue(s, s.cfg.Gossip, s.allocator, s.cfg.Clock)
			s.raftLogQueue = newRaftLogQueue(s, s.db, s.cfg.Gossip)
			s.scanner.AddQueues(s.replicateQueue, s.raftLogQueue)
		}
	*/
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
	if err := s.addReplicaToRangeMapLocked(repl); err != nil {
		return err
	}

	return nil
}

func (s *Store) BootstrapGroup(initialValues []multiraftbase.KeyValue, group *multiraftbase.GroupDescriptor) error {
	desc := *group
	if err := desc.Validate(); err != nil {
		return err
	}
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
	replicaDesc, found := group.GetReplicaDescriptor(s.nodeDesc.NodeID)
	if !found {
		return errors.New(fmt.Sprintf("send to wrong node %s", s.nodeDesc.NodeID))
	}
	r, _, err := s.getOrCreateReplica(context.Background(), group.GroupID, replicaDesc.ReplicaID, nil)
	//r, err := NewReplica(&desc, s, 0)
	//if err != nil {
	//	return err
	//}
	//s.mu.Lock()
	//err = s.addReplicaInternalLocked(r)
	//s.mu.Unlock()
	//if err != nil {
	//	return err
	//}
	//if _, ok := desc.GetReplicaDescriptor(s.NodeID()); !ok {
	//	// We are no longer a member of the range, but we didn't GC the replica
	//	// before shutting down. Add the replica to the GC queue.
	//	if added, err := s.replicaGCQueue.Add(rep, replicaGCPriorityRemoved); err != nil {
	//		helper.Logger.Printf(5, "%s: unable to add replica to GC queue: %s", rep, err)
	//	} else if added {
	//		helper.Logger.Printf(5, "%s: added to replica GC queue", rep)
	//	}
	//}
	peers := []raft.Peer{}
	replicaDescs := group.GetReplicas()
	for _, desc := range replicaDescs {
		peers = append(peers, raft.Peer{ID: uint64(desc.ReplicaID)})
	}

	r.mu.Lock()
	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*replicaRaftStorage)(r)),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			log.RaftLogger{helper.Logger},
		), peers)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup
	}
	r.mu.Unlock()

	return nil
}
