package main

import (
	"fmt"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/journeymidnight/badger"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/keys"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/uuid"
)

// replicaRaftStorage implements the raft.Storage interface.
type replicaRaftStorage Replica

var _ raft.Storage = (*replicaRaftStorage)(nil)

// All calls to raft.RawNode require that both Replica.raftMu and
// Replica.mu are held. All of the functions exposed via the
// raft.Storage interface will in turn be called from RawNode, so none
// of these methods may acquire either lock, but they may require
// their caller to hold one or both locks (even though they do not
// follow our "Locked" naming convention). Specific locking
// requirements are noted in each method's comments.
//
// Many of the methods defined in this file are wrappers around static
// functions. This is done to facilitate their use from
// Replica.Snapshot(), where it is important that all the data that
// goes into the snapshot comes from a consistent view of the
// database, and not the replica's in-memory state or via a reference
// to Replica.store.Engine().

// InitialState implements the raft.Storage interface.
// InitialState requires that r.mu is held.
func (r *replicaRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ctx := r.AnnotateCtx(context.TODO())
	eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)
	hs, err := r.mu.stateLoader.loadHardState(ctx, eng)
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) || err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	var cs raftpb.ConfState
	for _, rep := range r.mu.state.Desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	return hs, cs, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Passing maxBytes equal to zero disables size checking. Sideloaded
// proposals count towards maxBytes with their payloads inlined.
func (r *replicaRaftStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	ctx := r.AnnotateCtx(context.TODO())
	eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)
	return entries(ctx, eng, r.GroupID, r.store.raftEntryCache,
		lo, hi, maxBytes)
}

// entries retrieves entries from the engine. To accommodate loading the term,
// `sideloaded` can be supplied as nil, in which case sideloaded entries will
// not be inlined, the raft entry cache will not be populated with *any* of the
// loaded entries, and maxBytes will not be applied to the payloads.
func entries(
	ctx context.Context,
	e engine.Reader,
	groupID multiraftbase.GroupID,
	eCache *raftEntryCache,
	lo, hi, maxBytes uint64,
) ([]raftpb.Entry, error) {
	if lo > hi {
		return nil, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}

	ents := make([]raftpb.Entry, 0, n)

	ents, size, hitIndex := eCache.getEntries(ents, groupID, lo, hi, maxBytes)
	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if uint64(len(ents)) == hi-lo || (maxBytes > 0 && size > maxBytes) {
		return ents, nil
	}
	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	// Whether we can populate the Raft entries cache. False if we found a
	// sideloaded proposal, but the caller didn't give us a sideloaded storage.
	canCache := true

	exceededMaxBytes := false

	for i := expectedIndex; i < hi; i++ {
		v, err := e.Get(keys.RaftLogKey(groupID, i))
		if err != nil && err != badger.ErrKeyNotFound {
			return nil, err
		}
		var ent raftpb.Entry
		err = ent.Unmarshal(v)
		if err != nil {
			return nil, err
		}
		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())
		ents = append(ents, ent)
		exceededMaxBytes = maxBytes > 0 && size > maxBytes
		if exceededMaxBytes {
			break
		}
	}

	// Cache the fetched entries, if we may.
	if canCache {
		eCache.addEntries(groupID, ents)
	}

	// Did the correct number of results come back? If so, we're all good.
	if uint64(len(ents)) == hi-lo {
		return ents, nil
	}

	// Did we hit the size limit? If so, return what we have.
	if exceededMaxBytes {
		return ents, nil
	}

	// Did we get any results at all? Because something went wrong.
	if len(ents) > 0 {
		// Was the lo already truncated?
		if ents[0].Index > lo {
			return nil, raft.ErrCompacted
		}

		// Was the missing index after the last index?
		lastIndex, err := loadLastIndex(ctx, e, groupID)
		if err != nil {
			return nil, err
		}
		if lastIndex <= expectedIndex {
			return nil, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, errors.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, err := loadTruncatedState(ctx, e, groupID)
	if err != nil {
		return nil, err
	}
	if ts.Index >= lo {
		// The requested lo index has already been truncated.
		return nil, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, raft.ErrUnavailable
}

// invalidLastTerm is an out-of-band value for r.mu.lastTerm that
// invalidates lastTerm caching and forces retrieval of Term(lastTerm)
// from the raftEntryCache/RocksDB.
const invalidLastTerm = 0

// Term implements the raft.Storage interface.
func (r *replicaRaftStorage) Term(i uint64) (uint64, error) {
	// TODO(nvanbenschoten): should we set r.mu.lastTerm when
	//   r.mu.lastIndex == i && r.mu.lastTerm == invalidLastTerm?
	if r.mu.lastIndex == i && r.mu.lastTerm != invalidLastTerm {
		return r.mu.lastTerm, nil
	}
	// Try to retrieve the term for the desired entry from the entry cache.
	if term, ok := r.store.raftEntryCache.getTerm(r.GroupID, i); ok {
		return term, nil
	}
	ctx := r.AnnotateCtx(context.TODO())
	eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)
	return term(ctx, eng, r.GroupID, r.store.raftEntryCache, i)
}

// raftTermLocked requires that r.mu is locked for reading.
func (r *Replica) raftTermRLocked(i uint64) (uint64, error) {
	return (*replicaRaftStorage)(r).Term(i)
}

func term(
	ctx context.Context, eng engine.Reader, groupID multiraftbase.GroupID, eCache *raftEntryCache, i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, eng, groupID, eCache, i, i+1, 0)
	if err == raft.ErrCompacted {
		ts, err := loadTruncatedState(ctx, eng, groupID)
		if err != nil {
			return 0, err
		}
		if i == ts.Index {
			return ts.Term, nil
		}
		return 0, raft.ErrCompacted
	} else if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// LastIndex implements the raft.Storage interface.
func (r *replicaRaftStorage) LastIndex() (uint64, error) {
	return r.mu.lastIndex, nil
}

// raftLastIndexLocked requires that r.mu is held.
func (r *Replica) raftLastIndexLocked() (uint64, error) {
	return (*replicaRaftStorage)(r).LastIndex()
}

// raftTruncatedStateLocked returns metadata about the log that preceded the
// first current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
// raftTruncatedStateLocked requires that r.mu is held.
func (r *Replica) raftTruncatedStateLocked(
	ctx context.Context,
) (multiraftbase.RaftTruncatedState, error) {
	if r.mu.state.TruncatedState != nil {
		return *r.mu.state.TruncatedState, nil
	}
	eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)
	ts, err := r.mu.stateLoader.loadTruncatedState(ctx, eng)
	if err != nil {
		return ts, err
	}
	if ts.Index != 0 {
		r.mu.state.TruncatedState = &ts
	}
	return ts, nil
}

// FirstIndex implements the raft.Storage interface.
func (r *replicaRaftStorage) FirstIndex() (uint64, error) {
	ctx := r.AnnotateCtx(context.TODO())
	ts, err := (*Replica)(r).raftTruncatedStateLocked(ctx)
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

// raftFirstIndexLocked requires that r.mu is held.
func (r *Replica) raftFirstIndexLocked() (uint64, error) {
	return (*replicaRaftStorage)(r).FirstIndex()
}

// GetFirstIndex is the same function as raftFirstIndexLocked but it requires
// that r.mu is not held.
func (r *Replica) GetFirstIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftFirstIndexLocked()
}

// Snapshot implements the raft.Storage interface. Snapshot requires that
// r.mu is held. Note that the returned snapshot is a placeholder and
// does not contain any of the replica data. The snapshot is actually generated
// (and sent) by the Raft snapshot queue.
func (r *replicaRaftStorage) Snapshot() (raftpb.Snapshot, error) {
	r.mu.AssertHeld()
	appliedIndex := r.mu.state.RaftAppliedIndex
	term, err := r.Term(appliedIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: appliedIndex,
			Term:  term,
		},
	}, nil
}

// raftSnapshotLocked requires that r.mu is held.
func (r *Replica) raftSnapshotLocked() (raftpb.Snapshot, error) {
	return (*replicaRaftStorage)(r).Snapshot()
}

// snapshot creates an OutgoingSnapshot containing a rocksdb snapshot for the
// given range. Note that snapshot() is called without Replica.raftMu held.
func snapshot(
	ctx context.Context,
	desc multiraftbase.GroupDescriptor,
	snapType string,
	eng engine.Engine,
	snap engine.Reader,
	groupID multiraftbase.GroupID,
	eCache *raftEntryCache,
) (OutgoingSnapshot, error) {
	var snapData multiraftbase.RaftSnapshotData
	// Store RangeDescriptor as metadata, it will be retrieved by ApplySnapshot()
	snapData.GroupDescriptor = desc

	// Read the range metadata from the snapshot instead of the members
	// of the Range struct because they might be changed concurrently.
	appliedIndex, err := loadAppliedIndex(ctx, snap, groupID)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Synthesize our raftpb.ConfState from desc.
	var cs raftpb.ConfState
	for _, rep := range desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	term, err := term(ctx, snap, groupID, eCache, appliedIndex)
	if err != nil {
		return OutgoingSnapshot{}, errors.Errorf("failed to fetch term of %d: %s", appliedIndex, err)
	}

	rsl := makeReplicaStateLoader(groupID)
	state, err := rsl.load(ctx, snap, &desc)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Intentionally let this iterator and the snapshot escape so that the
	// streamer can send chunks from it bit by bit.
	iter := NewReplicaDataIterator(snap, true /* replicatedOnly */)
	snapUUID := uuid.MakeV4()

	helper.Printf(5, "generated %s snapshot %s at index %d",
		snapType, snapUUID.Short(), appliedIndex)
	return OutgoingSnapshot{
		RaftEntryCache: eCache,
		EngineSnap:     snap,
		Engine:         eng,
		Iter:           iter,
		State:          state,
		SnapUUID:       snapUUID,
		RaftSnap: raftpb.Snapshot{
			Data: snapUUID.GetBytes(),
			Metadata: raftpb.SnapshotMetadata{
				Index:     appliedIndex,
				Term:      term,
				ConfState: cs,
			},
		},
	}, nil
}

// GetSnapshot returns a snapshot of the replica appropriate for sending to a
// replica. If this method returns without error, callers must eventually call
// OutgoingSnapshot.Close.
func (r *Replica) GetSnapshot(
	ctx context.Context, snapType string,
) (_ *OutgoingSnapshot, err error) {
	// Get a snapshot while holding raftMu to make sure we're not seeing "half
	// an AddSSTable" (i.e. a state in which an SSTable has been linked in, but
	// the corresponding Raft command not applied yet).
	r.raftMu.Lock()
	eng := r.store.LoadGroupEngine(r.Desc().GroupID)
	snap := eng.NewSnapshot()
	desc := *r.mu.state.Desc
	r.raftMu.Unlock()

	defer func() {
		if err != nil {
			snap.Close()
		}
	}()

	r.mu.RLock()
	defer r.mu.RUnlock()
	groupID := r.GroupID

	snapData, err := snapshot(
		ctx, desc, snapType, eng, snap, groupID, r.store.raftEntryCache,
	)
	if err != nil {
		helper.Printf(5, "error generating snapshot: %s", err)
		return nil, err
	}
	return &snapData, nil
}

// OutgoingSnapshot contains the data required to stream a snapshot to a
// recipient. Once one is created, it needs to be closed via Close() to prevent
// resource leakage.
type OutgoingSnapshot struct {
	SnapUUID uuid.UUID
	// The Raft snapshot message to send. Contains SnapUUID as its data.
	RaftSnap raftpb.Snapshot
	// The RocksDB snapshot that will be streamed from.
	EngineSnap engine.Reader
	// The complete range iterator for the snapshot to stream.
	Iter   *ReplicaDataIterator
	Engine engine.Engine
	// The complete range iterator for the snapshot to stream.
	//Iter *ReplicaDataIterator
	// The replica state within the snapshot.
	State          multiraftbase.ReplicaState
	RaftEntryCache *raftEntryCache
}

// Close releases the resources associated with the snapshot.
func (s *OutgoingSnapshot) Close() {
	s.Iter.Close()
	s.EngineSnap.Close()
}

// IncomingSnapshot contains the data for an incoming streaming snapshot message.
type IncomingSnapshot struct {
	SnapUUID uuid.UUID
	// The RocksDB BatchReprs that make up this snapshot.
	Batches [][]byte
	// The Raft log entries for this snapshot.
	LogEntries [][]byte
	// The replica state at the time the snapshot was generated (never nil).
	State    *multiraftbase.ReplicaState
	snapType string
}

// append the given entries to the raft log. Takes the previous values of
// r.mu.lastIndex, r.mu.lastTerm, and r.mu.raftLogSize, and returns new values.
// We do this rather than modifying them directly because these modifications
// need to be atomic with the commit of the batch. This method requires that
// r.raftMu is held.
//
// append is intentionally oblivious to the existence of sideloaded proposals.
// They are managed by the caller, including cleaning up obsolete on-disk
// payloads in case the log tail is replaced.
func (r *Replica) append(
	ctx context.Context,
	batch engine.Writer,
	prevLastIndex uint64,
	prevLastTerm uint64,
	prevRaftLogSize int64,
	entries []raftpb.Entry,
) (uint64, uint64, int64, error) {
	if len(entries) == 0 {
		return prevLastIndex, prevLastTerm, prevRaftLogSize, nil
	}

	for i := range entries {
		ent := &entries[i]
		key := r.raftMu.stateLoader.RaftLogKey(ent.Index)
		data, err := ent.Marshal()
		if err != nil {
			return 0, 0, 0, err
		}
		err = batch.Put(key, data)
		if err != nil {
			return 0, 0, 0, err
		}
		prevRaftLogSize += int64(len(key) + len(data))
	}

	// Delete any previously appended log entries which never committed.
	lastIndex := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		// Note that the caller is in charge of deleting any sideloaded payloads
		// (which they must only do *after* the batch has committed).
		err := batch.Clear(r.raftMu.stateLoader.RaftLogKey(i))
		if err != nil {
			return 0, 0, 0, err
		}
		// TODO: update prevRaftLogSize
	}
	if err := r.raftMu.stateLoader.setLastIndex(ctx, batch, lastIndex); err != nil {
		return 0, 0, 0, err
	}

	return lastIndex, lastTerm, prevRaftLogSize, nil
}

// applySnapshot updates the replica based on the given snapshot and associated
// HardState (which may be empty, as Raft may apply some snapshots which don't
// require an update to the HardState). All snapshots must pass through Raft
// for correctness, i.e. the parameters to this method must be taken from
// a raft.Ready. It is the caller's responsibility to call
// r.store.processRangeDescriptorUpdate(r) after a successful applySnapshot.
// This method requires that r.raftMu is held.
func (r *Replica) applySnapshot(
	ctx context.Context, inSnap IncomingSnapshot, snap raftpb.Snapshot, hs raftpb.HardState,
) (err error) {
	helper.Println(5, "Apply Snapshot groupID:", r.GroupID)
	s := *inSnap.State
	if s.Desc.GroupID != r.GroupID {
		helper.Println(5, "s.Desc.GroupID:", s.Desc.GroupID, " r.GroupID:", r.GroupID)
		helper.Fatalf("unexpected group ID %s", s.Desc.GroupID)
	}

	if raft.IsEmptySnap(snap) {
		// Raft discarded the snapshot, indicating that our local state is
		// already ahead of what the snapshot provides. But we count it for
		// stats (see the defer above).
		return nil
	}

	// Note that we don't require that Raft supply us with a nonempty HardState
	// on a snapshot. We don't want to make that assumption because it's not
	// guaranteed by the contract. Raft *must* send us a HardState when it
	// increases the committed index as a result of the snapshot, but who is to
	// say it isn't going to accept a snapshot which is identical to the current
	// state?
	//
	// Note that since this snapshot comes from Raft, we don't have to synthesize
	// the HardState -- Raft wouldn't ask us to update the HardState in incorrect
	// ways.
	eng := r.store.LoadGroupEngine(r.mu.state.Desc.GroupID)

	logEntries := make([]raftpb.Entry, len(inSnap.LogEntries))
	for i, bytes := range inSnap.LogEntries {
		if err := logEntries[i].Unmarshal(bytes); err != nil {
			return err
		}
	}
	var raftLogSize int64
	// Write the snapshot's Raft log into the range.
	_, _, raftLogSize, err = r.append(
		ctx, eng, r.mu.lastIndex, r.mu.lastTerm, raftLogSize, logEntries,
	)
	if err != nil {
		helper.Fatalln("Failed to append log entries! err:", err)
		return err
	}
	//r.mu.lastIndex = lastIndex
	//r.mu.lastTerm = lastTerm

	if !raft.IsEmptyHardState(hs) {
		if err := r.raftMu.stateLoader.setHardState(ctx, eng, hs); err != nil {
			return errors.Wrapf(err, "unable to persist HardState %+v", &hs)
		}
	}

	// As outlined above, last and applied index are the same after applying
	// the snapshot (i.e. the snapshot has no uncommitted tail).
	if s.RaftAppliedIndex != snap.Metadata.Index {
		helper.Fatalf("snapshot RaftAppliedIndex %d doesn't match its metadata index %d",
			s.RaftAppliedIndex, snap.Metadata.Index)
	}

	r.mu.Lock()
	// We set the persisted last index to the last applied index. This is
	// not a correctness issue, but means that we may have just transferred
	// some entries we're about to re-request from the leader and overwrite.
	// However, raft.MultiNode currently expects this behaviour, and the
	// performance implications are not likely to be drastic. If our
	// feelings about this ever change, we can add a LastIndex field to
	// raftpb.SnapshotMetadata.
	r.mu.lastIndex = s.RaftAppliedIndex
	// We could recompute and return the lastTerm in the snapshot, but instead we
	// just set an invalid term and force a recomputation later.
	r.mu.lastTerm = invalidLastTerm
	//r.mu.raftLogSize = raftLogSize
	// Update the range and store stats.
	r.mu.state = s
	r.mu.Unlock()

	// As the last deferred action after committing the batch, update other
	// fields which are uninitialized or need updating. This may not happen
	// if the system config has not yet been loaded. While config update
	// will correctly set the fields, there is no order guarantee in
	// ApplySnapshot.
	// TODO: should go through the standard store lock when adding a replica.
	//if err := r.updateRangeInfo(s.Desc); err != nil {
	//panic(err)
	//}

	r.setDescWithoutProcessUpdate(s.Desc)

	return nil
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate. Requires raftMu to be locked.
func (r *Replica) setDescWithoutProcessUpdate(desc *multiraftbase.GroupDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if desc.GroupID != r.GroupID {
		helper.Fatalf("range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.GroupID, r.GroupID)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		helper.Fatalf("cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}

	r.mu.state.Desc = desc
}

type raftCommandEncodingVersion byte

// Raft commands are encoded with a 1-byte version (currently 0 or 1), an 8-byte
// ID, followed by the payload. This inflexible encoding is used so we can
// efficiently parse the command id while processing the logs.
//
// TODO(bdarnell): is this commandID still appropriate for our needs?
const (
	// The prescribed length for each command ID.
	raftCommandIDLen = 8
	// The initial Raft command version, used for all regular Raft traffic.
	raftVersionStandard raftCommandEncodingVersion = 0
	// A proposal containing an SSTable which preferably should be sideloaded
	// (i.e. not stored in the Raft log wholesale). Can be treated as a regular
	// proposal when arriving on the wire, but when retrieved from the local
	// Raft log it necessary to inline the payload first as it has usually
	// been sideloaded.
	raftVersionSideloaded raftCommandEncodingVersion = 1
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	//
	// TODO(tschottdorf): predates v1.0 by a significant margin. Remove.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

func encodeRaftCommandV1(commandID multiraftbase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionStandard, commandID, command)
}

// encode a command ID, an encoded RaftCommand, and
// whether the command contains a split.
func encodeRaftCommand(
	version raftCommandEncodingVersion, commandID multiraftbase.CmdIDKey, command []byte,
) []byte {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	x := make([]byte, 1, 1+raftCommandIDLen+len(command))
	x[0] = byte(version)
	x = append(x, []byte(commandID)...)
	x = append(x, command...)
	return x
}

// DecodeRaftCommand splits a raftpb.Entry.Data into its commandID and
// command portions. The caller is responsible for checking that the data
// is not empty (which indicates a dummy entry generated by raft rather
// than a real command). Usage is mostly internal to the storage package
// but is exported for use by debugging tools.
func DecodeRaftCommand(data []byte) (multiraftbase.CmdIDKey, []byte) {
	v := raftCommandEncodingVersion(data[0] & raftCommandNoSplitMask)
	if v != raftVersionStandard && v != raftVersionSideloaded {
		panic(fmt.Sprintf("unknown command encoding version %v", data[0]))
	}
	return multiraftbase.CmdIDKey(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}

func iterateEntries(
	ctx context.Context,
	e engine.Reader,
	groupID multiraftbase.GroupID,
	lo,
	hi uint64,
	scanFunc func(multiraftbase.KeyValue) (bool, error),
) error {
	for idx := lo; idx < hi; idx++ {
		key := keys.RaftLogKey(groupID, idx)
		val, err := e.Get(key)
		if err != nil {
			helper.Println(5, "Error getting raft log key, idx:", idx, " err:", err)
			return err
		}
		kv := multiraftbase.KeyValue{Key: key}
		kv.Value = multiraftbase.Value{RawBytes: val}
		_, err = scanFunc(kv)
		if err != nil {
			helper.Println(5, "Error calling scanFunc, err:", err)
			return err
		}
	}
	return nil
}
