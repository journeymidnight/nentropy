package main

import (
	"golang.org/x/net/context"

	"encoding/binary"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/badger"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/keys"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/protoutil"
)

type replicaStateLoader struct {
	keys.GroupIDPrefixBuf
}

func makeReplicaStateLoader(groupID multiraftbase.GroupID) replicaStateLoader {
	return replicaStateLoader{
		GroupIDPrefixBuf: keys.MakeGroupIDPrefixBuf(groupID),
	}
}

func (rsl replicaStateLoader) loadHardState(
	ctx context.Context, reader engine.Reader,
) (raftpb.HardState, error) {
	var data []byte
	data, err := reader.Get(rsl.RaftHardStateKey())
	if err != nil && err != badger.ErrKeyNotFound {
		return raftpb.HardState{}, err
	}
	var hs raftpb.HardState
	err = hs.Unmarshal(data)
	if err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

func (rsl replicaStateLoader) loadTruncatedState(
	ctx context.Context, reader engine.Reader,
) (multiraftbase.RaftTruncatedState, error) {
	var truncState multiraftbase.RaftTruncatedState
	var data []byte
	data, err := reader.Get(rsl.RaftTruncatedStateKey())
	if err != nil && err != badger.ErrKeyNotFound {
		return multiraftbase.RaftTruncatedState{}, err
	}
	err = truncState.Unmarshal(data)
	if err != nil {
		return multiraftbase.RaftTruncatedState{}, err
	}
	return truncState, nil
}

func (rsl replicaStateLoader) setTruncatedState(
	ctx context.Context,
	eng engine.Writer,
	truncState *multiraftbase.RaftTruncatedState,
) error {
	if (*truncState == multiraftbase.RaftTruncatedState{}) {
		return nil
	}
	val, err := truncState.Marshal()
	if err != nil {

	}
	return eng.Put(rsl.RaftTruncatedStateKey(), val)
}

func loadTruncatedState(
	ctx context.Context, reader engine.Reader, groupID multiraftbase.GroupID,
) (multiraftbase.RaftTruncatedState, error) {
	rsl := makeReplicaStateLoader(groupID)
	return rsl.loadTruncatedState(ctx, reader)
}

func (rsl replicaStateLoader) setHardState(
	ctx context.Context, batch engine.Writer, st raftpb.HardState,
) error {
	data, err := st.Marshal()
	if err != nil {
		return err
	}
	return batch.Put(rsl.RaftHardStateKey(), data)
}

// loadAppliedIndex returns the Raft applied index and the lease applied index.
func (rsl replicaStateLoader) loadAppliedIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, error) {
	var appliedIndex uint64
	v, err := reader.Get(rsl.RaftAppliedIndexKey())
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, err
	}
	if v != nil {
		int64AppliedIndex, n := binary.Varint(v)
		if n <= 0 {
			return 0, fmt.Errorf("int64 varint decoding failed: %d", n)
		}
		appliedIndex = uint64(int64AppliedIndex)
	}

	return appliedIndex, nil
}

// setAppliedIndex sets the {raft,lease} applied index values, properly
// accounting for existing keys in the returned stats.
func (rsl replicaStateLoader) setAppliedIndex(
	ctx context.Context,
	eng engine.Writer,
	appliedIndex uint64,
) error {
	data := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(data, int64(appliedIndex))
	if err := eng.Put(rsl.RaftAppliedIndexKey(),
		data); err != nil {
		return err
	}
	return nil
}

// loadState loads a ReplicaState from disk. The exception is the Desc field,
// which is updated transactionally, and is populated from the supplied
// GroupDescriptor under the convention that that is the latest committed
// version.
func (rsl replicaStateLoader) load(
	ctx context.Context, reader engine.Reader, desc *multiraftbase.GroupDescriptor,
) (multiraftbase.ReplicaState, error) {
	var s multiraftbase.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*multiraftbase.GroupDescriptor)

	var err error
	if s.RaftAppliedIndex, err = rsl.loadAppliedIndex(ctx, reader); err != nil {
		return multiraftbase.ReplicaState{}, err
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := rsl.loadTruncatedState(ctx, reader)
	if err != nil {
		return multiraftbase.ReplicaState{}, err
	}
	s.TruncatedState = &truncState
	helper.Println(5, "replicaStateLoader load RaftAppliedIndex:", s.RaftAppliedIndex)
	helper.Println(5, "replicaStateLoader load TruncatedState Index:", s.TruncatedState.Index, " Term:", s.TruncatedState.Term)

	return s, nil
}

func (rsl replicaStateLoader) save(
	ctx context.Context, eng engine.Writer, state multiraftbase.ReplicaState,
) error {
	if err := rsl.setAppliedIndex(
		ctx, eng, state.RaftAppliedIndex,
	); err != nil {
		return err
	}
	if err := rsl.setTruncatedState(ctx, eng, state.TruncatedState); err != nil {
		return err
	}

	return nil
}

// The rest is not technically part of ReplicaState.
// TODO(tschottdorf): more consolidation of ad-hoc structures: last index and
// hard state. These are closely coupled with ReplicaState (and in particular
// with its TruncatedState) but are different in that they are not consistently
// updated through Raft.

func loadLastIndex(
	ctx context.Context, reader engine.Reader, groupID multiraftbase.GroupID,
) (uint64, error) {
	rsl := makeReplicaStateLoader(groupID)
	return rsl.loadLastIndex(ctx, reader)
}

func (rsl replicaStateLoader) loadLastIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, error) {
	var lastIndex uint64
	v, err := reader.Get(rsl.RaftLastIndexKey())
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, err
	}
	if v != nil {
		int64LastIndex, n := binary.Varint(v)
		if n <= 0 {
			return 0, fmt.Errorf("int64 varint decoding failed: %d", n)
		}
		lastIndex = uint64(int64LastIndex)
	} else {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, err := rsl.loadTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

func (rsl replicaStateLoader) setLastIndex(
	ctx context.Context, writer engine.Writer, lastIndex uint64,
) error {
	data := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(data, int64(lastIndex))
	return writer.Put(rsl.RaftLastIndexKey(), data)
}

func loadAppliedIndex(
	ctx context.Context, reader engine.Reader, groupID multiraftbase.GroupID,
) (uint64, error) {
	helper.Println(5, " loadAppliedIndex, groupID:", groupID)
	rsl := makeReplicaStateLoader(groupID)
	return rsl.loadAppliedIndex(ctx, reader)
}
