package multiraft

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/mon/osd/engine"
	"github.com/journeymidnight/nentropy/mon/osd/keys"
	"github.com/journeymidnight/nentropy/protos"
)

type replicaStateLoader struct {
	keys.RangeIDPrefixBuf
}

func makeReplicaStateLoader(rangeID protos.RangeID) replicaStateLoader {
	return replicaStateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
}

func loadHardState(
	ctx context.Context, reader engine.Reader, rangeID protos.RangeID,
) (raftpb.HardState, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadHardState(ctx, reader)
}

func (rsl replicaStateLoader) loadHardState(
	ctx context.Context, reader engine.Reader,
) (raftpb.HardState, error) {
	var data []byte
	data, err := reader.Get(rsl.RaftHardStateKey())
	if err != nil {
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
) (protos.RaftTruncatedState, error) {
	var truncState protos.RaftTruncatedState
	var data []byte
	data, err := reader.Get(rsl.RaftTruncatedStateKey())
	if err != nil {
		return protos.RaftTruncatedState{}, err
	}
	err = truncState.Unmarshal(data)
	if err != nil {
		return protos.RaftTruncatedState{}, err
	}
	return truncState, nil
}

func (rsl replicaStateLoader) setLastIndex(
	ctx context.Context, writer engine.ReadWriter, lastIndex uint64,
) error {
	var value protos.Value
	value.SetInt(int64(lastIndex))
	data, err := value.Marshal()
	if err != nil {
		return err
	}
	return writer.Put(rsl.RaftLastIndexKey(), data)
}

func loadTruncatedState(
	ctx context.Context, reader engine.Reader, rangeID protos.RangeID,
) (protos.RaftTruncatedState, error) {
	rsl := makeReplicaStateLoader(rangeID)
	return rsl.loadTruncatedState(ctx, reader)
}

func (rsl replicaStateLoader) setHardState(
	ctx context.Context, batch engine.ReadWriter, st raftpb.HardState,
) error {
	return batch.Put(rsl.RaftHardStateKey(), st)
}

// loadAppliedIndex returns the Raft applied index and the lease applied index.
func (rsl replicaStateLoader) loadAppliedIndex(
	ctx context.Context, reader engine.Reader,
) (uint64, uint64, error) {
	var appliedIndex uint64
	v, _, err := engine.MVCCGet(ctx, reader, rsl.RaftAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		appliedIndex = uint64(int64AppliedIndex)
	}
	// TODO(tschottdorf): code duplication.
	var leaseAppliedIndex uint64
	v, _, err = engine.MVCCGet(ctx, reader, rsl.LeaseAppliedIndexKey(),
		hlc.Timestamp{}, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64LeaseAppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		leaseAppliedIndex = uint64(int64LeaseAppliedIndex)
	}

	return appliedIndex, leaseAppliedIndex, nil
}
