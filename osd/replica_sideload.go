package main

import (
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/keys"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/protoutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var errSideloadedFileNotFound = errors.New("sideloaded file not found")

func (r *Replica) maybeSideloadEntriesRaftMuLocked(
	ctx context.Context, entriesToAppend []raftpb.Entry, batch engine.Batch,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {
	// TODO(tschottdorf): allocating this closure could be expensive. If so make
	// it a method on Replica.
	maybeRaftCommand := func(cmdID multiraftbase.CmdIDKey) (multiraftbase.RaftCommand, bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		cmd, ok := r.mu.proposals[cmdID]
		if ok {
			return cmd.command, true
		}
		return multiraftbase.RaftCommand{}, false
	}
	return maybeSideloadEntriesImpl(ctx, entriesToAppend, batch, maybeRaftCommand)
}

func maybeSideloadEntriesImpl(
	ctx context.Context,
	entriesToAppend []raftpb.Entry,
	batch engine.Batch,
	maybeRaftCommand func(multiraftbase.CmdIDKey) (multiraftbase.RaftCommand, bool),
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {

	cow := false
	helper.Println(15, "enter maybeSideloadEntriesImpl")
	for i := range entriesToAppend {
		var err error
		if sniffSideloadedRaftCommand(entriesToAppend[i].Data) {
			helper.Println(15, "sideloading command in append")
			if !cow {
				// Avoid mutating the passed-in entries directly. The caller
				// wants them to remain "fat".
				helper.Printf(15, "copying entries slice of length %d", len(entriesToAppend))
				cow = true
				entriesToAppend = append([]raftpb.Entry(nil), entriesToAppend...)
			}

			ent := &entriesToAppend[i]
			cmdID, data := DecodeRaftCommand(ent.Data) // cheap
			strippedCmd, ok := maybeRaftCommand(cmdID)
			if ok {
				helper.Println(15, "command already in memory")

			} else {
				// Bad luck: we didn't have the proposal in-memory, so we'll
				// have to unmarshal it.
				helper.Println(15, "proposal not already in memory; unmarshaling")
				if err := protoutil.Unmarshal(data, &strippedCmd); err != nil {
					return nil, 0, err
				}
			}

			if strippedCmd.WriteBatch == nil {
				// Still no AddSSTable; someone must've proposed a v2 command
				// but not becaused it contains an inlined SSTable. Strange, but
				// let's be future proof.
				helper.Println(5, "encountered sideloaded Raft command without inlined payload")
				continue
			}

			if strippedCmd.Method != multiraftbase.Put {
				err = helper.Errorf("side load request must be a put request")
				return nil, 0, err
			}

			putReq := multiraftbase.PutRequest{}
			err = putReq.Unmarshal(strippedCmd.WriteBatch.Data)
			if err != nil {
				helper.Printf(5, "Cannot unmarshal data to kv")
				return nil, 0, err
			}
			dataKey := keys.DataKey(entriesToAppend[i].Term, entriesToAppend[i].Index)
			dataToSideload := putReq.Value
			sideloadLenth := len(dataToSideload)
			putReq.Value = nil

			{
				var err error
				data, err = putReq.Marshal()
				if err != nil {
					return nil, 0, errors.Wrap(err, "while marshaling PutRequest")
				}
				strippedCmd.WriteBatch.Data = data
			}

			{
				var err error
				data, err = protoutil.Marshal(&strippedCmd)
				if err != nil {
					return nil, 0, errors.Wrap(err, "while marshaling stripped sideloaded command")
				}
			}

			ent.Data = encodeRaftCommandV2(cmdID, data)
			helper.Printf(15, "writing payload at index=%d term=%d", ent.Index, ent.Term)
			if err = batch.Put(dataKey, dataToSideload); err != nil {
				return nil, 0, err
			}
			sideloadedEntriesSize += int64(sideloadLenth)
		}
	}
	return entriesToAppend, sideloadedEntriesSize, nil
}

func sniffSideloadedRaftCommand(data []byte) (sideloaded bool) {
	return len(data) > 0 && data[0] == byte(raftVersionSideloaded)
}

// maybeInlineSideloadedRaftCommand takes an entry and inspects it. If its
// command encoding version indicates a sideloaded entry, it uses the entryCache
// or sideloadStorage to inline the payload, returning a new entry (which must
// be treated as immutable by the caller) or nil (if inlining does not apply)
//
// If a payload is missing, returns an error whose Cause() is
// errSideloadedFileNotFound.
func maybeInlineSideloadedRaftCommand(
	ctx context.Context,
	groupID multiraftbase.GroupID,
	ent raftpb.Entry,
	e engine.Reader,
	entryCache *raftEntryCache,
) (*raftpb.Entry, error) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil, nil
	}
	helper.Printf(10, "restore fat raft log ")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _ := entryCache.getEntries(
		nil, groupID, ent.Index, ent.Index+1, 1<<20,
	)

	if len(cachedSingleton) > 0 {
		helper.Println(20, "using cache hit")
		return &cachedSingleton[0], nil
	}

	// Make a shallow copy.
	entCpy := ent
	ent = entCpy
	helper.Println(20, "inlined entry not cached")

	// Out of luck, for whatever reason the inlined proposal isn't in the cache.
	cmdID, data := DecodeRaftCommand(ent.Data)

	var command multiraftbase.RaftCommand
	if err := protoutil.Unmarshal(data, &command); err != nil {
		return nil, err
	}

	var putReq multiraftbase.PutRequest
	if err := putReq.Unmarshal(command.WriteBatch.Data); err != nil {
		return nil, err
	}

	if len(putReq.Value) > 0 {
		helper.Println(20, "entry already inlined")
		return &ent, nil
	}

	key := keys.DataKey(ent.Term, ent.Index)
	value, err := e.Get(key)
	if err != nil {
		return nil, errSideloadedFileNotFound
	}

	if uint64(len(value)) != putReq.Size_ {
		helper.Println(5, "size mismatch when read value of key from badger")
		return nil, errors.New("size mismatch when read value of key from badger")
	}

	putReq.Value = value
	{
		var err error
		data, err = putReq.Marshal()
		if err != nil {
			return nil, errors.Wrap(err, "while marshaling PutRequest")
		}
		command.WriteBatch.Data = data
	}

	{
		var err error
		data, err = protoutil.Marshal(&command)
		if err != nil {
			return nil, errors.Wrap(err, "while marshaling stripped sideloaded command")
		}
	}

	ent.Data = encodeRaftCommandV2(cmdID, data)
	helper.Printf(10, "restore payload success")

	return &ent, nil
}

//// assertSideloadedRaftCommandInlined asserts that if the provided entry is a
//// sideloaded entry, then its payload has already been inlined. Doing so
//// requires unmarshalling the raft command, so this assertion should be kept out
//// of performance critical paths.
//func assertSideloadedRaftCommandInlined(ctx context.Context, ent *raftpb.Entry) {
//	if !sniffSideloadedRaftCommand(ent.Data) {
//		return
//	}
//
//	var command multiraftbase.RaftCommand
//	_, data := DecodeRaftCommand(ent.Data)
//	if err := protoutil.Unmarshal(data, &command); err != nil {
//		log.Fatal(ctx, err)
//	}
//
//	if len(command.ReplicatedEvalResult.AddSSTable.Data) == 0 {
//		// The entry is "thin", which is what this assertion is checking for.
//		log.Fatalf(ctx, "found thin sideloaded raft command: %+v", command)
//	}
//}
