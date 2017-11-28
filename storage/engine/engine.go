// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
)

// SimpleIterator is an interface for iterating over key/value pairs in an
// engine. SimpleIterator implementations are thread safe unless otherwise
// noted. SimpleIterator is a subset of the functionality offered by Iterator.
//type SimpleIterator interface {
//	// Close frees up resources held by the iterator.
//	Close()
//	// Seek advances the iterator to the first key in the engine which
//	// is >= the provided key.
//	Seek(key MVCCKey)
//	// Valid must be called after any call to Seek(), Next(), Prev(), or
//	// similar methods. It returns (true, nil) if the iterator points to
//	// a valid key (it is undefined to call Key(), Value(), or similar
//	// methods unless Valid() has returned (true, nil)). It returns
//	// (false, nil) if the iterator has moved past the end of the valid
//	// range, or (false, err) if an error has occurred. Valid() will
//	// never return true with a non-nil error.
//	Valid() (bool, error)
//	// Next advances the iterator to the next key/value in the
//	// iteration. After this call, Valid() will be true if the
//	// iterator was not positioned at the last key.
//	Next()
//	// NextKey advances the iterator to the next MVCC key. This operation is
//	// distinct from Next which advances to the next version of the current key
//	// or the next key if the iterator is currently located at the last version
//	// for a key.
//	NextKey()
//	// UnsafeKey returns the same value as Key, but the memory is invalidated on
//	// the next call to {Next,Prev,Seek,SeekReverse,Close}.
//	UnsafeKey() MVCCKey
//	// UnsafeValue returns the same value as Value, but the memory is
//	// invalidated on the next call to {Next,Prev,Seek,SeekReverse,Close}.
//	UnsafeValue() []byte
//}

// Iterator is an interface for iterating over key/value pairs in an
// engine. Iterator implementations are thread safe unless otherwise
// noted.
//type Iterator interface {
//	SimpleIterator
//
//	// SeekReverse advances the iterator to the first key in the engine which
//	// is <= the provided key.
//	SeekReverse(key MVCCKey)
//	// Prev moves the iterator backward to the previous key/value
//	// in the iteration. After this call, Valid() will be true if the
//	// iterator was not positioned at the first key.
//	Prev()
//	// PrevKey moves the iterator backward to the previous MVCC key. This
//	// operation is distinct from Prev which moves the iterator backward to the
//	// prev version of the current key or the prev key if the iterator is
//	// currently located at the first version for a key.
//	PrevKey()
//	// Key returns the current key.
//	Key() MVCCKey
//	// Value returns the current value as a byte slice.
//	Value() []byte
//	// ValueProto unmarshals the value the iterator is currently
//	// pointing to using a protobuf decoder.
//	ValueProto(msg protoutil.Message) error
//	// Less returns true if the key the iterator is currently positioned at is
//	// less than the specified key.
//	Less(key MVCCKey) bool
//	// ComputeStats scans the underlying engine from start to end keys and
//	// computes stats counters based on the values. This method is used after a
//	// range is split to recompute stats for each subrange. The start key is
//	// always adjusted to avoid counting local keys in the event stats are being
//	// recomputed for the first range (i.e. the one with start key == KeyMin).
//	// The nowNanos arg specifies the wall time in nanoseconds since the
//	// epoch and is used to compute the total age of all intents.
//	ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error)
//	// FindSplitKey finds a key from the given span such that the left side of
//	// the split is roughly targetSize bytes. The returned key will never be
//	// chosen from the key ranges listed in keys.NoSplitSpans if
//	// allowMeta2Splits is true and keys.NoSplitSpansWithoutMeta2Splits if
//	// allowMeta2Splits is false.
//	//
//	// TODO: remove allowMeta2Splits in version 1.3.
//	FindSplitKey(start, end MVCCKey, targetSize int64, allowMeta2Splits bool) (MVCCKey, error)
//}

// Reader is the read interface to an engine's data.
type Reader interface {
	// Close closes the reader, freeing up any outstanding resources. Note that
	// various implementations have slightly different behaviors. In particular,
	// Distinct() batches release their parent batch for future use while
	// Engines, Snapshots and Batches free the associated C++ resources.
	Close()
	// Get returns the value for the given key, nil otherwise.
	Get(key []byte) ([]byte, error)
}

// Writer is the write interface to an engine's data.
type Writer interface {
	//// Clear removes the item from the db with the given key.
	//// Note that clear actually removes entries from the storage
	//// engine, rather than inserting tombstones.
	Clear(key []byte) error

	//// The logic for merges is written in db.cc in order to be compatible with RocksDB.
	//Merge(key MVCCKey, value []byte) error
	// Put sets the given key to the value provided.
	Put(key []byte, value []byte) error
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// Engine is the interface that wraps the core operations of a key/value store.
type Engine interface {
	ReadWriter
	//// Attrs returns the engine/store attributes.
	//Attrs() roachpb.Attributes
	//// Capacity returns capacity details for the engine's available storage.
	//Capacity() (roachpb.StoreCapacity, error)
	//// Flush causes the engine to write all in-memory data to disk
	//// immediately.
	//Flush() error
	//// GetStats retrieves stats from the engine.
	//GetStats() (*Stats, error)
	//// GetAuxiliaryDir returns a path under which files can be stored
	//// persistently, and from which data can be ingested by the engine.
	////
	//// Not thread safe.
	//GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	//// NewReadOnly returns a new instance of a ReadWriter that wraps
	//// this engine. This wrapper panics when unexpected operations (e.g., write
	//// operations) are executed on it and caches iterators to avoid the overhead
	//// of creating multiple iterators for batched reads.
	//NewReadOnly() ReadWriter
	//// NewWriteOnlyBatch returns a new instance of a batched engine which wraps
	//// this engine. A write-only batch accumulates all mutations and applies them
	//// atomically on a call to Commit(). Read operations return an error.
	////
	//// TODO(peter): This should return a WriteBatch interface, but there are mild
	//// complications in both defining that interface and implementing it. In
	//// particular, Batch.Close would no longer come from Reader and we'd need to
	//// refactor a bunch of code in rocksDBBatch.
	//NewWriteOnlyBatch() Batch
	//// NewSnapshot returns a new instance of a read-only snapshot
	//// engine. Snapshots are instantaneous and, as long as they're
	//// released relatively quickly, inexpensive. Snapshots are released
	//// by invoking Close(). Note that snapshots must not be used after the
	//// original engine has been stopped.
	//NewSnapshot() Reader
	//// IngestExternalFile links a file into the RocksDB log-structured
	//// merge-tree.
	//IngestExternalFile(ctx context.Context, path string, move bool) error
}

// Batch is the interface for batch specific operations.
type Batch interface {
	ReadWriter
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the engine was created via NewBatch(). If
	// sync is true, the batch is synchronously committed to disk.
	Commit() error
	//// Distinct returns a view of the existing batch which only sees writes that
	//// were performed before the Distinct batch was created. That is, the
	//// returned batch will not read its own writes, but it will read writes to
	//// the parent batch performed before the call to Distinct(). The returned
	//// batch needs to be closed before using the parent batch again. This is used
	//// as an optimization to avoid flushing mutations buffered by the batch in
	//// situations where we know all of the batched operations are for distinct
	//// keys.
	//Distinct() ReadWriter
	//// Repr returns the underlying representation of the batch and can be used to
	//// reconstitute the batch on a remote node using Writer.ApplyBatchRepr().
	//Repr() []byte
}

// Stats is a set of RocksDB stats. These are all described in RocksDB
//
// Currently, we collect stats from the following sources:
// 1. RocksDB's internal "tickers" (i.e. counters). They're defined in
//    rocksdb/statistics.h
// 2. DBEventListener, which implements RocksDB's EventListener interface.
// 3. rocksdb::DB::GetProperty().
//
// This is a good resource describing RocksDB's memory-related stats:
// https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
type Stats struct {
	BlockCacheHits                 int64
	BlockCacheMisses               int64
	BlockCacheUsage                int64
	BlockCachePinnedUsage          int64
	BloomFilterPrefixChecked       int64
	BloomFilterPrefixUseful        int64
	MemtableTotalSize              int64
	Flushes                        int64
	Compactions                    int64
	TableReadersMemEstimate        int64
	PendingCompactionBytesEstimate int64
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. Returns the length in bytes of
// key and the value.
//func PutProto(
//	engine Writer, key MVCCKey, msg protoutil.Message,
//) (keyBytes, valBytes int64, err error) {
//	bytes, err := protoutil.Marshal(msg)
//	if err != nil {
//		return 0, 0, err
//	}
//
//	if err := engine.Put(key, bytes); err != nil {
//		return 0, 0, err
//	}
//
//	return int64(key.EncodedSize()), int64(len(bytes)), nil
//}
//
//// Scan returns up to max key/value objects starting from
//// start (inclusive) and ending at end (non-inclusive).
//// Specify max=0 for unbounded scans.
//func Scan(engine Reader, start, end MVCCKey, max int64) ([]MVCCKeyValue, error) {
//	var kvs []MVCCKeyValue
//	err := engine.Iterate(start, end, func(kv MVCCKeyValue) (bool, error) {
//		if max != 0 && int64(len(kvs)) >= max {
//			return true, nil
//		}
//		kvs = append(kvs, kv)
//		return false, nil
//	})
//	return kvs, err
//}
