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
	//// Not thread safe.
	//GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
}

// Batch is the interface for batch specific operations.
type Batch interface {
	Writer
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the engine was created via NewBatch(). If
	// sync is true, the batch is synchronously committed to disk.
	Commit() error
	Close()
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
