// Copyright 2015 The Cockroach Authors.
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

package main

import (
	"github.com/journeymidnight/nentropy/storage/engine"
)

// ReplicaDataIterator provides a complete iteration over all key / value
// rows in a range, including all system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// all of the range's data.
//
// A ReplicaDataIterator provides a subset of the engine.Iterator interface.
type ReplicaDataIterator struct {
	curIndex int
	iterator engine.Iterator
}

// NewReplicaDataIterator creates a ReplicaDataIterator for the given replica.
func NewReplicaDataIterator(e engine.Reader, replicatedOnly bool,
) *ReplicaDataIterator {
	ri := &ReplicaDataIterator{
		iterator: e.NewIterator(),
	}
	return ri
}

// Close the underlying iterator.
func (ri *ReplicaDataIterator) Close() {
	ri.iterator.Close()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaDataIterator) Next() {
	ri.iterator.Next()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaDataIterator) Rewind() {
	ri.iterator.Rewind()
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaDataIterator) Valid() bool {
	return ri.iterator.Valid()
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaDataIterator) Item() engine.ItemIntf {
	return ri.iterator.Item()
}
