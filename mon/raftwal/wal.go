/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package raftwal

import (
	"bytes"
	"encoding/binary"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger"

	"github.com/journeymidnight/nentropy/helper"
)

type Wal struct {
	wals *badger.DB
	id   uint64
}

func Init(walStore *badger.DB, id uint64) *Wal {
	return &Wal{wals: walStore, id: id}
}

func (w *Wal) snapshotKey(gid uint32) []byte {
	b := make([]byte, 14)
	binary.BigEndian.PutUint64(b[0:8], w.id)
	copy(b[8:10], []byte("ss"))
	binary.BigEndian.PutUint32(b[10:14], gid)
	return b
}

func (w *Wal) hardStateKey(gid uint32) []byte {
	b := make([]byte, 14)
	binary.BigEndian.PutUint64(b[0:8], w.id)
	copy(b[8:10], []byte("hs"))
	binary.BigEndian.PutUint32(b[10:14], gid)
	return b
}

func (w *Wal) entryKey(gid uint32, term, idx uint64) []byte {
	b := make([]byte, 28)
	binary.BigEndian.PutUint64(b[0:8], w.id)
	binary.BigEndian.PutUint32(b[8:12], gid)
	binary.BigEndian.PutUint64(b[12:20], term)
	binary.BigEndian.PutUint64(b[20:28], idx)
	return b
}

func (w *Wal) prefix(gid uint32) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[0:8], w.id)
	binary.BigEndian.PutUint32(b[8:12], gid)
	return b
}

func (w *Wal) StoreSnapshot(gid uint32, s raftpb.Snapshot) error {
	if raft.IsEmptySnap(s) {
		return nil
	}
	data, err := s.Marshal()
	if err != nil {
		return helper.Wrapf(err, "wal.Store: While marshal snapshot")
	}
	if err := w.wals.Update(func(txn *badger.Txn) error {
		err := txn.Set(w.snapshotKey(gid), data)
		return err
	}); err != nil {
		return err
	}
	helper.Printf(10, "Writing snapshot to WAL: %+v\n", s)

	// Delete all entries before this snapshot to save disk space.
	start := w.entryKey(gid, 0, 0)
	last := w.entryKey(gid, s.Metadata.Term, s.Metadata.Index)
	opt := badger.DefaultIteratorOptions
	//opt.FetchValues = false
	txn := w.wals.NewTransaction(true)

	it := txn.NewIterator(opt)
	//defer it.Close()
	for it.Seek(start); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if bytes.Compare(k, last) > 0 {
			break
		}
		err = txn.Delete(k)
		if err != nil {
			break
		}
	}
	it.Close()

	if err != nil {
		txn.Discard()
		return err
	}

	if err = txn.Commit(nil); err != nil {
		txn.Discard()
		return err
	}

	return nil
}

// Store stores the snapshot, hardstate and entries for a given RAFT group.
func (w *Wal) Store(gid uint32, h raftpb.HardState, es []raftpb.Entry) error {
	txn := w.wals.NewTransaction(true)

	if !raft.IsEmptyHardState(h) {
		data, err := h.Marshal()
		if err != nil {
			return helper.Wrapf(err, "wal.Store: While marshal hardstate")
		}
		err = txn.Set(w.hardStateKey(gid), data)
		if err != nil {
			txn.Discard()
			return err
		}

	}

	var t, i uint64
	for _, e := range es {
		t, i = e.Term, e.Index
		data, err := e.Marshal()
		if err != nil {
			return helper.Wrapf(err, "wal.Store: While marshal entry")
		}
		k := w.entryKey(gid, e.Term, e.Index)
		err = txn.Set(k, data)
		if err != nil {
			txn.Discard()
			return err
		}
	}

	// If we get no entries, then the default value of t and i would be zero. That would
	// end up deleting all the previous valid raft entry logs. This check avoids that.
	if t > 0 || i > 0 {
		// When writing an Entry with Index i, any previously-persisted entries
		// with Index >= i must be discarded.
		start := w.entryKey(gid, t, i+1)
		prefix := w.prefix(gid)
		opt := badger.DefaultIteratorOptions
		//opt.FetchValues = false
		itr := txn.NewIterator(opt)

		for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
			key := itr.Item().Key()
			newk := make([]byte, len(key))
			copy(newk, key)
			err := txn.Delete(newk)
			if err != nil {
				break
			}
		}
		itr.Close()
	}

	if err := txn.Commit(nil); err != nil {
		txn.Discard()
		return err
	}

	return nil
}

func (w *Wal) Snapshot(gid uint32) (snap raftpb.Snapshot, rerr error) {
	var val []byte
	if err := w.wals.View(func(tx *badger.Txn) error {
		item, err := tx.Get(w.snapshotKey(gid))
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return
	}

	//val := item.Value()
	// Originally, with RocksDB, this can return an error and a non-null rdb.Slice object with Data=nil.
	// And for this case, we do NOT return.
	rerr = helper.Wrapf(snap.Unmarshal(val), "While unmarshal snapshot")
	return
}

func (w *Wal) HardState(gid uint32) (hd raftpb.HardState, rerr error) {
	var val []byte
	if err := w.wals.View(func(tx *badger.Txn) error {
		item, err := tx.Get(w.hardStateKey(gid))
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return
	}
	//val := item.Value()
	// Originally, with RocksDB, this can return an error and a non-null rdb.Slice object with Data=nil.
	// And for this case, we do NOT return.
	rerr = helper.Wrapf(hd.Unmarshal(val), "While unmarshal hardstate")
	return
}

func (w *Wal) Entries(gid uint32, fromTerm, fromIndex uint64) (es []raftpb.Entry, rerr error) {
	if err := w.wals.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		prefix := w.prefix(gid)
		start := w.entryKey(gid, fromTerm, fromIndex)
		it := txn.NewIterator(opts)
		for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.Value()
			if err != nil {
				return err
			}
			var e raftpb.Entry
			if err := e.Unmarshal(v); err != nil {
				return err
			}
			es = append(es, e)
		}
		return nil
	}); err != nil {
		return es, helper.Wrapf(err, "While unmarshal raftpb.Entry")
	}
	return
}
