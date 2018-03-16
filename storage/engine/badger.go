package engine

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

type BadgerDB struct {
	db *badger.DB
}

type badgerDBBatch struct {
	b   *BadgerDB
	txn *badger.Txn
}

type KVOpt struct {
	Dir string
}

func NewBadgerDB(opt *KVOpt) (Engine, error) {
	dbOpts := badger.DefaultOptions
	dbOpts.SyncWrites = true
	dbOpts.Dir = opt.Dir
	dbOpts.ValueDir = opt.Dir
	dbOpts.TableLoadingMode = options.MemoryMap

	r := &BadgerDB{}

	r.db, _ = badger.Open(dbOpts)
	return r, nil
}

func (b *BadgerDB) Get(key []byte) ([]byte, error) {
	var val []byte
	if err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return val, nil
}

func (b *BadgerDB) Clear(key []byte) error {
	if err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	}); err != nil {
		return err
	}
	return nil
}

func (b *BadgerDB) Close() {
	b.db.Close()
}

func (b *BadgerDB) Put(key []byte, value []byte) error {
	if err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	}); err != nil {
		return err
	}
	return nil
}

func (b *BadgerDB) NewIterator() Iterator {
	if b.db == nil {
		panic("BadgerDB is not initialized yet")
	}
	bgIt := &badgerIterator{}
	bgIt.txn = b.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	bgIt.iter = bgIt.txn.NewIterator(opts)
	return bgIt
}

type badgerDBSnapshot struct {
	parent *badger.DB
	txn    *badger.Txn
}

func (b *badgerDBSnapshot) Close() {
	b.txn.Discard()
}

func (b *badgerDBSnapshot) Get(key []byte) ([]byte, error) {
	var val []byte
	txn := b.txn
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	val, err = item.Value()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (b *badgerDBSnapshot) NewIterator() Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	bgIt := &badgerIterator{}
	bgIt.iter = b.txn.NewIterator(opts)
	return bgIt
}

// NewSnapshot creates a snapshot handle from engine and returns a
// read-only rocksDBSnapshot engine.
func (b *BadgerDB) NewSnapshot() Reader {
	if b.db == nil {
		panic("BadgerDB is not initialized yet")
	}
	txn := b.db.NewTransaction(false)
	return &badgerDBSnapshot{
		parent: b.db,
		txn:    txn,
	}
}

func (b *BadgerDB) NewBatch(update bool) Batch {
	return newBadgerDBBatch(b, update)
}

func newBadgerDBBatch(b *BadgerDB, update bool) *badgerDBBatch {
	r := &badgerDBBatch{b: b}
	r.txn = b.db.NewTransaction(update)
	return r
}

func (r *badgerDBBatch) NewIterator() Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	bgIt := &badgerIterator{}
	bgIt.iter = r.txn.NewIterator(opts)
	return bgIt
}

func (r *badgerDBBatch) Close() {
	// calling this multiple times doesn't cause any issues
	r.txn.Discard()
}

func (r *badgerDBBatch) Put(key []byte, value []byte) error {
	newKey := make([]byte, len(key))
	for i, v := range key {
		newKey[i] = v
	}
	err := r.txn.Set(newKey, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *badgerDBBatch) Clear(key []byte) error {
	err := r.txn.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (r *badgerDBBatch) Get(key []byte) ([]byte, error) {
	item, err := r.txn.Get(key)
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *badgerDBBatch) Commit() error {
	if err := r.txn.Commit(nil); err != nil {
		return err
	}
	return nil
}

//-----------------------------------------------------
type badgerIterator struct {
	iter *badger.Iterator
	txn  *badger.Txn
}

func (it *badgerIterator) Close() {
	it.iter.Close()
	if it.txn != nil {
		// Close txn opened with read-only mode
		it.txn.Discard()
	}
}

func (it *badgerIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *badgerIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *badgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(prefix)
}

func (it *badgerIterator) Next() {
	it.iter.Next()
}

func (it *badgerIterator) Rewind() {
	it.iter.Rewind()
}

func (it *badgerIterator) Item() ItemIntf {
	return it.iter.Item()
}
