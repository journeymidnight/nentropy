package engine

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/helper"
)

type BadgerDB struct {
	db *badger.DB
}

type badgerDBBatch struct {
	b   *BadgerDB
	txn *badger.Txn
	wb  []*badger.Entry
}

type KVOpt struct {
	WALDir string
}

func NewBadgerDB(opt *KVOpt) (*BadgerDB, error) {
	dbOpts := badger.DefaultOptions
	dbOpts.SyncWrites = true
	dbOpts.Dir = opt.WALDir
	dbOpts.ValueDir = opt.WALDir
	dbOpts.TableLoadingMode = options.MemoryMap

	r := &BadgerDB{}

	var err error
	r.db, err = badger.Open(dbOpts)
	helper.Checkf(err, "Error while creating badger KV WAL store")
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
	}); err != nil && err != badger.ErrKeyNotFound {
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

func (b *BadgerDB) NewBatch() Batch {
	return newBadgerDBBatch(b)
}

func newBadgerDBBatch(b *BadgerDB) *badgerDBBatch {
	r := &badgerDBBatch{b: b}
	r.txn = b.db.NewTransaction(true)
	return r
}

func (r *badgerDBBatch) Close() {

}

func (r *badgerDBBatch) Put(key []byte, value []byte) error {
	err := r.txn.Set(key, value)
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
