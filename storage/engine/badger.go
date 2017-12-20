package engine

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/helper"
)

type BadgerDB struct {
	kv *badger.KV
}

type badgerDBBatch struct {
	b  *BadgerDB
	wb []*badger.Entry
}

type KVOpt struct {
	WALDir string
}

func NewBadgerDB(opt *KVOpt) (*BadgerDB, error) {
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = opt.WALDir
	kvOpt.ValueDir = opt.WALDir
	kvOpt.TableLoadingMode = options.MemoryMap

	r := &BadgerDB{}

	var err error
	r.kv, err = badger.NewKV(&kvOpt)
	helper.Checkf(err, "Error while creating badger KV WAL store")
	return r, nil
}

func getItemValue(item *badger.KVItem) (val []byte) {
	err := item.Value(func(v []byte) error {
		if v == nil {
			return nil
		}
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})

	if err != nil {
		helper.Check(err)
	}
	return val
}

func (b *BadgerDB) Get(key []byte) ([]byte, error) {
	var item badger.KVItem
	if err := b.kv.Get(key, &item); err != nil {
		rerr := helper.Wrapf(err, "while fetching hardstate from wal")
		return nil, rerr
	}
	val := getItemValue(&item)
	return val, nil
}

func (b *BadgerDB) Clear(key []byte) error {
	e := &badger.Entry{
		Key:  key,
		Meta: badger.BitDelete,
	}
	ret := make(chan error)
	f := func(err error) {
		ret <- err
	}

	b.kv.BatchSetAsync([]*badger.Entry{e}, f)
	err := <-ret
	return err
}

func (b *BadgerDB) Close() {

}

func (b *BadgerDB) Put(key []byte, value []byte) error {
	wb := make([]*badger.Entry, 0, 1)
	wb = badger.EntriesSet(wb, key, value)
	if err := b.kv.BatchSet(wb); err != nil {
		return err
	}
	for _, wbe := range wb {
		if err := wbe.Error; err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerDB) NewBatch() Batch {
	return newBadgerDBBatch(b)
}

func newBadgerDBBatch(b *BadgerDB) *badgerDBBatch {
	r := &badgerDBBatch{b: b}
	r.wb = make([]*badger.Entry, 0, 100)
	return r
}

func (r *badgerDBBatch) Close() {

}

func (r *badgerDBBatch) Put(key []byte, value []byte) error {
	r.wb = badger.EntriesSet(r.wb, key, value)
	return nil
}

func (r *badgerDBBatch) Clear(key []byte) error {
	return r.b.Clear(key)
}

func (r *badgerDBBatch) Get(key []byte) ([]byte, error) {
	return r.b.Get(key)
}

func (r *badgerDBBatch) Commit() error {
	if err := r.b.kv.BatchSet(r.wb); err != nil {
		return err
	}
	for _, wbe := range r.wb {
		if err := wbe.Error; err != nil {
			return err
		}
	}
	return nil
}
