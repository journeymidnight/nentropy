package engine

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/journeymidnight/nentropy/helper"
)

type BadgerDB struct {
}

func NewBadgerDB() {
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = helper.CONFIG.WALDir
	kvOpt.ValueDir = helper.CONFIG.WALDir
	kvOpt.TableLoadingMode = options.MemoryMap

	var err error
	s.WALstore, err = badger.NewKV(&kvOpt)
	helper.Checkf(err, "Error while creating badger KV WAL store")
}

func (b *BadgerDB) Get(key []byte) ([]byte, error) {

}

func (b *BadgerDB) Put(key []byte, value []byte) error {

}
