package engine

import (
	"fmt"
	"testing"
)

func Test_PutDataNoTxn(t *testing.T) {
	opt := KVOpt{Dir: "./testDir"}
	eng, err := NewBadgerDB(&opt)
	if err != nil {
		t.Error(err)
	}

	err = eng.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}

	eng.Close()

	fmt.Println("Finished!")
}

func Test_GetData(t *testing.T) {
	opt := KVOpt{Dir: "./testDir"}
	eng, err := NewBadgerDB(&opt)
	if err != nil {
		t.Error(err)
	}

	val, err := eng.Get([]byte("key1"))
	if err != nil {
		t.Error(err)
	}

	eng.Close()

	fmt.Println("Finished! val:", val)
}

func Test_PutDataByTxn(t *testing.T) {
	opt := KVOpt{Dir: "./testDir"}
	var eng Engine
	badger, err := NewBadgerDB(&opt)
	if err != nil {
		t.Error(err)
	}
	eng = badger
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()

	err = batch.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}

	err = batch.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Error(err)
	}

	err = batch.Commit()
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Finished!")
}

func Test_IteratorKeys(t *testing.T) {
	opt := KVOpt{Dir: "../../basedir/osd.1/1.6"}
	eng, err := NewBadgerDB(&opt)
	if err != nil {
		t.Error(err)
	}

	it := eng.NewIterator()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		fmt.Println("key=", k)
	}

	eng.Close()
}

func Test_Snapshot(t *testing.T) {
	opt := KVOpt{Dir: "../../osd/basedir/osd.1/1.1"}
	eng, err := NewBadgerDB(&opt)
	if err != nil {
		t.Error(err)
	}

	snap := eng.NewSnapshot()
	iter := snap.NewIterator()

	for iter.Rewind(); ; iter.Next() {
		if ok := iter.Valid(); !ok {
			break
		}
		item := iter.Item()
		k := item.Key()
		fmt.Println("key=", k)
	}

	eng.Close()
}
