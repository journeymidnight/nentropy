package engine

import (
	"fmt"
	"testing"
)

func Test_PutDataNoTxn(t *testing.T) {
	opt := KVOpt{WALDir: "./testDir"}
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
	opt := KVOpt{WALDir: "./testDir"}
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
	opt := KVOpt{WALDir: "./testDir"}
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
