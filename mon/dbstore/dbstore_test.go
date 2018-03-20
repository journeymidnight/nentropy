package dbstore

import (
	"os"
	"testing"

	"github.com/journeymidnight/badger"
	"github.com/journeymidnight/badger/options"
)

func Test_findByPk(t *testing.T) {
	testDir := "./badgerdb"
	os.Mkdir(testDir, os.ModePerm)
	defer os.RemoveAll(testDir)
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = testDir
	kvOpt.ValueDir = testDir
	kvOpt.TableLoadingMode = options.MemoryMap

	var err error
	db, err := badger.Open(&kvOpt)
	if err != nil {
		t.Fatal(err)
	}
	store := Init(db)
	trans, err := store.GetTransaction()
	if err != nil {
		t.Fatal(err)
	}
	trans.Set("test-prefix1", "test-key1", []byte("test-value1"))
	trans.Set("test-prefix2", "test-key2", []byte("test-value2"))
	err = store.SubmitTransactionSync(trans)
	if err != nil {
		t.Fatal(err)
	}

	val, err := store.GetEntry("test-prefix1", "test-key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "test-value1" {
		t.Fatal(err)
	}

	val, err = store.GetEntry("test-prefix1", "test-key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "test-value1" {
		t.Fatal(err)
	}
}
