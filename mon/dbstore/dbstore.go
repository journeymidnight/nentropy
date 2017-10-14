package dbstore

import (
	"errors"

	"github.com/dgraph-io/badger"
	"github.com/journeymidnight/nentropy/helper"
)

type Store struct {
	db *badger.KV
}

type Transaction interface {
	Set(string, string, []byte) error
}

type transaction struct {
	entries []*badger.Entry
}

/*Init init badger db
 */
func Init(db *badger.KV) *Store {
	return &Store{db: db}
}

func (t *transaction) Set(prefix, key string, data []byte) error {
	t.entries = badger.EntriesSet(t.entries, []byte(prefix+key), data)
	return nil
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

func (s *Store) GetTransaction() (Transaction, error) {
	t := transaction{}
	t.entries = make([]*badger.Entry, 0, 100)
	return &t, nil
}

func (s *Store) SubmitTransactionSync(v interface{}) error {
	t, ok := v.(*transaction)
	if !ok {
		return errors.New("Erorr type assertions for transaction")
	}
	if err := s.db.BatchSet(t.entries); err != nil {
		return err
	}
	for _, entry := range t.entries {
		if err := entry.Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetEntry(prefix, key string) (entry []byte, rerr error) {
	var item badger.KVItem
	if err := s.db.Get([]byte(prefix+key), &item); err != nil {
		rerr = helper.Wrapf(err, "while fetching hardstate from wal")
		return
	}
	entry = getItemValue(&item)
	return
}
