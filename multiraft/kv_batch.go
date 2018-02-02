package multiraft

import (
	"github.com/pkg/errors"
)

type KVBatch struct {
	data []byte
	curr int
	len  int
}

func putInt32(b []byte, v int32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func getInt32(b []byte) (v int32) {
	v = int32(b[0])
	v = v<<8 + int32(b[1])
	v = v<<8 + int32(b[2])
	v = v<<8 + int32(b[3])
	return
}

func newKVBatch(len int, data []byte) *KVBatch {
	batch := &KVBatch{}
	batch.data = data
	batch.len = len
	return batch
}

func (kv *KVBatch) Put(key, val []byte) error {
	keyLen := len(key)
	valLen := len(val)
	if (kv.curr + keyLen + valLen + 8) > kv.len {
		return errors.New("The cursor reaches the bottom")
	}
	putInt32(kv.data[kv.curr:], int32(keyLen))
	kv.curr += 4
	copy(kv.data[kv.curr:kv.curr+keyLen], key)
	kv.curr += keyLen
	putInt32(kv.data[kv.curr:], int32(valLen))
	kv.curr += 4
	copy(kv.data[kv.curr:kv.curr+valLen], val)
	kv.curr += valLen
	return nil
}

func (kv *KVBatch) Repr() []byte {
	return kv.data[:kv.curr]
}

func (kv *KVBatch) Reset() {
	kv.curr = 0
}

func (kv *KVBatch) GetNextKVData() (key, val []byte, err error) {
	if kv.curr == kv.len {
		return nil, nil, errors.New("The cursor reaches the bottom")
	}
	keyLen := int(getInt32(kv.data[kv.curr:]))
	kv.curr += 4
	key = kv.data[kv.curr : kv.curr+keyLen]
	kv.curr += keyLen

	valLen := int(getInt32(kv.data[kv.curr:]))
	kv.curr += 4
	val = kv.data[kv.curr : kv.curr+valLen]
	kv.curr += valLen
	return
}
