package multiraft

import (
	"fmt"
	"github.com/pkg/errors"
	"testing"
)

func Test_Int32ToByte(t *testing.T) {
	data := make([]byte, 4)
	old := int32(23492)
	putInt32(data, old)
	val := getInt32(data)
	if val != old {
		str := fmt.Sprintf("Error encoding or decoding old %d, new %d", old, val)
		t.Error(errors.New(str))
	}
}

func Test_KVBatch(t *testing.T) {
	bufLen := 2048 * 1024
	data := make([]byte, bufLen)
	batch := newKVBatch(bufLen, data)
	key := "123"
	val := "abcd"
	batch.Put([]byte(key), []byte(val))

	bufData := batch.Repr()
	t.Logf("bufData length %d", len(bufData))
	batch = newKVBatch(len(bufData), bufData)
	k, v, _ := batch.GetNextKVData()
	if string(k) != key {
		str := fmt.Sprintf("key is not equal. old %s, new %s", key, string(k))
		t.Error(errors.New(str))
	}
	if string(v) != val {
		str := fmt.Sprintf("val is not equal. old %s, new %s", val, string(v))
		t.Error(errors.New(str))
	}
	_, _, err := batch.GetNextKVData()
	if err == nil {
		str := fmt.Sprintf("There should be no data in buffer!")
		t.Error(errors.New(str))
	}
}
