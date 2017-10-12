package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleSetGet(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	key := []byte("hello")
	value := []byte("world")
	err = coll.Put(key, value)
	require.Equal(t, err, nil)

	newvalue, err := coll.Get(key)
	require.Equal(t, newvalue, value)
	require.Equal(t, err, nil)
	coll.Close()
	coll.Remove()
}
func TestSimpleSetGet_rewrite(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	key := []byte("hello")
	value := []byte("world")
	err = coll.Put(key, value)
	require.Equal(t, err, nil)

	value2 := []byte("hello")
	err = coll.Put(key, value2)
	require.Equal(t, err, nil)

	newvalue, err := coll.Get(key)
	require.Equal(t, newvalue, value2)
	require.Equal(t, err, nil)
	coll.Close()
	coll.Remove()
}

func TestSimpleIterator(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		err = coll.Put(k, k)
		require.Equal(t, err, nil)
	}

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, itr.Value(), []byte(fmt.Sprintf("%04d", i)))
		i++
	}
	coll.Close()
	coll.Remove()
}

func TestIteratorSeeking(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		err = coll.Put(k, k)
		require.Equal(t, err, nil)
	}

	itr := coll.NewIterator()
	tobeseeked := []byte(fmt.Sprintf("%04d", 55))
	itr.Seek(tobeseeked)

	require.Equal(t, itr.Valid(), true)
	require.Equal(t, itr.Value(), tobeseeked)
	coll.Close()
	coll.Remove()
}

func TestWriteBatch_AllPuts(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	wb := NewWriteBatch()

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Put(k, k)
	}

	err = coll.Write(wb)
	require.Equal(t, err, nil)

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, itr.Value(), []byte(fmt.Sprintf("%04d", i)))
		i++
	}

	coll.Close()
	coll.Remove()
}

func TestWriteBatch_rewritekeyinabatch(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	n := 100
	key := []byte("helloworld")
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprintf("%04d", i))
		coll.Put(key, v)
	}

	wb := NewWriteBatch()
	err = coll.Write(wb)
	require.Equal(t, err, nil)

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, itr.Value(), []byte(fmt.Sprintf("%04d", 99)))
		i++
	}

	require.Equal(t, i, 1)

	v, _ := coll.Get(key)
	require.Equal(t, v, []byte(fmt.Sprintf("%04d", 99)))
	coll.Close()
	coll.Remove()
}
func TestWriteBatch_AllDeletes(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		coll.Put(k, k)
	}

	wb := NewWriteBatch()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Delete(k)
	}
	err = coll.Write(wb)
	require.Equal(t, err, nil)

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		i++
	}

	require.Equal(t, i, 0)
	coll.Close()
	coll.Remove()
}
func TestWriteBatch_DeleteAll(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	wb := NewWriteBatch()

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Put(k, k)
	}

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Delete(k)
	}

	err = coll.Write(wb)
	require.Equal(t, err, nil)

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, itr.Value(), []byte(fmt.Sprintf("%04d", i)))
		i++
	}
	require.Equal(t, i, 0)

	coll.Close()
	coll.Remove()
}

func TestWriteBatch_DeleteHalf(t *testing.T) {
	cid := "asdf"
	os.Mkdir(cid, 0755)
	coll, err := NewCollection(cid)
	require.Equal(t, err, nil)

	wb := NewWriteBatch()

	n := 100
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Put(k, k)
	}

	for i := 50; i < n; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		wb.Delete(k)
	}

	err = coll.Write(wb)
	require.Equal(t, err, nil)

	itr := coll.NewIterator()
	i := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, itr.Value(), []byte(fmt.Sprintf("%04d", i)))
		i++
	}
	require.Equal(t, i, 50)

	coll.Close()
	coll.Remove()
}
