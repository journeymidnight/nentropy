package osd

import (
	"encoding/binary"

	"github.com/journeymidnight/nentropy/store"
)

// DefaultStripeSize default size for each stripe
var DefaultStripeSize uint64 = 64 << 10

var onodePrefix = []byte("O")
var dataPrefix = []byte("D")

// onode holds the metadata of each object in osd store
// use Capital because bson can only serialize Capital fileds
type onode struct {
	//nid        uint64 //numeric id (locally unique)
	Oid        []byte //objectid of an object
	Size       uint64 //object size
	StripeSize uint64 //size of each  stripe
}

func newOnode(oid []byte) *onode {
	return &onode{Oid: oid, Size: 0, StripeSize: DefaultStripeSize}
}

func getDataKey(oid []byte, offset uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf[:], offset)
	newbuf := make([]byte, binary.MaxVarintLen64+len(oid))
	copy(newbuf, append(oid, buf...))
	return newbuf
}

func getOffset(oid, s []byte) uint64 {
	offbuf := s[len(oid):]
	sz, _ := binary.Uvarint(offbuf)
	return sz
}

func stripeWrite(bat *store.WriteBatch, offset uint64, n *onode, value []byte) {
	key := getDataKey(n.Oid, offset)
	bat.Put(key, value)
}

func stripeRead(coll *store.Collection, offset uint64, n *onode) ([]byte, error) {
	key := getDataKey(n.Oid, offset)
	value, err := coll.Get(key)
	return value, err
}

func stripeDelete(bat *store.WriteBatch, offset uint64, n *onode) {
	key := getDataKey(n.Oid, offset)
	bat.Delete(key)
}
