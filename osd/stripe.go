package osd

import (
	"encoding/binary"
	"fmt"

	"github.com/journeymidnight/nentropy/store"
)

// default size for each stripe
var defaultStripeSize uint64 = 64 << 10

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
	return &onode{Oid: oid, Size: 0, StripeSize: defaultStripeSize}
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
	fmt.Println("Write: before Put, current batch is: ")
	bat.Traverse()
	key := getDataKey(n.Oid, offset)
	fmt.Println("stripeWrite, key is: ", key, "value is: ", value)
	bat.Put(key, value)
	fmt.Println("Write: after Put, current batch is: ")
	bat.Traverse()
}

func stripeRead(coll *store.Collection, offset uint64, n *onode) ([]byte, error) {
	key := getDataKey(n.Oid, offset)
	value, err := coll.Get(key)
	fmt.Println("stripeRead, key is: ", key, "value is: ", value)
	return value, err
}
