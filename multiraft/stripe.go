package multiraft

import (
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/pkg/errors"

	"encoding/binary"

	"gopkg.in/mgo.v2/bson"
)

//ErrNoSuchPG : pg not exist
var ErrNoSuchPG = errors.New("no such pg")

//ErrPGAlreadyExists : pg already exist
var ErrPGAlreadyExists = errors.New("pg already exists")

//ErrFailedOpenBadgerStore : failed to open badger store
var ErrFailedOpenBadgerStore = errors.New("failed to open badger store")

//ErrFailedSettingKey : failed setting key to badger
var ErrFailedSettingKey = errors.New("failed setting key to badger")

//ErrFailedRemovingKey : failed removing key to badger
var ErrFailedRemovingKey = errors.New("failed removing key to badger")

//ErrNoValueForKey : no value for this key
var ErrNoValueForKey = errors.New("no value for this key")

// DefaultStripeSize default size for each stripe
var DefaultStripeSize uint64 = 64 << 10

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
	copy(newbuf, oid)
	newbuf = append(newbuf, buf...)
	return newbuf
}

func getOffset(oid, s []byte) uint64 {
	offbuf := s[len(oid):]
	sz, _ := binary.Uvarint(offbuf)
	return sz
}

func put(bat engine.Batch, offset uint64, n *onode, value []byte) {
	key := getDataKey(n.Oid, offset)
	bat.Put(key, value)
}

func get(eng engine.Engine, offset uint64, n *onode) ([]byte, error) {
	key := getDataKey(n.Oid, offset)
	value, err := eng.Get(key)
	return value, err
}

func clear(bat engine.Batch, offset uint64, n *onode) {
	key := getDataKey(n.Oid, offset)
	bat.Clear(key)
}

func createOrGetOnonde(eng engine.Engine, oid []byte) (o *onode) {
	nodebuffer, err := eng.Get(oid)
	//if cann't get an onode, we should create a new one for this object
	if err != nil || len(nodebuffer) <= 0 {
		o = newOnode(oid)
	} else {
		var p onode
		bson.Unmarshal(nodebuffer, &p)
		o = &p
	}
	return
}

func stripeWrite(eng engine.Engine, oid, value []byte, offset, length uint64) error {
	// find the onode, onode also stores in the same collection
	n := createOrGetOnonde(eng, oid)

	batch := eng.NewBatch()
	valuelen := uint64(len(value))
	stripeSize := n.StripeSize

	//cannot write more than you provide
	if length > valuelen {
		length = valuelen
	}
	// valueLocator is used to locate the user input value correctly
	valueLocator := uint64(0)

	for length > 0 {
		// align to stripeSize
		offsetRem := offset % stripeSize
		endRem := (offset + length) % stripeSize

		// how many stripes for remaining data
		remainStripeNumber := length / stripeSize

		// situation 1: aligned write, if there is a whole stripe of data, no matter rewrite or write a new one, we can safely write it
		if offsetRem == 0 && remainStripeNumber > 0 {
			helper.Printf(20, "full stripe at offset: %d\r\n", offset)
			put(batch, offset, n, value[valueLocator:valueLocator+stripeSize])
			offset += stripeSize
			length -= stripeSize
			valueLocator += stripeSize
			continue
		}

		// read at the aligned offset(align to stripe size)
		stripeOff := offset - offsetRem

		// read the original data from store
		prev, _ := get(eng, stripeOff, n)
		prevLen := uint64(len(prev))
		helper.Printf(5, "read previous stripe at offset %d, got %d \r\n", stripeOff, prevLen)

		var buf []byte

		// if we are not aligned to stripe size, maybe we need to add zeros or reusing some of the original data
		if offsetRem > 0 {
			var p uint64
			if prevLen < offsetRem {
				p = prevLen
			} else {
				p = offsetRem
			}

			// situation 2: resuing leading p bytes from the original data
			if p > 0 {
				helper.Printf(20, "reusing leading %d bytes \r\n", p)
				buf = append(buf, prev[:p]...)
			}
			// situation 3: original data(aka prevLen) shorter than our new offset, so we append zeros to the space for area (prevLen, offsetRem)
			// notice that, if previous data is totally empty, we also need to add leading zeros
			if p < offsetRem {
				helper.Printf(20, "add leading %d zeros for (prevLen, offsetRem) aka (%d, %d)\r\n", offsetRem-p, prevLen, offsetRem)
				//(fixme) this is ugly
				for i := uint64(0); i < offsetRem-p; i++ {
					buf = append(buf, 0)
				}
			}
		}

		// situation 4: we try use the whole remaning length of this stripe, but the new data length may be shorter than the remaning length
		// in which case, we only use (endRem - offsetRem)
		use := stripeSize - offsetRem
		if use > length {
			use = endRem - offsetRem
		}

		helper.Printf(5, "using %d bytes for this stripe \r\n", use)
		buf = append(buf, value[valueLocator:use+valueLocator]...)

		if endRem > 0 && remainStripeNumber == 0 {
			// situation 5: at the end of the stripe, reusing original bytes if we don't modify it
			if endRem < prevLen {
				l := prevLen - endRem
				buf = append(buf, prev[endRem:prevLen]...)
				helper.Printf(20, "resue trailing %d bytes \r\n", l)
			}
		}
		put(batch, stripeOff, n, buf)
		offset += use
		length -= use
		valueLocator += use
	}

	if offset > n.Size {
		n.Size = offset
		helper.Printf(5, "extending size to %d \r\n", offset+length)
	}

	//put onode to store
	newbuf, _ := bson.Marshal(n)
	batch.Put(oid, newbuf)

	helper.Printf(20, "finished processing stripes\r\n")
	return batch.Commit()
}

func min(a, b uint64) (c uint64) {
	if a < b {
		c = a
	} else {
		c = b
	}
	return
}

// Read reads an object from store
func stripeRead(eng engine.Engine, oid []byte, offset, length uint64) ([]byte, error) {
	//(fixme)find the onode first, should use cache
	val, err := eng.Get(oid)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, ErrNoValueForKey
	}

	var n onode
	bson.Unmarshal(val, &n)
	stripeSize := n.StripeSize
	size := n.Size

	if offset+length > size {
		length = size - offset
	}

	var buf []byte
	stripeOff := offset % stripeSize
	for length > 0 {
		stripebuf, _ := get(eng, offset-stripeOff, &n)
		buflen := uint64(len(stripebuf))
		helper.Printf(20, "got %d bytes for offset %d\r\n", buflen, offset-stripeOff)

		swant := min(stripeSize-stripeOff, length)
		if buflen > 0 {
			if swant == buflen {
				buf = append(buf, stripebuf...)
				helper.Printf(20, "taking full stripe at offset %d \r\n", stripeOff)
			} else {
				l := min(stripeSize-stripeOff, swant)

				//maybe wrong
				buf = append(buf, stripebuf[stripeOff:l+stripeOff]...)
				helper.Printf(20, "taking at offset %d ~ %d\r\n", stripeOff, l)

				if l < swant {
					helper.Printf(20, "adding %d zeros\r\n", swant-l)
					for i := uint64(0); i < swant-l; i++ {
						buf = append(buf, 0)
					}
				}
			}
		} else {
			helper.Printf(20, "adding %d zeros\r\n", swant)
			for i := uint64(0); i < swant; i++ {
				buf = append(buf, 0)
			}
		}

		offset += swant
		length -= swant
		stripeOff = 0
	}

	return buf, nil
}

//Remove removes a object from store
func stripeRemove(eng engine.Engine, oid []byte) error {
	//(fixme)find the onode first, should use cache
	val, err := eng.Get(oid)
	if err != nil {
		return err
	}

	if val == nil {
		return ErrNoValueForKey
	}

	var n onode
	bson.Unmarshal(val, &n)

	size := n.Size
	stripeSize := n.StripeSize
	stripeNum := size / stripeSize
	stripeRem := size % stripeSize

	bat := eng.NewBatch()
	var i uint64
	for ; i < stripeNum; i++ {
		clear(bat, i*stripeSize, &n)
	}

	if stripeRem > 0 {
		clear(bat, i*stripeSize, &n)
	}

	// also delete the onode
	bat.Clear(oid)
	return bat.Commit()
}
