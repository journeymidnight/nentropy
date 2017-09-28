// Copyright (C) 2012 Numerotron Inc.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

// Package consistent provides a consistent hashing function.
//
// Consistent hashing is often used to distribute requests to a changing set of servers.  For example,
// say you have some cache servers cacheA, cacheB, and cacheC.  You want to decide which cache server
// to use to look up information on a user.
//
// You could use a typical hash table and hash the user id
// to one of cacheA, cacheB, or cacheC.  But with a typical hash table, if you add or remove a server,
// almost all keys will get remapped to different results, which basically could bring your service
// to a grinding halt while the caches get rebuilt.
//
// With a consistent hash, adding or removing a server drastically reduces the number of keys that
// get remapped.
//
// Read more about consistent hashing on wikipedia:  http://en.wikipedia.org/wiki/Consistent_hashing
//
package consistent

import (
	"errors"
	pb "github.com/journeymidnight/nentropy/protos"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle          map[uint32]*pb.Osd
	members         map[*pb.Osd]bool
	sortedHashes    uints
	defaultReplicas uint64
	count           uint64
	sync.RWMutex
}

// New creates a new Consistent object with a default setting of 10 replicas for weight 1.
//
// To change the number of replicas, set NumberOfReplicas before adding entries.
func New(osdMap *pb.OsdMap) *Consistent {
	c := new(Consistent)
	c.defaultReplicas = 10
	c.circle = make(map[uint32]*pb.Osd)
	c.members = make(map[*pb.Osd]bool)
	c.Set(osdMap)
	return c
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	return strconv.Itoa(idx) + elt
}

// Add inserts a osd in the consistent hash.
func (c *Consistent) Add(osd *pb.Osd) {
	c.Lock()
	defer c.Unlock()
	c.add(osd)
}

// need c.Lock() before calling
func (c *Consistent) add(osd *pb.Osd) {
	vNodeCounts := c.defaultReplicas * osd.Weight
	for i := 0; uint64(i) < vNodeCounts; i++ {
		c.circle[c.hashKey(c.eltKey(string(osd.Id), i))] = osd
	}
	c.members[osd] = true
	c.updateSortedHashes()
	c.count++
}

// Remove removes an element from the hash.
func (c *Consistent) Remove(osd *pb.Osd) {
	c.Lock()
	defer c.Unlock()
	c.remove(osd)
}

// need c.Lock() before calling
func (c *Consistent) remove(osd *pb.Osd) {
	vNodeCounts := c.defaultReplicas * osd.Weight
	for i := 0; uint64(i) < vNodeCounts; i++ {
		delete(c.circle, c.hashKey(c.eltKey(string(osd.Id), i)))
	}
	delete(c.members, osd)
	c.updateSortedHashes()
	c.count--
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elts, they will be removed.
func (c *Consistent) Set(osdMap *pb.OsdMap) {
	c.Lock()
	defer c.Unlock()
	for k := range c.members {
		found := false
		for _, v := range osdMap.MemberList {
			if k.Id == v.Id {
				found = true
				break
			}
		}
		if !found {
			c.remove(k)
		}
	}
	for _, v := range osdMap.MemberList {
		_, exists := c.members[v]
		if exists {
			continue
		}
		c.add(v)
	}
}

func (c *Consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	var m []string
	for k := range c.members {
		m = append(m, string(k.Id))
	}
	return m
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (*pb.Osd, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *Consistent) GetTwo(name string) (*pb.Osd, *pb.Osd, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, nil, ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	a := c.circle[c.sortedHashes[i]]

	if c.count == 1 {
		return a, nil, nil
	}

	start := i
	var b *pb.Osd
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		b = c.circle[c.sortedHashes[i]]
		if b != a {
			break
		}
	}
	return a, b, nil
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetN(name string, n int) ([]*pb.Osd, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}

	if c.count < uint64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]*pb.Osd, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

func (c *Consistent) hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []*pb.Osd, member *pb.Osd) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
