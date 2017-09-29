package osd

import (
	"log"
	"os"

	"github.com/journeymidnight/nentropy/store"
)

// this file mainly about describe the variable stored on osd start

// Collmap is in-memory map stores all Collections
type Collmap struct {
	collections map[string]*store.Collection
}

//collmap stores all collections
func getCollMap() *Collmap {
	return &Collmap{collections: make(map[string]*store.Collection)}
}

var collmap = getCollMap()

var logger = log.New(os.Stdout, "osd::init", log.Ldate)

func openMetaCollection() *store.Collection {
	coll, err := store.NewCollection("meta")
	if err != nil {
		logger.Fatal("failed to create meta collection")
	}

	return coll
}

func openExistingCollections() {
	metacoll := openMetaCollection()

	pgs, err := metacoll.Get(MetaPGKey)
	if err != nil {
		logger.Fatal("failed to read meta collection")
	}

	if len(pgs) == 0 {
		logger.Println("no existing data pgs")
		return
	}

	//it := metacoll.NewIterator()

}
