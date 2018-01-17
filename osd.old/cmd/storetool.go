package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/journeymidnight/nentropy/store"
)

var action = flag.String("action", "", "action to store, support: put, get")
var dir = flag.String("dir", "", "dir of the badger store")
var oid = flag.String("oid", "", "oid of an object")
var value = flag.String("value", "", "value of the object")

func main() {
	flag.Parse()
	if *action == "" {
		fmt.Println("please provide action name")
		os.Exit(-1)
	}

	if *dir == "" {
		fmt.Println("please provide the dir")
		os.Exit(-1)
	}

	coll, err := store.NewCollection(*dir)
	if err != nil {
		if err == store.ErrDirNotExists {
			fmt.Println("the dir is not exist")
			os.Exit(-1)
		} else {
			fmt.Println("failed to open collection, maybe it's locked by other daemons")
			os.Exit(-1)
		}
	}

	defer coll.Close()
	if *action == "put" {
		fmt.Println("not done yet, wait a moment")
		os.Exit(-1)
	} else if *action == "get" {
		value, err := coll.Get([]byte(*oid))
		if err != nil || len(value) == 0 {
			fmt.Println("key doesno't exist, error is ", err, value)
			os.Exit(-1)
		}

		fmt.Println(value)
	} else {
		fmt.Println("action not supported")
		os.Exit(-1)
	}
}
