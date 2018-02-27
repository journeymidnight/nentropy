package main

import (
	"fmt"
	"github.com/journeymidnight/nentropy/storage/engine"
	"io/ioutil"
	//"math"
	"math"
	"os"
	"testing"
)

func Test_StripPutData(t *testing.T) {
	opt := engine.KVOpt{Dir: "./test"}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		fmt.Println("Error open badger db.")
		t.Error(err)
	}

	key := "key"
	filename := "./testfile"

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("Error reading local file, err:", err)
		t.Error(err)
	}

	err = stripeWrite(eng, []byte(key), data, 0, uint64(len(data)))
	if err != nil {
		fmt.Println("Error putting data to db.")
		t.Error(err)
	}

	eng.Close()

	fmt.Println("Finished!")
}

func Test_StripGetData(t *testing.T) {
	opt := engine.KVOpt{Dir: "./test"}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		fmt.Println("Error open badger db.")
		t.Error(err)
	}

	key := "key"
	data, err := StripeRead(eng, []byte(key), 0, math.MaxUint64)
	if err != nil {
		fmt.Println("Error getting data to db. err", err)
		t.Error(err)
	}
	fmt.Println("stripRead ret lengh:", len(data))
	err = ioutil.WriteFile("testfile.res", data, os.ModePerm)
	if err != nil {
		fmt.Println("Error writing local file, err:", err)
		t.Error(err)
	}

	eng.Close()

	//fmt.Println("Finished! data:", data)
}
