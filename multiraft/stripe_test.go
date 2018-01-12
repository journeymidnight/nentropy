package multiraft

import (
	"fmt"
	"github.com/journeymidnight/nentropy/storage/engine"
	"testing"
)

func Test_StripPutData(t *testing.T) {
	opt := engine.KVOpt{Dir: "./test"}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		fmt.Println("Error open badger db.")
		t.Error(err)
	}

	key := "key1"
	val := "val1"

	err = stripeWrite(eng, []byte(key), []byte(val), 0, uint64(len(val)))
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

	key := "key1"
	data, err := stripeRead(eng, []byte(key), 0, uint64(0xffffffff))
	if err != nil {
		fmt.Println("Error putting data to db.")
		t.Error(err)
	}

	eng.Close()

	fmt.Println("Finished! data:", data)
}
