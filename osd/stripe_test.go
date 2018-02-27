package main

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/nentropy/storage/engine"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

func Test_StripPutAndGetData(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	dir := fmt.Sprintf("mytest-%d", 10000+rand.Intn(10000))
	err := os.Mkdir(dir, 0755)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(dir)

	var letter = []byte("abcdefghijklmnopqrstuvwxy")
	bigBuff := make([]byte, 1024*1024)
	for i := range bigBuff {
		bigBuff[i] = letter[rand.Intn(len(letter))]
	}

	opt := engine.KVOpt{Dir: dir}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		fmt.Println("Error open badger db.")
		t.Error(err)
	}

	key := "key"
	data := bigBuff

	err = stripeWrite(eng, []byte(key), data, 0, uint64(len(data)))
	if err != nil {
		fmt.Println("Error putting data to db.")
		t.Error(err)
	}

	eng.Close()

	opt = engine.KVOpt{Dir: dir}
	eng, err = engine.NewBadgerDB(&opt)
	if err != nil {
		fmt.Println("Error open badger db.")
		t.Error(err)
	}

	key = "key"
	data, err = StripeRead(eng, []byte(key), 0, math.MaxUint64)
	if err != nil {
		fmt.Println("Error getting data to db. err", err)
		t.Error(err)
	}
	for i := 0; i < len(bigBuff); i++ {
		if data[i] != bigBuff[i] {
			t.Error(errors.New(""))
			break
		}
	}

	eng.Close()

	fmt.Println("Finished!")
}
