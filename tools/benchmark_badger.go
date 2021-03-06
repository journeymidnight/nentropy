package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/journeymidnight/nentropy/storage/engine"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

var (
	cmd      = flag.String("cmd", "", "Benchmark mode, write or read")
	dir      = flag.String("dir", "./badgerDB", "The dir to write data")
	bs       = flag.Int("bs", 4096, "Block size")
	count    = flag.Int("count", 0, "The count of blocks to write or read")
	cNum     = flag.Int("cNum", 0, "Concurrent goroutine number")
	bRunName = flag.String("bench_run_name", "", "bench run name, default:")
)

func read_data() error {
	if *cmd == "" || *count == 0 || *cNum == 0 {
		return errors.New("cmd, count, cNum must be set")
	}
	_, err := os.Stat(*dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.IsNotExist(err) {
		err = os.MkdirAll(*dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	var rCount int32
	rCount = 0
	max := (*count) / (*cNum)
	ch := make(chan struct{}, *cNum)
	f := func(idx int) error {
		dbPath := fmt.Sprintf("%s/db-%d", *dir, idx)
		opt := engine.KVOpt{Dir: dbPath}
		eng, err := engine.NewBadgerDB(&opt)
		if err != nil {
			fmt.Println("Error open badger db. err:", err)
			return err
		}
		defer eng.Close()
		for {
			seq := rand.Intn(max)
			key := fmt.Sprintf("%d-%s-%d", idx, *bRunName, seq)
			_, err = eng.Get([]byte(key))
			if err != nil {
				fmt.Println("Error getting data to badger db. err:", err)
				return err
			}
			atomic.AddInt32(&rCount, 1)
		}
		fmt.Println("goroutine idx ", idx, " exit...")
		ch <- struct{}{}
		return nil
	}

	for i := 0; i < *cNum; i++ {
		go f(i)
	}

	exitSubFunc := 0
	exit := false
	var preRCount int32 = 0
	ticker := time.NewTicker(1000 * time.Millisecond)
	for !exit {
		select {
		case <-ticker.C:
			val := atomic.LoadInt32(&rCount)
			fmt.Printf("%d ops/s, %d KB/s for write\n", val-preRCount, (val-preRCount)*(int32(*bs))/1024)
			preRCount = val
		case <-ch:
			exitSubFunc++
			if exitSubFunc == *cNum {
				val := atomic.LoadInt32(&rCount)
				fmt.Printf("%d ops/s, %d KB/s for write\n", val-preRCount, (val-preRCount)*(int32(*bs))/1024)

				exit = true
			}
		}
	}

	fmt.Println("Finished!")

	return nil
}

func write_data() error {
	if *cmd == "" || *count == 0 || *cNum == 0 {
		return errors.New("cmd, count, cNum must be set")
	}
	_, err := os.Stat(*dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.IsNotExist(err) {
		err = os.MkdirAll(*dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	var wCount int32
	wCount = 0
	prefix := rand.Int()
	max := (*count) / (*cNum)
	ch := make(chan struct{}, *cNum)
	f := func(idx int) error {
		dbPath := fmt.Sprintf("%s/db-%d", *dir, idx)
		opt := engine.KVOpt{Dir: dbPath}
		eng, err := engine.NewBadgerDB(&opt)
		if err != nil {
			fmt.Println("Error open badger db. err:", err)
			return err
		}
		defer eng.Close()
		sequence := 0
		buf := make([]byte, *bs, *bs)
		for {
			key := fmt.Sprintf("%d-%d-%d", idx, prefix, sequence)
			err = eng.Put([]byte(key), buf)
			if err != nil {
				fmt.Println("Error putting data to badger db. err:", err)
				return err
			}
			atomic.AddInt32(&wCount, 1)

			sequence++
			if sequence >= max {
				break
			}
		}
		fmt.Println("goroutine idx ", idx, " exit...")
		ch <- struct{}{}
		return nil
	}

	for i := 0; i < *cNum; i++ {
		go f(i)
	}

	exitSubFunc := 0
	exit := false
	var preWCount int32 = 0
	ticker := time.NewTicker(1000 * time.Millisecond)
	for !exit {
		select {
		case <-ticker.C:
			val := atomic.LoadInt32(&wCount)
			fmt.Printf("%d ops/s, %d KB/s for write\n", val-preWCount, (val-preWCount)*(int32(*bs))/1024)
			preWCount = val
		case <-ch:
			exitSubFunc++
			if exitSubFunc == *cNum {
				val := atomic.LoadInt32(&wCount)
				fmt.Printf("%d ops/s, %d KB/s for write\n", val-preWCount, (val-preWCount)*(int32(*bs))/1024)

				exit = true
			}
		}
	}

	fmt.Println("Finished! runname:", prefix)

	return nil
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	switch *cmd {
	case "write":
		err := write_data()
		if err != nil {
			fmt.Println("Error writing data. err:", err)
		}
	case "read":
		err := read_data()
		if err != nil {
			fmt.Println("Error reading data. err:", err)
		}
	default:
		fmt.Println("Unsupported command! ")
	}

	return
}
