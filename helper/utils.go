package helper

import (
	"crypto/subtle"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"sync"
	"syscall"
)

func CreatePidfile(pidFile string) error {
	if pidFile != "" {
		fmt.Println(6, "enter 11:")
		if err := WritePid(pidFile); err != nil {
			return err
		}
	}
	return nil
}

func RemovePidfile(pidFile string) {
	if pidFile != "" {
		if err := os.Remove(pidFile); err != nil {
			Logger.Printf(5, "error to remove pidfile %s:", err)
		}
	}
}

func WritePid(pidfile string) error {
	var file *os.File
	fmt.Println(6, "enter 12:")
	if _, err := os.Stat(pidfile); os.IsNotExist(err) {
		fmt.Println(6, "enter 14:")
		if file, err = os.Create(pidfile); err != nil {
			return err
		}
	} else {
		fmt.Println(6, "enter 15:")
		if file, err = os.OpenFile(pidfile, os.O_RDWR, 0); err != nil {
			return err
		}
		pidstr := make([]byte, 8)

		n, err := file.Read(pidstr)
		if err != nil {
			return err
		}
		fmt.Println(6, "enter 16:")
		if n > 0 {
			pid, err := strconv.Atoi(string(pidstr[:n]))
			if err != nil {
				fmt.Printf("err: %s, overwriting pidfile", err)
			}
			fmt.Println(6, "enter 17:")
			process, _ := os.FindProcess(pid)
			fmt.Println(6, "enter 19:")
			if err = process.Signal(syscall.Signal(0)); err == nil {
				fmt.Println(6, "enter 20:")
				return fmt.Errorf("pid: %d is running", pid)
			} else {
				fmt.Println(6, "enter 21:")
				fmt.Printf("err: %s, cleanup pidfile", err)
			}
			fmt.Println(6, "enter 18:")
			if file, err = os.Create(pidfile); err != nil {
				return err
			}

		}

	}
	defer file.Close()
	fmt.Println(6, "enter 13:")
	pid := strconv.Itoa(os.Getpid())
	fmt.Fprintf(file, "%s", pid)
	return nil
}

// From https://github.com/codegangsta/martini-contrib/blob/master/auth/util.go
// SecureCompare performs a constant time compare of two strings to limit timing attacks.
func SecureCompare(given string, actual string) bool {
	if subtle.ConstantTimeEq(int32(len(given)), int32(len(actual))) == 1 {
		return subtle.ConstantTimeCompare([]byte(given), []byte(actual)) == 1
	} else {
		/* Securely compare actual to itself to keep constant time, but always return false */
		return subtle.ConstantTimeCompare([]byte(actual), []byte(actual)) == 1 && false
	}
}

type SafeMap struct {
	lock *sync.RWMutex
	bm   map[interface{}]interface{}
}

// NewSafeMap return new safemap
func NewSafeMap() *SafeMap {
	return &SafeMap{
		lock: new(sync.RWMutex),
		bm:   make(map[interface{}]interface{}),
	}
}

// Get from maps return the k's value
func (m *SafeMap) Get(k interface{}) interface{} {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if val, ok := m.bm[k]; ok {
		return val
	}
	return nil
}

// Maps the given key and value. Returns false
// if the key is already in the map and changes nothing.
func (m *SafeMap) Set(k interface{}, v interface{}) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if val, ok := m.bm[k]; !ok {
		m.bm[k] = v
	} else if val != v {
		m.bm[k] = v
	} else {
		return false
	}
	return true
}

// Returns true if k is exist in the map.
func (m *SafeMap) Check(k interface{}) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, ok := m.bm[k]; !ok {
		return false
	}
	return true
}

// Delete the given key and value.
func (m *SafeMap) Delete(k interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.bm, k)
}

// Items returns all items in safemap.
func (m *SafeMap) Items() map[interface{}]interface{} {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.bm
}

func HashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}
