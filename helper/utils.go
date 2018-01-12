package helper

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"hash/crc32"
	"os"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"unsafe"
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
			Printf(5, "error to remove pidfile %s:", err)
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

// Maps the given key and value. Returns false
// if the key is already in the map and changes nothing.
func (m *SafeMap) LoadOrStore(k interface{}, v interface{}) (actual interface{}, loaded bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if val, ok := m.bm[k]; !ok {
		m.bm[k] = v
		return v, false
	} else {
		return val, true
	}
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

// EncodeUint64Ascending encodes the uint64 value using a big-endian 8 byte
// representation. The bytes are appended to the supplied buffer and
// the final buffer is returned.
func EncodeUint64Ascending(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

const (
	// The gap between floatNaNDesc and bytesMarker was left for
	// compatibility reasons.
	bytesMarker byte = 0x12
)

const (
	// <term>     -> \x00\x01
	// \x00       -> \x00\xff
	escape      byte = 0x00
	escapedTerm byte = 0x01
	escaped00   byte = 0xff
	escapedFF   byte = 0x00
)

// EncodeBytesAscending encodes the []byte value using an escape-based
// encoding. The encoded value is terminated with the sequence
// "\x00\x01" which is guaranteed to not occur elsewhere in the
// encoded value. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeBytesAscending(b []byte, data []byte) []byte {
	b = append(b, bytesMarker)
	for {
		// IndexByte is implemented by the go runtime in assembly and is
		// much faster than looping over the bytes in the slice.
		i := bytes.IndexByte(data, escape)
		if i == -1 {
			break
		}
		b = append(b, data[:i]...)
		b = append(b, escape, escaped00)
		data = data[i+1:]
	}
	b = append(b, data...)
	return append(b, escape, escapedTerm)
}

// EncodeStringAscending encodes the string value using an escape-based encoding. See
// EncodeBytes for details. The encoded bytes are append to the supplied buffer
// and the resulting buffer is returned.
func EncodeStringAscending(b []byte, s string) []byte {
	if len(s) == 0 {
		return EncodeBytesAscending(b, nil)
	}
	// We unsafely convert the string to a []byte to avoid the
	// usual allocation when converting to a []byte. This is
	// kosher because we know that EncodeBytes{,Descending} does
	// not keep a reference to the value it encodes. The first
	// step is getting access to the string internals.
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	// Next we treat the string data as a maximally sized array which we
	// slice. This usage is safe because the pointer value remains in the string.
	arg := (*[0x7fffffff]byte)(unsafe.Pointer(hdr.Data))[:len(s):len(s)]
	return EncodeBytesAscending(b, arg)
}

type Notifier struct {
	c   chan struct{}
	err error
}

func NewNotifier() *Notifier {
	return &Notifier{
		c: make(chan struct{}),
	}
}

func (nc *Notifier) Notify(err error) {
	nc.err = err
	close(nc.c)
}

func (nc *Notifier) GetChan() chan struct{} {
	return nc.c
}

func (nc *Notifier) GetErr() error {
	return nc.err
}

func GetDataDir(rootDir string, id uint64, isMon bool) (string, error) {
	var dir string
	if isMon {
		dir = fmt.Sprintf("%s/mon.%d", rootDir, id)
	} else {
		dir = fmt.Sprintf("%s/osd.%d", rootDir, id)
	}
	_, err := os.Stat(dir)
	if err == nil {
		return dir, nil
	}
	if os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			Println(0, "Cannot create data dir! err:", err)
		}
		return dir, nil
	} else {
		return "", err
	}
}
