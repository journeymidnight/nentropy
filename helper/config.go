package helper

import (
	"encoding/json"
	"math/rand"
	"os"
)

func Ternary(IF bool, THEN interface{}, ELSE interface{}) interface{} {
	if IF {
		return THEN
	} else {
		return ELSE
	}
}

// Static alphaNumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

var NumericTable = []byte("0123456789")

func GenerateRandomId() []byte {
	alpha := make([]byte, 16, 16)
	for i := 0; i < 16; i++ {
		n := rand.Intn(len(alphaNumericTable))
		alpha[i] = alphaNumericTable[n]
	}
	return alpha
}

func GenerateRandomIdByLength(length int) []byte {
	alpha := make([]byte, length, length)
	for i := 0; i < length; i++ {
		n := rand.Intn(len(alphaNumericTable))
		alpha[i] = alphaNumericTable[n]
	}
	return alpha
}

func GenerateRandomNumberId() []byte {
	alpha := make([]byte, 16, 16)
	for i := 0; i < 16; i++ {
		n := rand.Intn(len(NumericTable))
		alpha[i] = NumericTable[n]
	}
	return alpha
}

type Options struct {
	LogPath             string
	PanicLogPath        string
	PidFile             string
	DebugMode           bool
	LogLevel            int //1-20
	Monitors            string
	WALDir              string
	WorkerPort          int
	Join                bool
	NumPendingProposals int
	Tracing             float64
	PeerAddr            string
	MyAddr              string
	RaftId              uint64
	MaxPendingCount     uint64
	HttpPort            int
}

var DefaultOption = Options{
	LogPath:             "/var/log/nentropy/nentropy.log",
	PanicLogPath:        "/var/log/nentropy/panic.log",
	PidFile:             "/var/run/nentropy/nentropy.pid",
	DebugMode:           false,
	LogLevel:            5,
	Monitors:            "",
	WALDir:              "w",
	Join:                false,
	WorkerPort:          12345,
	NumPendingProposals: 2000,
	Tracing:             0.0,
	PeerAddr:            "",
	MyAddr:              "",
	RaftId:              1,
	MaxPendingCount:     1000,
	HttpPort:            8080,
}

var CONFIG Options

func SetupConfig() {
	f, err := os.Open("/etc/nentropy/nentropy.json")
	if err != nil {
		panic("Cannot open nentropy.json")
	}
	defer f.Close()

	defaults := DefaultOption
	var config Options
	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		panic("Failed to parsenentropy.json: " + err.Error())
	}

	// setup CONFIG with defaults
	defaults.LogPath = config.LogPath
	defaults.PanicLogPath = config.PanicLogPath
	defaults.PidFile = config.PidFile
	defaults.DebugMode = config.DebugMode
	defaults.LogLevel = Ternary(config.LogLevel == 0, 5, config.LogLevel).(int)
	defaults.Monitors = config.Monitors
	CONFIG = defaults
}
