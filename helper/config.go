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

type Config struct {
	LogPath      string
	PanicLogPath string
	PidFile      string
	DebugMode    bool
	LogLevel     int //1-20
}

type config struct {
	LogPath      string
	PanicLogPath string
	PidFile      string
	DebugMode    bool
	LogLevel     int //1-20
}

var CONFIG Config

func SetupConfig() {
	f, err := os.Open("/etc/nentropy.json")
	if err != nil {
		panic("Cannot open nentropy.json")
	}
	defer f.Close()

	var c config
	err = json.NewDecoder(f).Decode(&c)
	if err != nil {
		panic("Failed to parsenentropy.json: " + err.Error())
	}

	// setup CONFIG with defaults
	CONFIG.LogPath = c.LogPath
	CONFIG.PanicLogPath = c.PanicLogPath
	CONFIG.PidFile = c.PidFile
	CONFIG.DebugMode = c.DebugMode
	CONFIG.LogLevel = Ternary(c.LogLevel == 0, 5, c.LogLevel).(int)
}
