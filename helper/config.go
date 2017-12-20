package helper

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/util/envutil"
	"math/rand"
	"os"
	"time"
)

const (
	// The default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	defaultHTTPAddr = ":" + DefaultHTTPPort

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = 200 * time.Millisecond
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
	LogPath             string
	PanicLogPath        string
	PidFile             string
	DebugMode           bool
	LogLevel            int //1-20
	WALDir              string
	MonPort             int
	JoinMon             bool
	NumPendingProposals int
	Tracing             float64
	Monitors            string
	MyAddr              string
	RaftId              uint64
	MaxPendingCount     uint64
	MemberBindPort      int
	JoinMemberAddr      string
	NodeID              int
	NodeType            string

	// AdvertiseAddr is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Addr is listening on.
	AdvertiseAddr string
	HTTPAddr      string
}

var DefaultConfig = Config{
	LogPath:             "/var/log/nentropy/nentropy.log",
	PanicLogPath:        "/var/log/nentropy/panic.log",
	PidFile:             "/var/run/nentropy/nentropy.pid",
	DebugMode:           false,
	LogLevel:            10,
	WALDir:              "w",
	JoinMon:             false,
	MonPort:             7900,
	NumPendingProposals: 2000,
	Tracing:             0.0,
	Monitors:            "",
	MyAddr:              "",
	RaftId:              1,
	MaxPendingCount:     1000,
	MemberBindPort:      7946,
	JoinMemberAddr:      "",
}

var CONFIG Config

func (c *Config) parseCmdArgs() {

	flag.StringVar(&c.WALDir, "w", DefaultConfig.WALDir,
		"Directory to store raft write-ahead logs.")
	flag.IntVar(&c.MonPort, "monPort", DefaultConfig.MonPort,
		"Port used by mon for internal communication.")
	flag.StringVar(&c.Monitors, "mons", DefaultConfig.Monitors,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.IntVar(&c.MemberBindPort, "memberBindPort", 0,
		"Port used by memberlist for internal communication.")
	flag.StringVar(&c.JoinMemberAddr, "joinMemberAddr", DefaultConfig.JoinMemberAddr,
		"a valid member addr to join.")
	flag.IntVar(&c.NodeID, "nodeID", DefaultConfig.NodeID,
		"a unique numbers in cluster [1-256]")
	flag.StringVar(&c.NodeType, "nodeType", DefaultConfig.NodeType,
		"specify node type [osd/mon].")
	flag.StringVar(&c.AdvertiseAddr, "advertiseAddr", "",
		"specify rpc listen address, like [10.11.11.11:8888]")

	flag.Parse()
	if !flag.Parsed() {
		Logger.Fatal(0, "Unable to parse flags")
	}
	//TODO: add argument check here

	c.LogPath = fmt.Sprintf("/var/log/nentropy/%s.%d.log", c.NodeType, c.NodeID)
	c.PidFile = fmt.Sprintf("/var/run/nentropy/%s.%d.pid", c.NodeType, c.NodeID)
	c.PanicLogPath = fmt.Sprintf("/var/log/nentropy/%s.%d.panic.log", c.NodeType, c.NodeID)

}

func (c *Config) InitConfig() {
	cfgPath := "/etc/nentropy/nentropy.json"
	defaults := DefaultConfig
	f, err := os.OpenFile(cfgPath, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("Failed to open configuration file.\n")
	} else {
		defer f.Close()
		err = json.NewDecoder(f).Decode(&defaults)
		if err != nil {
			panic("Failed to parse nentropy.json: " + err.Error())
		}
	}

	defaults.parseCmdArgs()

	f, err = os.OpenFile(c.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		Logger = log.New(os.Stdout, "[nentropy]", log.LstdFlags, defaults.LogLevel)
		Logger.Printf(0, "Failed to open log file %s, use stdout!", c.LogPath)
	} else {
		defer f.Close()
		Logger = log.New(f, "[nentropy]", log.LstdFlags, defaults.LogLevel)
	}
	*c = defaults
}

var defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt(
	"NENTROPY_RAFT_ELECTION_TIMEOUT_TICKS", 5)

// RaftConfig holds raft tuning parameters.
type RaftConfig struct {
	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// RaftElectionTimeoutTicks is the number of raft ticks before the
	// previous election expires. This value is inherited by individual stores
	// unless overridden.
	RaftElectionTimeoutTicks int

	// RangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the leader
	// lease active duration should be of the raft election timeout.
	RangeLeaseRaftElectionTimeoutMultiplier float64
}

// SetDefaults initializes unset fields.
func (cfg *RaftConfig) SetDefaults() {
	if cfg.RaftTickInterval == 0 {
		cfg.RaftTickInterval = defaultRaftTickInterval
	}
	if cfg.RaftElectionTimeoutTicks == 0 {
		cfg.RaftElectionTimeoutTicks = defaultRaftElectionTimeoutTicks
	}
}
