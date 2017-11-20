package base

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"time"
)

const (
	defaultInsecure = false
	httpScheme      = "http"
	httpsScheme     = "https"

	// From IANA Service Name and Transport Protocol Port Number Registry. See
	// https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=cockroachdb
	DefaultPort = "26257"

	// The default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	// NB: net.JoinHostPort is not a constant.
	defaultAddr     = ":" + DefaultPort
	defaultHTTPAddr = ":" + DefaultHTTPPort

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultCertsDirectory is the default value for the cert directory flag.
	DefaultCertsDirectory = "${HOME}/.cockroach-certs"

	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = 200 * time.Millisecond

	// defaultRangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the
	// leader lease active duration should be of the raft election timeout.
	defaultRangeLeaseRaftElectionTimeoutMultiplier = 3
)

var defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS", 15)

// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
	// Insecure specifies whether to use SSL or not.
	// This is really not recommended.
	Insecure bool

	// SSLCAKey is used to sign new certs.
	SSLCAKey string
	// SSLCertsDir is the path to the certificate/key directory.
	SSLCertsDir string

	// User running this process. It could be the user under which
	// the server is running or the user passed in client calls.
	User string

	// Addr is the address the server is listening on.
	Addr string

	// AdvertiseAddr is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Addr is listening on.
	AdvertiseAddr string

	// HTTPAddr is server's public HTTP address.
	//
	// This is temporary, and will be removed when grpc.(*Server).ServeHTTP
	// performance problems are addressed upstream.
	//
	// See https://github.com/grpc/grpc-go/issues/586.
	HTTPAddr string
}

// InitDefaults sets up the default values for a config.
// This is also used in tests to reset global objects.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.SSLCertsDir = DefaultCertsDirectory
}

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
	if cfg.RangeLeaseRaftElectionTimeoutMultiplier == 0 {
		cfg.RangeLeaseRaftElectionTimeoutMultiplier = defaultRangeLeaseRaftElectionTimeoutMultiplier
	}
}
