package multiraft

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"time"
)

const (
	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = 200 * time.Millisecond
)

var defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS", 15)

type RaftConfig struct {
	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// RaftElectionTimeoutTicks is the number of raft ticks before the
	// previous election expires. This value is inherited by individual stores
	// unless overridden.
	RaftElectionTimeoutTicks int
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
