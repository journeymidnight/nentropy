package multiraft

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"golang.org/x/net/context"
	"runtime"
	"sync"
)

const ()

var schedulerConcurrency = envutil.EnvOrDefaultInt(
	"NENTROPY_SCHEDULER_CONCURRENCY", 8*runtime.NumCPU())

type RaftContainerConfig struct {
	Transport *RaftTransport
	RaftConfig
	RaftHeartbeatIntervalTicks int
}

type RaftContainer struct {
	cfg RaftContainerConfig
	mu  struct {
		sync.Mutex
		replicas map[multiraftbase.GroupID]*Replica
	}
	started           int32
	stopper           *stop.Stopper
	raftRequestQueues map[multiraftbase.GroupID]*raftRequestQueue
	scheduler         *raftScheduler
}

func (rcc *RaftContainerConfig) SetDefaults() {
	rcc.RaftConfig.SetDefaults()
}

func NewRaftContainer(cfg RaftContainerConfig) *RaftContainer {
	cfg.SetDefaults()
	rc := &RaftContainer{
		cfg: cfg,
	}
	rc.scheduler = newRaftScheduler(rc, schedulerConcurrency)
	rc.mu.Lock()
	rc.mu.replicas = map[multiraftbase.GroupID]*Replica{}
	rc.mu.Unlock()
	return rc
}

func (rc *RaftContainer) processReady(ctx context.Context, id multiraftbase.GroupID) {
	return
}

func (rc *RaftContainer) processRequestQueue(ctx context.Context, id multiraftbase.GroupID) {
	return
}

func (rc *RaftContainer) processTick(ctx context.Context, id multiraftbase.GroupID) bool {
	return true
}
