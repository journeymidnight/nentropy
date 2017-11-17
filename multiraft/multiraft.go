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

type StoreConfig struct {
	Transport *RaftTransport
	RaftConfig
	RaftHeartbeatIntervalTicks int
}

type Store struct {
	cfg StoreConfig
	mu  struct {
		sync.Mutex
		replicas map[multiraftbase.GroupID]*Replica
	}
	started           int32
	stopper           *stop.Stopper
	raftRequestQueues map[multiraftbase.GroupID]*raftRequestQueue
	scheduler         *raftScheduler
}

func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()
}

func NewStore(cfg StoreConfig) *Store {
	cfg.SetDefaults()
	s := &Store{
		cfg: cfg,
	}
	s.scheduler = newRaftScheduler(s, schedulerConcurrency)
	s.mu.Lock()
	s.mu.replicas = map[multiraftbase.GroupID]*Replica{}
	s.mu.Unlock()
	return s
}

func (s *Store) processReady(ctx context.Context, id multiraftbase.GroupID) {
	return
}

func (s *Store) processRequestQueue(ctx context.Context, id multiraftbase.GroupID) {
	return
}

func (s *Store) processTick(ctx context.Context, id multiraftbase.GroupID) bool {
	return true
}
