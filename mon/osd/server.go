package main

import (
	"crypto/tls"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/multiraft"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type OsdServer struct {
	nodeID        string
	cfg           Config
	grpc          *grpc.Server
	rpcContext    *rpc.Context
	raftTransport *multiraft.RaftTransport
	stopper       *stop.Stopper
	store         *multiraft.Store
}

// NewServer creates a Server from a server.Context.
func NewOsdServer(cfg Config, stopper *stop.Stopper) (*OsdServer, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	s := &OsdServer{
		stopper: stopper,
		cfg:     cfg,
	}

	// Add a dynamic log tag value for the node ID.
	//
	// We need to pass an ambient context to the various server components, but we
	// won't know the node ID until we Start(). At that point it's too late to
	// change the ambient contexts in the components (various background processes
	// will have already started using them).
	//
	// NodeIDContainer allows us to add the log tag to the context now and update
	// the value asynchronously. It's not significantly more expensive than a
	// regular tag since it's just doing an (atomic) load when a log/trace message
	// is constructed. The node ID is set by the Store if this host was
	// bootstrapped; otherwise a new one is allocated in Node.
	s.rpcContext = rpc.NewContext(s.cfg.Config, s.stopper)
	s.grpc = rpc.NewServer(s.rpcContext)

	//init member list here
	memberlist.Init(false, (uint64)(cfg.id), cfg.AdvertiseAddr, logger.Logger)

	s.raftTransport = multiraft.NewRaftTransport(
		multiraft.GossipAddressResolver(), s.grpc, s.rpcContext,
	)

	//register osd outer rpc service for mon or client
	protos.RegisterOsdRpcServer(s.grpc, s)

	//// TODO(bdarnell): make StoreConfig configurable.
	//storeCfg := storage.StoreConfig{
	//	Settings:                st,
	//	AmbientCtx:              s.cfg.AmbientCtx,
	//	RaftConfig:              s.cfg.RaftConfig,
	//	Clock:                   s.clock,
	//	DB:                      s.db,
	//	Gossip:                  s.gossip,
	//	NodeLiveness:            s.nodeLiveness,
	//	Transport:               s.raftTransport,
	//	RPCContext:              s.rpcContext,
	//	ScanInterval:            s.cfg.ScanInterval,
	//	ScanMaxIdleTime:         s.cfg.ScanMaxIdleTime,
	//	MetricsSampleInterval:   s.cfg.MetricsSampleInterval,
	//	HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
	//	StorePool:               s.storePool,
	//	SQLExecutor:             sqlExecutor,
	//	LogRangeEvents:          s.cfg.EventLogEnabled,
	//	TimeSeriesDataStore:     s.tsDB,
	//
	//	EnableEpochRangeLeases: true,
	//}
	//if storeTestingKnobs := s.cfg.TestingKnobs.Store; storeTestingKnobs != nil {
	//	storeCfg.TestingKnobs = *storeTestingKnobs.(*storage.StoreTestingKnobs)
	//}
	//
	//s.recorder = status.NewMetricsRecorder(s.clock, s.nodeLiveness, s.rpcContext.RemoteClocks, s.gossip)
	//s.registry.AddMetricStruct(s.rpcContext.RemoteClocks.Metrics())
	//
	//s.runtime = status.MakeRuntimeStatSampler(s.clock)
	//s.registry.AddMetricStruct(s.runtime)
	//
	//s.node = NewNode(storeCfg, s.recorder, s.registry, s.stopper, txnMetrics, sql.MakeEventLogger(s.leaseMgr))
	//roachpb.RegisterInternalServer(s.grpc, s.node)
	//storage.RegisterConsistencyServer(s.grpc, s.node.storesServer)
	//serverpb.RegisterInitServer(s.grpc, &noopInitServer{clusterID: s.ClusterID})

	return s, nil
}

type ListenError struct {
	error
	Addr string
}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *OsdServer) Start(ctx context.Context) error {

	// The following code is a specialization of util/net.go's ListenAndServe
	// which adds pgwire support. A single port is used to serve all protocols
	// (pg, http, h2) via the following construction:
	//
	// non-TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// Note that the difference between the TLS and non-TLS cases exists due to
	// Go's lack of an h2c (HTTP2 Clear Text) implementation. See inline comments
	// in util.ListenAndServe for an explanation of how h2c is implemented there
	// and here.

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return ListenError{
			error: err,
			Addr:  s.cfg.Addr,
		}
	}
	helper.Logger.Println(5, "listening on port %s", s.cfg.Addr)
	workersCtx := context.Background()
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		<-s.stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		ln.Close()
		<-s.stopper.ShouldStop()
		s.grpc.Stop()
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(ln))
	})

	s.engines, err = s.cfg.CreateEngines(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

	// We ran this before, but might've bootstrapped in the meantime. This time
	// we'll get the actual list of bootstrapped and empty engines.
	bootstrappedEngines, emptyEngines, cv, err := inspectEngines(ctx, s.engines, s.cfg.Settings.Version.MinSupportedVersion, s.cfg.Settings.Version.ServerVersion)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been bootstrapped or
	// we're joining an existing cluster for the first time.
	err = s.node.start(
		ctx,
		unresolvedAdvertAddr,
		bootstrappedEngines, emptyEngines,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		cv,
	)

	if err != nil {
		return err
	}

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent()

	if s.cfg.PIDFile != "" {
		if err := ioutil.WriteFile(s.cfg.PIDFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Error(ctx, err)
		}
	}

	if s.cfg.ListeningURLFile != "" {
		pgURL, err := s.cfg.PGURL(url.User(security.RootUser))
		if err == nil {
			err = ioutil.WriteFile(s.cfg.ListeningURLFile, []byte(fmt.Sprintf("%s\n", pgURL)), 0644)
		}

		if err != nil {
			log.Error(ctx, err)
		}
	}

	if err := sdnotify.Ready(); err != nil {
		log.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
	}
	log.Event(ctx, "server ready")

	return nil
}
