package main

import (
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/multiraft"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

type OsdServer struct {
	nodeID        string
	cfg           Config
	grpc          *grpc.Server
	rpcContext    *rpc.Context
	raftTransport *multiraft.RaftTransport
	stopper       *stop.Stopper
	store         *multiraft.Store
	engine        engine.Engine
}

// NewServer creates a Server from a server.Context.
func NewOsdServer(ctx context.Context, cfg Config, stopper *stop.Stopper) (*OsdServer, error) {
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
	s.grpc = rpc.NewServer()

	//init member list here
	memberlist.Init(false, (uint64)(cfg.id), cfg.AdvertiseAddr, logger.Logger)

	s.raftTransport = multiraft.NewRaftTransport(
		multiraft.GossipAddressResolver(), s.grpc, s.rpcContext,
	)

	//register osd outer rpc service for mon or client
	protos.RegisterOsdRpcServer(s.grpc, s)

	multiraftbase.RegisterInternalServer(s.grpc, s)

	eng, err := engine.NewBadgerDB()
	if err != nil {
		return nil, errors.New("Error to create db.")
	}
	s.engine = eng

	storeCfg := multiraft.StoreConfig{
		AmbientCtx: s.cfg.AmbientCtx,
		RaftConfig: s.cfg.RaftConfig,
		Transport:  s.raftTransport,
	}
	s.store = multiraft.NewStore(storeCfg, eng, &multiraftbase.NodeDescriptor{})

	if err := s.store.Start(ctx, stopper); err != nil {
		return nil, err
	}

	return s, nil
}

type ListenError struct {
	error
	Addr string
}

func (s *OsdServer) Batch(
	ctx context.Context, args *multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, error) {
	return nil, nil
}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *OsdServer) Start(ctx context.Context) error {
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
		s.grpc.Serve(ln)
	})

	return nil
}
