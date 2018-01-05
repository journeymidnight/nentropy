package main

import (
	"fmt"
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
	"sync"
	"time"
)

type OsdServer struct {
	nodeID          string
	cfg             Config
	grpc            *grpc.Server
	rpcContext      *rpc.Context
	raftTransport   *multiraft.RaftTransport
	stopper         *stop.Stopper
	store           *multiraft.Store
	engine          engine.Engine
	pgMaps          *protos.PgMaps
	mapLock         *sync.Mutex
	confChangeLock  *sync.Mutex
	lastPgMapsEpoch uint64
	storeCfg        multiraft.StoreConfig // Config to use and pass to stores
}

// NewServer creates a Server from a server.Context.
func NewOsdServer(ctx context.Context, cfg Config, stopper *stop.Stopper) (*OsdServer, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	s := &OsdServer{
		stopper: stopper,
		cfg:     cfg,
		nodeID:  fmt.Sprintf("%d.%d", cfg.NodeType, cfg.NodeID),
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
	s.rpcContext = rpc.NewContext(&s.cfg.Config, s.stopper)
	s.grpc = rpc.NewServer()

	//init member list here
	memberlist.Init(false, (uint64)(cfg.NodeID), cfg.AdvertiseAddr, logger.Logger, cfg.JoinMemberAddr)

	s.raftTransport = multiraft.NewRaftTransport(
		s.cfg.AmbientCtx,
		multiraft.GossipAddressResolver(), s.grpc, s.rpcContext,
	)

	//register osd outer rpc service for mon or client
	protos.RegisterOsdRpcServer(s.grpc, s)

	multiraftbase.RegisterInternalServer(s.grpc, s)

	opt := engine.KVOpt{WALDir: config.WALDir}
	eng, err := engine.NewBadgerDB(&opt)
	if err != nil {
		return nil, errors.New("Error to create db.")
	}
	s.engine = eng

	storeCfg := multiraft.StoreConfig{
		AmbientCtx:                  s.cfg.AmbientCtx,
		RaftConfig:                  s.cfg.RaftConfig,
		Transport:                   s.raftTransport,
		CoalescedHeartbeatsInterval: 50 * time.Millisecond,
		RaftHeartbeatIntervalTicks:  1,
	}
	desc := multiraftbase.NodeDescriptor{}
	//if s.cfg.NodeID == 1 {
	//	desc.NodeID = "1"
	//} else if s.cfg.NodeID == 2 {
	//	desc.NodeID = "2"
	//} else if s.cfg.NodeID == 3 {
	//	desc.NodeID = "3"
	//}
	desc.NodeID = multiraftbase.NodeID(fmt.Sprintf("%s.%d", s.cfg.NodeType, s.cfg.NodeID))
	helper.Logger.Println(0, "New osd server nodeid: ", s.cfg.NodeID)
	s.store = multiraft.NewStore(storeCfg, eng, &desc)

	if err := s.store.Start(ctx, stopper); err != nil {
		return nil, err
	}

	return s, nil
}

type ListenError struct {
	error
	Addr string
}

func (s *OsdServer) batchInternal(
	ctx context.Context, args *multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, error) {

	var br *multiraftbase.BatchResponse

	if err := s.stopper.RunTaskWithErr(ctx, "node.Node: batch", func(ctx context.Context) error {

		var pErr *multiraftbase.Error
		br, pErr = s.store.Send(ctx, *args)
		if pErr != nil {
			br = &multiraftbase.BatchResponse{}
			helper.Logger.Printf(5, "%T", pErr.GetDetail())
		}
		if br.Error != nil {
			helper.Logger.Panicln(0, "unexpectedly error. error:", br.Error)
		}
		br.Error = pErr
		return nil
	}); err != nil {
		return nil, err
	}
	return br, nil
}

func (s *OsdServer) Batch(
	ctx context.Context, args *multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, error) {

	helper.Logger.Println(0, "Get a batch request! GroupID:", args.GroupID)

	// NB: Node.Batch is called directly for "local" calls. We don't want to
	// carry the associated log tags forward as doing so makes adding additional
	// log tags more expensive and makes local calls differ from remote calls.
	ctx = s.storeCfg.AmbientCtx.ResetAndAnnotateCtx(ctx)

	br, err := s.batchInternal(ctx, args)

	// We always return errors via BatchResponse.Error so structure is
	// preserved; plain errors are presumed to be from the RPC
	// framework and not from cockroach.
	if err != nil {
		if br == nil {
			br = &multiraftbase.BatchResponse{}
		}
		if br.Error != nil {
			helper.Logger.Fatalf(
				5, "attempting to return both a plain error (%s) and roachpb.Error (%s)", err, br.Error,
			)
		}
		br.Error = multiraftbase.NewError(err)
	}

	return br, nil
}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *OsdServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Config.AdvertiseAddr)
	if err != nil {
		return ListenError{
			error: err,
			Addr:  s.cfg.Config.AdvertiseAddr,
		}
	}
	workersCtx := context.Background()
	//s.stopper.RunWorker(workersCtx, func(context.Context) {
	//	<-s.stopper.ShouldQuiesce()
	//	// TODO(bdarnell): Do we need to also close the other listeners?
	//	ln.Close()
	//	<-s.stopper.ShouldStop()
	//	s.grpc.Stop()
	//})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		s.grpc.Serve(ln)
	})

	//replicas :=
	//	[]multiraftbase.ReplicaDescriptor{
	//		{
	//			NodeID:    "1",
	//			StoreID:   1,
	//			ReplicaID: 1,
	//		},
	//		{
	//			NodeID:    "2",
	//			StoreID:   2,
	//			ReplicaID: 2,
	//		},
	//	}
	//groupDesc := multiraftbase.GroupDescriptor{
	//	GroupID:       "1",
	//	PoolId:        1,
	//	Replicas:      replicas,
	//	NextReplicaID: 4}
	//s.store.BootstrapGroup(nil, &groupDesc)

	return nil
}
