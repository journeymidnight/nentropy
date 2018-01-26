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
	nodeID            string
	cfg               Config
	grpc              *grpc.Server
	rpcContext        *rpc.Context
	raftTransport     *multiraft.RaftTransport
	stopper           *stop.Stopper
	store             *multiraft.Store
	engine            engine.Engine
	pgMaps            *protos.PgMaps
	leaderPgStatusMap sync.Map //map[string]protos.PgStatus
	mapLock           *sync.Mutex
	confChangeLock    *sync.Mutex
	lastPgMapsEpoch   uint64
	storeCfg          multiraft.StoreConfig // Config to use and pass to stores
}

const OSD_STATUS_REPORT_PERIOD = 2 * time.Second

func getOSDDataDir(baseDir string, id int) string {
	return fmt.Sprintf("%s/osd.%d", baseDir, id)
}

func getOsdMap() (*protos.OsdMap, error) {
	mon := memberlist.GetLeaderMon()
	helper.Println(5, "Connect to mon ", mon.Addr)
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure())
	if err != nil {
		helper.Printf(5, "fail to dial: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	req := protos.OsdConfigRequest{"", &protos.Osd{}, protos.OsdConfigRequest_LIST}
	reply, err := client.OsdConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list osds error: ", err)
		return nil, err
	}
	return reply.Map, nil
}

func getPgWorkDir(string multiraftbase.GroupID) (string, error) {
	return "", nil
}

func getPgMaps() (*protos.PgMaps, error) {
	mon := memberlist.GetLeaderMon()
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure())
	if err != nil {
		helper.Printf(5, "fail to dial: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	req := protos.PgConfigRequest{"", protos.PgConfigRequest_LIST, "", 0}
	reply, err := client.PgConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list osds error: ", err)
		return nil, err
	}
	return reply.Maps, nil
}

func getOsdSysDataDir() (string, error) {
	dir, err := helper.GetDataDir(config.BaseDir, uint64(config.NodeID), false)
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	return dir + "/sys-data", nil
}

// NewServer creates a Server from a server.Context.
func NewOsdServer(ctx context.Context, cfg Config, stopper *stop.Stopper) (*OsdServer, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	s := &OsdServer{
		stopper:        stopper,
		cfg:            cfg,
		nodeID:         fmt.Sprintf("%d.%d", cfg.NodeType, cfg.NodeID),
		confChangeLock: &sync.Mutex{},
		mapLock:        &sync.Mutex{},
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
	rpcPort := Listener.Addr().(*net.TCPAddr).Port
	advertiseAddr := memberlist.GetMyIpAddress(rpcPort)
	memberlist.Init(false, false, (uint64)(cfg.NodeID), advertiseAddr, cfg.MemberBindPort, logger.Logger, cfg.JoinMemberAddr)

	//get osd map
	osdmap, err := getOsdMap()
	if err != nil {
		return nil, err
	}

	//check if osd not existed in osd map
	found := false
	for k, _ := range osdmap.MemberList {
		if k == int32(cfg.NodeID) {
			found = true
			break
		}
	}
	if found == false {
		memberlist.List.Shutdown()
		return nil, errors.New(fmt.Sprintf("Invaid osd id %d, should create osd first", cfg.NodeID))
	}

	//get pgsmap
	pgmaps, err := getPgMaps()
	if err != nil {
		return nil, err
	}
	s.pgMaps = pgmaps

	//TODO: load existed pgs

	//i := 1
	//for i <= 100 {
	//	mon := memberlist.GetPrimaryMon()
	//	if mon != nil {
	//		helper.Println(5, "got primary mon", mon)
	//	}
	//	time.Sleep(1 * time.Second)
	//	i = i + 1
	//}

	s.raftTransport = multiraft.NewRaftTransport(
		s.cfg.AmbientCtx,
		multiraft.GossipAddressResolver(), s.grpc, s.rpcContext,
	)

	//register osd outer rpc service for mon or client
	protos.RegisterOsdRpcServer(s.grpc, s)

	multiraftbase.RegisterInternalServer(s.grpc, s)

	dir, err := getOsdSysDataDir()
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	opt := engine.KVOpt{Dir: dir}
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
		NodeID:  config.NodeID,
		BaseDir: config.BaseDir,
	}
	desc := multiraftbase.NodeDescriptor{}
	desc.NodeID = multiraftbase.NodeID(fmt.Sprintf("%s.%d", s.cfg.NodeType, s.cfg.NodeID))
	helper.Println(0, "New osd server nodeid: ", s.cfg.NodeID)
	s.store = multiraft.NewStore(storeCfg, eng, &desc, UpdatePgStatusMap)

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
			helper.Printf(5, "%T", pErr.GetDetail())
		}
		if br.Error != nil {
			helper.Panicln(0, "unexpectedly error. error:", br.Error)
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

	helper.Println(0, "Get a batch request! GroupID:", args.GroupID)

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
			helper.Fatalf("attempting to return both a plain error (%s) and roachpb.Error (%s)", err, br.Error)
		}
		br.Error = multiraftbase.NewError(err)
	}

	return br, nil
}

func (s *OsdServer) sendOsdStatusToMon(addr string) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		helper.Println(5, "fail to dial: %v", err)
		return
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	groups, _ := s.store.GetGroupIdsByLeader()
	req := protos.OsdStatusReportRequest{}
	req.LeaderPgsStatus = make(map[string]protos.PgStatus)
	for _, v := range groups {
		value, ok := s.leaderPgStatusMap.Load(v)
		if ok {
			req.LeaderPgsStatus[v] = value.(protos.PgStatus)
		} else {
			req.LeaderPgsStatus[v] = protos.PgStatus{int32(s.cfg.NodeID), protos.PG_STATE_UNINITIAL}
		}
	}
	ctx := context.Background()
	res, err := client.OsdStatusReport(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send rpc request!")
		return
	}

	getRes := res.GetRetCode()
	helper.Println(5, "Finished! sendOsdStatusToMon retcode=%d", getRes, len(groups))

}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *OsdServer) Start(ctx context.Context) error {

	workersCtx := context.Background()
	//s.stopper.RunWorker(workersCtx, func(context.Context) {
	//	<-s.stopper.ShouldQuiesce()
	//	// TODO(bdarnell): Do we need to also close the other listeners?
	//	ln.Close()
	//	<-s.stopper.ShouldStop()
	//	s.grpc.Stop()
	//})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		s.grpc.Serve(Listener)
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		reportTicker := time.NewTicker(OSD_STATUS_REPORT_PERIOD)
		defer reportTicker.Stop()
		for {
			select {
			case <-reportTicker.C:
				mon := memberlist.GetLeaderMon()
				if mon == nil {
					helper.Println(5, "can not get primary mon addr yet!")
					continue
				}
				s.sendOsdStatusToMon(mon.Addr)
			case <-s.stopper.ShouldStop():
				return
			}
		}

	})
	//s.pgMaps

	s.createOrRemoveReplica()

	return nil
}

func UpdatePgStatusMap(pgId string, status int32) {
	Server.mapLock.Lock()
	defer Server.mapLock.Unlock()
	Server.leaderPgStatusMap.Store(&pgId, &protos.PgStatus{int32(Server.cfg.NodeID), status})
}
