package main

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/osd/client"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	"github.com/journeymidnight/nentropy/protos"
	"github.com/journeymidnight/nentropy/rpc"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math"
	"net"
	"sync"
	"time"
)

type OsdServer struct {
	nodeID            string
	cfg               Config
	grpc              *grpc.Server
	rpcContext        *rpc.Context
	raftTransport     *RaftTransport
	stopper           *stop.Stopper
	store             *Store
	engine            engine.Engine
	pgMaps            *protos.PgMaps
	poolMap           *protos.PoolMap
	leaderPgStatusMap sync.Map //map[string]protos.PgStatus
	pgStatusLock      *sync.Mutex
	confChangeLock    *sync.Mutex
	mc                *migrateCenter
	lastPgMapsEpoch   uint64
	storeCfg          StoreConfig // Config to use and pass to stores

	// used to create pgmap
	pgMapChan chan *protos.PgMaps
	waitChan  chan struct{}
	stopping  chan struct{}
}

const OSD_STATUS_REPORT_PERIOD = 2 * time.Second

func getOSDDataDir(baseDir string, id int) string {
	return fmt.Sprintf("%s/osd.%d", baseDir, id)
}

func getOsdMap() (*protos.OsdMap, error) {
	mon := memberlist.GetLeaderMon()
	helper.Println(5, "Connect to mon ", mon.Addr)
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure(), grpc.WithBlock())
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

func getPgMaps(epoch uint64) (*protos.PgMaps, error) {
	mon := memberlist.GetLeaderMon()
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		helper.Printf(5, "fail to dial: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	req := protos.PgConfigRequest{"", protos.PgConfigRequest_LIST, "", 0, epoch}
	reply, err := client.PgConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list osds error: ", err)
		return nil, err
	}
	return reply.Maps, nil
}

func getOsdSysDataDir() (string, error) {
	dir, err := helper.GetDataDir(config.BaseDir, uint64(config.NodeID), false, true)
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	return dir + "/sys-data", nil
}

// NewServer creates a Server from a server.Context.
func NewOsdServer(cfg Config, stopper *stop.Stopper) (*OsdServer, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr); err != nil {
		return nil, errors.Errorf("unable to resolve RPC address %q: %v", cfg.AdvertiseAddr, err)
	}

	s := &OsdServer{
		stopper:        stopper,
		cfg:            cfg,
		nodeID:         fmt.Sprintf("%d.%d", cfg.NodeType, cfg.NodeID),
		confChangeLock: &sync.Mutex{},
		pgStatusLock:   &sync.Mutex{},
		mc:             &migrateCenter{},
	}
	//s.cfg.AmbientCtx.AddLogTag("n", &s.nodeIDContainer)
	ctx := s.AnnotateCtx(context.Background())

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
	s.rpcContext = rpc.NewContext(s.cfg.AmbientCtx, &s.cfg.Config, s.stopper)
	s.grpc = rpc.NewServer()

	//init member list here
	rpcPort := Listener.Addr().(*net.TCPAddr).Port
	advertiseAddr := memberlist.GetMyIpAddress(cfg.AdvertiseIp, rpcPort)
	helper.Println(5, "advertiseAddr:", advertiseAddr)
	memberlist.Init(false, false, (uint64)(cfg.NodeID), advertiseAddr, cfg.MemberBindPort, logger.Logger, cfg.JoinMemberAddr)

	//get osd map
	osdmap, err := getOsdMap()
	if err != nil {
		return nil, err
	}
	helper.Println(5, "get osd map at startup")
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

	s.raftTransport = NewRaftTransport(
		s.cfg.AmbientCtx,
		GossipAddressResolver(), s.grpc, s.rpcContext,
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

	storeCfg := StoreConfig{
		AmbientCtx:                  s.cfg.AmbientCtx,
		RaftConfig:                  s.cfg.RaftConfig,
		Transport:                   s.raftTransport,
		CoalescedHeartbeatsInterval: 50 * time.Millisecond,
		RaftHeartbeatIntervalTicks:  1,
		NodeID:  config.NodeID,
		BaseDir: config.BaseDir,
	}
	s.storeCfg = storeCfg
	desc := multiraftbase.NodeDescriptor{}
	desc.NodeID = multiraftbase.NodeID(fmt.Sprintf("%s.%d", s.cfg.NodeType, s.cfg.NodeID))
	helper.Println(0, "New osd server nodeid: ", s.cfg.NodeID)
	s.store = NewStore(storeCfg, eng, &desc)

	if err := s.store.Start(ctx, stopper); err != nil {
		return nil, err
	}

	s.waitChan = make(chan struct{}, 4096)
	s.stopping = make(chan struct{})
	s.pgMapChan = make(chan *protos.PgMaps, 4096)
	go s.createReplicaLoop()

	return s, nil
}

type ListenError struct {
	error
	Addr string
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *OsdServer) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

func (s *OsdServer) fetchAndStoreObjectFromParent(ctx context.Context, ba multiraftbase.BatchRequest) error {
	child := string(ba.GroupID)
	st := s.mc.getStatics(child)
	mon := memberlist.GetLeaderMon()
	if mon == nil {
		helper.Println(5, "can not get primary mon addr yet!")
		return errors.New("can not get primary mon addr yet! when fetchAndStoreObjectFromParent")
	}
	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		helper.Println(5, "fail to dial: %v, when try connect to mon", err)
		return errors.New("fail to dial: when try connect to mon! when fetchAndStoreObjectFromParent")

	}
	defer conn.Close()
	client_mon := protos.NewMonitorClient(conn)
	req := protos.GetPgStatusRequest{}
	req.PgId = st.parent
	res, err := client_mon.GetPgStatus(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send rpc request! when fetchAndStoreObjectFromParent", err)
		return errors.New("Error send rpc request! when fetchAndStoreObjectFromParent")

	}
	nodeId := res.Status.LeaderNodeId
	member := memberlist.GetMemberByName(fmt.Sprintf("osd.%d", nodeId))
	if member == nil {
		helper.Println(5, "Exec GetMemberByName failed! nodeId:", nodeId)
		return errors.New("Exec GetMemberByName failed! when fetchAndStoreObjectFromParent")

	}
	conn_osd, err := grpc.Dial(member.Addr, grpc.WithInsecure())
	if err != nil {
		helper.Println(5, "fail to dial: %v, when try connect to member", err)
		return errors.New("fail to dial: %v, when try connect to member! when fetchAndStoreObjectFromParent")

	}
	defer conn_osd.Close()
	client_osd := protos.NewOsdRpcClient(conn_osd)

	oid := []byte{}
	bReq := ba.Request.GetValue().(multiraftbase.Request)
	if bReq.Method() == multiraftbase.Get {
		gReq := bReq.(*multiraftbase.GetRequest)
		oid = gReq.Key
	} else {
		pReq := bReq.(*multiraftbase.PutRequest)
		oid = pReq.Key
	}

	getReq := protos.MigrateGetRequest{}
	getReq.Marker = oid
	getReq.FlagNext = false
	helper.Println(5, "try migrated get object from old pg", getReq.Marker, getReq.ParentPgId, getReq.ChildPgId)
	getRes, err := client_osd.MigrateGet(ctx, &getReq)
	if err != nil {
		helper.Println(5, "Error send rpc request!", err)
		return errors.New("Error send rpc request! when fetchAndStoreObjectFromParent")

	}
	if len(getRes.Key) > 0 {
		b := &client.Batch{}
		b.Header.GroupID = multiraftbase.GroupID(child)
		b.AddRawRequest(&multiraftbase.PutRequest{
			Key:   multiraftbase.Key(getRes.Key),
			Value: getRes.Value,
		})
		if err := Server.store.Db.Run(context.Background(), b); err != nil {
			helper.Printf(5, "Error run batch! try put migrated get kv failed")
			return errors.New("Error run batch! try put migrated get kv failed! when fetchAndStoreObjectFromParent")
		}
	}
	return nil
}

// setupSpanForIncomingRPC takes a context and returns a derived context with a
// new span in it. Depending on the input context, that span might be a root
// span or a child span. If it is a child span, it might be a child span of a
// local or a remote span. Note that supporting both the "child of local span"
// and "child of remote span" cases are important, as this RPC can be called
// either through the network or directly if the caller is local.
//
// It returns the derived context and a cleanup function to be called when
// servicing the RPC is done. The cleanup function will close the span and, in
// case the span was the child of a remote span and "snowball tracing" was
// enabled on that parent span, it serializes the local trace into the
// BatchResponse. The cleanup function takes the BatchResponse in which the
// response is to serialized. The BatchResponse can be nil in case no response
// is to be returned to the rpc caller.
func (s *OsdServer) setupSpanForIncomingRPC(
	ctx context.Context, isLocalRequest bool,
) (context.Context, func(*multiraftbase.BatchResponse)) {
	// The operation name matches the one created by the interceptor in the
	// remoteTrace case below.
	const opName = "/nentropy.multiraft/Batch"
	var newSpan, grpcSpan opentracing.Span
	if isLocalRequest {
		// This is a local request which circumvented gRPC. Start a span now.
		ctx, newSpan = tracing.ChildSpan(ctx, opName)
	} else {
		grpcSpan = opentracing.SpanFromContext(ctx)
		if grpcSpan == nil {
			// If tracing information was passed via gRPC metadata, the gRPC interceptor
			// should have opened a span for us. If not, open a span now (if tracing is
			// disabled, this will be a noop span).
			newSpan = s.storeCfg.AmbientCtx.Tracer.StartSpan(opName)
			ctx = opentracing.ContextWithSpan(ctx, newSpan)
		}
	}

	finishSpan := func(br *multiraftbase.BatchResponse) {
		if newSpan != nil {
			newSpan.Finish()
		}
		if br == nil {
			return
		}
		if grpcSpan != nil {
			// If this is a "snowball trace", we'll need to return all the recorded
			// spans in the BatchResponse at the end of the request.
			// We don't want to do this if the operation is on the same host, in which
			// case everything is already part of the same recording.
			if rec := tracing.GetRecording(grpcSpan); rec != nil {
				//br.CollectedSpans = append(br.CollectedSpans, rec...)
			}
		}
	}
	return ctx, finishSpan
}

func (s *OsdServer) batchInternal(
	ctx context.Context, args *multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, error) {

	var br *multiraftbase.BatchResponse

	if err := s.stopper.RunTaskWithErr(ctx, "node.Node: batch", func(ctx context.Context) error {
		var pErr *multiraftbase.Error
		br = &multiraftbase.BatchResponse{}
		state, err := s.GetPgStateFromMap(string(args.GroupID))
		if err != nil {
			br.Error = multiraftbase.NewError(err)
			return nil
		}
		if state&protos.PG_STATE_MIGRATING != 0 {
			exist, err := s.store.ExistCheck(ctx, *args)
			if err != nil {
				br.Error = err
				return nil
			}
			if exist == false {
				err := s.fetchAndStoreObjectFromParent(ctx, *args)
				if err != nil {
					br.Error = multiraftbase.NewError(err)
					return nil
				}
			}
		}

		var finishSpan func(*multiraftbase.BatchResponse)
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, finishSpan = s.setupSpanForIncomingRPC(ctx, false)
		defer func(br **multiraftbase.BatchResponse) {
			finishSpan(*br)
		}(&br)

		br, pErr = s.store.Send(ctx, *args)
		if pErr != nil {
			if br == nil {
				br = &multiraftbase.BatchResponse{}
			}
			br.Error = pErr
			helper.Printf(5, "%T", pErr.GetDetail())
			return pErr.GoError()
		}
		if br.Error != nil {
			helper.Panicln(0, "unexpectedly error. error:", br.Error)
		}
		return nil
	}); err != nil {
		return br, err
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
			br.Error = multiraftbase.NewError(err)
		}
		return br, err
	}

	return br, nil
}

func (s *OsdServer) sendOsdStatusToMon(addr string) error {
	helper.Println(5, "enter sendOsdStatusToMon every two second")
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		helper.Println(5, "fail to dial: %v", err)
		return err
	}
	helper.Println(5, "enter sendOsdStatusToMon every two second, got conn")
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	groups, _ := s.store.GetGroupIdsByLeader()
	req := protos.OsdStatusReportRequest{}
	req.LeaderPgsStatus = make(map[string]protos.PgStatus)
	for _, v := range groups {
		value, ok := s.leaderPgStatusMap.Load(v)
		if ok {
			req.LeaderPgsStatus[v] = *(value.(*protos.PgStatus))
			helper.Println(5, "find pgid in map", v, req.LeaderPgsStatus[v])
		} else {
			helper.Println(5, "not find pgid in map", v)
			req.LeaderPgsStatus[v] = protos.PgStatus{
				LeaderNodeId: int32(s.cfg.NodeID),
				Status:       protos.PG_STATE_UNINITIAL,
				MigratedCnt:  0,
			}
		}
	}
	ctx := context.Background()
	res, err := client.OsdStatusReport(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send rpc request! sendOsdStatusToMon", err)
		return err
	}

	getRes := res.GetRetCode()
	helper.Println(5, "Finished! sendOsdStatusToMon retcode=%d", getRes, len(groups))
	return nil

}

func (s *OsdServer) SentOsdStatus() error {
	mon := memberlist.GetLeaderMon()
	if mon == nil {
		helper.Println(5, "can not get primary mon addr yet!")
		return errors.New("can not get primary mon addr yet!")
	}
	return s.sendOsdStatusToMon(mon.Addr)
}

func (s *OsdServer) getPGStatusFromMon(addr string, groupId multiraftbase.GroupID) (*protos.PgStatus, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		helper.Println(5, "fail to dial: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	ctx := context.Background()
	req := protos.GetPgStatusRequest{}
	req.PgId = string(groupId)
	res, err := client.GetPgStatus(ctx, &req)
	if err != nil {
		helper.Println(5, "Error send rpc request! getPGStatusFromMon", err)
		return nil, err
	}
	status := res.GetStatus()

	return status, nil

}

func (s *OsdServer) GetPGStatus(groupId multiraftbase.GroupID) (*protos.PgStatus, error) {
	mon := memberlist.GetLeaderMon()
	if mon == nil {
		helper.Println(5, "can not get primary mon addr yet!")
		return nil, errors.New("can not get primary mon addr yet!")
	}
	return s.getPGStatusFromMon(mon.Addr, groupId)
}

func (s *OsdServer) GetPoolMap() (*protos.PoolMap, error) {
	mon := memberlist.GetLeaderMon()
	if mon == nil {
		helper.Println(5, "can not get primary mon addr yet!")
		return nil, errors.New("can not get primary mon addr yet!")
	}

	conn, err := grpc.Dial(mon.Addr, grpc.WithInsecure())
	if err != nil {
		helper.Println(5, "fail to dial: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := protos.NewMonitorClient(conn)
	req := protos.PoolConfigRequest{"", protos.PoolConfigRequest_LIST, "", 0, 0, 0}
	reply, err := client.PoolConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("get pools error: ", err)
		return nil, err
	}
	return reply.GetMap(), nil
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

	//s.pgMaps
	pgmaps, err := getPgMaps(0)
	if err != nil {
		return err
	}
	s.pgMaps = pgmaps
	helper.Println(5, "pgmaps.epoch:", pgmaps.Epoch, " pool count:", len(pgmaps.Pgmaps))

	s.initPGState(pgmaps)
	s.createOrRemoveReplica(pgmaps)

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		time.Sleep(1 * time.Second)
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

	return nil
}

func (s *OsdServer) delReplicasToPGState(reps []protos.PgReplica, groupID multiraftbase.GroupID) ([]protos.PgReplica, error) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()

	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(string(groupID)); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	replicas := make([]protos.PgReplica, 0)
	for _, replica := range pgStatus.Members {
		var exist bool
		for _, rep := range reps {
			if replica.ReplicaIndex == rep.ReplicaIndex && replica.OsdId == rep.OsdId {
				exist = true
			}
		}
		if !exist {
			replicas = append(replicas, replica)
		}
	}
	pgStatus.Members = replicas
	s.leaderPgStatusMap.Store(string(groupID), pgStatus)
	s.WritePgStatusMap(string(groupID), pgStatus)
	return nil, nil
}

func (s *OsdServer) addReplicasToPGState(reps []protos.PgReplica, groupID multiraftbase.GroupID) ([]protos.PgReplica, error) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()

	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(string(groupID)); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	replicas := make([]protos.PgReplica, 0)
	for _, replica := range pgStatus.Members {
		replicas = append(replicas, replica)
	}

	var newReplica []protos.PgReplica
	for _, rep := range reps {
		var exist bool
		for _, replica := range pgStatus.Members {
			if replica.ReplicaIndex == rep.ReplicaIndex && replica.OsdId == rep.OsdId {
				exist = true
			}
		}
		if !exist {
			replicas = append(replicas, rep)
			newReplica = append(newReplica, rep)
		}
	}
	pgStatus.Members = replicas
	s.leaderPgStatusMap.Store(string(groupID), pgStatus)
	s.WritePgStatusMap(string(groupID), pgStatus)
	return newReplica, nil
}

func PrintPgStatusMap() {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	f := func(key, value interface{}) bool {
		pgId := key.(string)
		pgStatus := value.(*protos.PgStatus)

		helper.Printf(5, " GroupID: %s ", pgId)
		helper.Printf(5, "Member replica ")
		for _, rep := range pgStatus.Members {
			helper.Printf(5, " %d:%d ", rep.OsdId, rep.ReplicaIndex)
		}
		helper.Printf(5, "\n")
		return true
	}
	Server.leaderPgStatusMap.Range(f)
}

func GetPgMember(pgId string) ([]protos.PgReplica, error) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(pgId); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	member := pgStatus.Members
	return member, nil
}

func UpdatePgMemberMap(pgId string, member []protos.PgReplica) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(pgId); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	pgStatus.Members = member
	Server.leaderPgStatusMap.Store(pgId, pgStatus)
}

func UpdatePgStatusMap(pgId string, status int32, cnt int64) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(pgId); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	pgStatus.LeaderNodeId = int32(Server.cfg.NodeID)
	pgStatus.Status = status
	pgStatus.MigratedCnt = cnt

	Server.leaderPgStatusMap.Store(pgId, pgStatus)
}

func (s *OsdServer) InitPgStatusMap(pgId string) (*protos.PgStatus, error) {
	ok := s.store.isExistReplicaWorkDir(multiraftbase.GroupID(pgId))
	if !ok {
		return nil, nil
	}
	eng, err := s.store.GetGroupStore(multiraftbase.GroupID(pgId))
	if err != nil {
		helper.Fatalln("Can not new a badger db.")
	}

	data, err := StripeRead(eng, []byte("system_pg_state"), 0, math.MaxUint32)
	if err != nil {
		helper.Check(err)
	}
	pgStatus := &protos.PgStatus{}
	err = pgStatus.Unmarshal(data)
	if err != nil {
		helper.Check(err)
	}

	return pgStatus, nil
}

func (s *OsdServer) WritePgStatusMap(pgId string, pgStatus *protos.PgStatus) error {

	eng, err := s.store.GetGroupStore(multiraftbase.GroupID(pgId))
	if err != nil {
		helper.Fatalln("Can not new a badger db.")
	}

	data, err := pgStatus.Marshal()
	if err != nil {
		helper.Check(err)
	}

	err = stripeWrite(eng, []byte("system_pg_state"), data, 0, uint64(len(data)))
	if err != nil {
		helper.Check(err)
	}
	return nil
}

func (s *OsdServer) SetPgState(pgId string, pgState int32) error {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(pgId); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	pgStatus.Status = pgState
	data, err := pgStatus.Marshal()
	if err != nil {
		return err
	}

	b := &client.Batch{}
	b.Header.GroupID = multiraftbase.GroupID(pgId)
	b.AddRawRequest(&multiraftbase.PutRequest{
		Key:   multiraftbase.Key([]byte("system_pg_state")),
		Value: data,
	})
	if err := s.store.Db.Run(context.Background(), b); err != nil {
		helper.Printf(5, "Error run batch! %s", err)
		return err
	}
	return nil
}

/*
func (s *OsdServer) SetPgState(pgId string, pgState int32) error {
	b := &client.Batch{}
	b.Header.GroupID = multiraftbase.GroupID(pgId)
	state := []byte(strconv.Itoa(int(pgState)))
	b.AddRawRequest(&multiraftbase.PutRequest{
		Key:   multiraftbase.Key([]byte("system_pg_state")),
		Value: state,
	})
	if err := s.store.Db.Run(context.Background(), b); err != nil {
		helper.Printf(5, "Error run batch! %s", err)
		return err
	}
	return nil
}
*/
func (s *OsdServer) GetPgState(pgId string) (int32, error) {
	Server.pgStatusLock.Lock()
	defer Server.pgStatusLock.Unlock()
	var pgStatus *protos.PgStatus
	if value, ok := Server.leaderPgStatusMap.Load(pgId); ok {
		pgStatus = value.(*protos.PgStatus)
	} else {
		pgStatus = &protos.PgStatus{}
	}
	return pgStatus.Status, nil
}

/*
func (s *OsdServer) GetPgState(pgId string) (int32, error) {
	b := &client.Batch{}
	b.Header.GroupID = multiraftbase.GroupID(pgId)
	b.AddRawRequest(&multiraftbase.GetRequest{
		Key:   multiraftbase.Key([]byte("system_pg_state")),
		Value: multiraftbase.Value{},
	})
	if err := s.store.Db.Run(context.Background(), b); err != nil {
		return 0, err
	}
	state := b.Results[0].Rows[0].Value
	ret, _ := strconv.Atoi(string(*state))
	return int32(ret), nil
}
*/
func (s *OsdServer) GetPgStateFromMap(pgId string) (int32, error) {
	value, ok := Server.leaderPgStatusMap.Load(pgId)
	if !ok {
		return protos.PG_STATE_UNINITIAL, errors.New("pg state not existed")
	}
	return value.(*protos.PgStatus).Status, nil
}

func GetPgState(pgId string) (int32, error) {
	return Server.GetPgState(pgId)
}

func GetPoolSize(pgId string) (int32, error) {
	return Server.poolMap.GetPoolSize(pgId)
}

func GetExpectedReplicaId(pgId string) (int32, bool) {
	id, err := Server.pgMaps.GetPgExpectedId(pgId)
	if err != nil {
		helper.Printf(5, "GetExpectedPrimaryId failed:%d", pgId)
		return 0, false
	}
	return id, true
}

func GetPgReplicas(pgId string) ([]protos.PgReplica, bool) {
	reps, err := Server.pgMaps.GetPgReplicas(pgId)
	if err != nil {
		helper.Printf(5, "GetExpectedPrimaryId failed:%d", pgId)
		return nil, false
	}
	return reps, true
}

func ReplicaDelReplicaCallback(reps []protos.PgReplica, groupID multiraftbase.GroupID) {
	helper.Println(5, "group:", groupID, "del rep:", reps)
	go Server.delReplicasToPGState(reps, groupID)
}

func ReplicaStateChangeCallback(pgId string, replicaState string) {
	helper.Logger.Println(5, fmt.Sprintf("enter ReplicaStateChangeCallback %s, %s", pgId, replicaState))
	if replicaState == raft.StateLeader.String() {
		go func() {
			state, err := Server.GetPgState(pgId)
			helper.Logger.Println(5, fmt.Sprintf("try load old pg state, state=%d, error=%v", state, err))
			if state == 0 {

				parent, err := Server.pgMaps.GetPgParentId(pgId)
				if err != nil {
					helper.Printf(5, "GetPgParentId failed:%d", pgId)
					return
				}
				if parent != pgId {
					helper.Printf(5, "enlarge pg:%s, set pg state PG_STATE_MIGRATING", pgId)
					newState := protos.PG_STATE_ACTIVE | protos.PG_STATE_MIGRATING
					err = Server.SetPgState(pgId, int32(newState))
					if err != nil {
						helper.Printf(5, "failed update pg state for pg:", pgId, err)
						return
					}
					UpdatePgStatusMap(pgId, int32(newState), 0)
					err = Server.SentOsdStatus()
					if err != nil {
						helper.Printf(5, "failed to notify mon that pg status has changed: %d, %v", pgId, err)
						return
					}
					//start migrate
					Server.mc.addTask(parent, pgId)
					return
				} else {
					helper.Printf(5, "root pg:%s, set pg state PG_STATE_ACTIVE & PG_STATE_CLEAN", pgId)
					newState := protos.PG_STATE_ACTIVE | protos.PG_STATE_CLEAN
					err = Server.SetPgState(pgId, int32(newState))
					if err != nil {
						helper.Printf(5, "failed set pg state for pg:", pgId, err)
						return
					}
					UpdatePgStatusMap(pgId, int32(newState), 0)
					return
				}
			} else if err == nil {
				helper.Printf(5, "Restarted pg:%s, state:%s", pgId, protos.Pg_state_string(state))
				UpdatePgStatusMap(pgId, int32(state), 0)
				return
			} else {
				helper.Printf(5, "Try GetPgState failed, please check: %v", err)
				return
			}

		}()
	} else if replicaState == raft.StateFollower.String() {

	}

}
