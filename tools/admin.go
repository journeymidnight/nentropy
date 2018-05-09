package main

import (
	"flag"
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	pb "github.com/journeymidnight/nentropy/protos"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	caFile             = flag.String("ca_file", "", "The file containning the CA root cert file")
	serverAddr         = flag.String("server_addr", "127.0.0.1:9999", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name use to verify the hostname returned by TLS handshake")
	cmd                = flag.String("c", "", "The command sent to monitor")
	target             = flag.String("t", "", "The command target sent to monitor")
	pool               = flag.String("p", "", "The pool name sent to monitor")
	oid                = flag.String("oid", "", "The object name sent to monitor")
	pgNumber           = flag.Int("pg_number", 64, "set pg numbers for pool, default: 64")
	policy             = flag.String("policy", "osd", "set pg policy [zone/host/osd] default: osd")
	size               = flag.Int("size", 3, "set pg replicate numbers, default: 3")
	id                 = flag.Int("id", 0, "id for target")
	offset             = flag.Int64("offset", 0, "the offset of object to read or write, default: 0")
	length             = flag.Uint64("len", 0, "the length of object to read or write, default: 0")
	weight             = flag.Int("weight", 1, "set osd weight 1 per T")
	host               = flag.String("host", "localhost", "set osd host name, default: localhost")
	zone               = flag.String("zone", "default", "set osd zone name, default: default")
	val                = flag.String("val", "", "val")
	filename           = flag.String("f", "", "the filename to read or write")
	bThread            = flag.Int("bench_thread", 16, "bench concurrent IOS, default:16")
	bSize              = flag.Int("bench_size", 131072, "bench value size, default:128K")
	bRunName           = flag.String("bench_run_name", "", "bench run name, default:")
	client             pb.MonitorClient
	Bc                 *benchControl
)

func createPool(poolName string, poolSize int32, pgNumber int32, policy pb.DistributePolicy) {
	req := pb.PoolConfigRequest{"", pb.PoolConfigRequest_ADD, poolName, poolSize, pgNumber, policy}
	_, err := client.PoolConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("create pool error: ", err)
	}
	return
}

func deletePool(poolName string) {
	req := pb.PoolConfigRequest{"", pb.PoolConfigRequest_DEL, poolName, 0, 0, 0}
	_, err := client.PoolConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("delete pool error: ", err)
	}
	return
}

func listPools() {
	req := pb.PoolConfigRequest{"", pb.PoolConfigRequest_LIST, "", 0, 0, 0}
	reply, err := client.PoolConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list pools error: ", err)
		return
	}
	fmt.Println("List Pools Result:")
	fmt.Println("Epoch:", reply.Map.Epoch)
	for _, v := range reply.Map.Pools {
		fmt.Println("================================")
		fmt.Println("id:", v.Id)
		fmt.Println("name:", v.Name)
		fmt.Println("policy:", v.Policy)
		fmt.Println("pgNumbers:", v.PgNumbers)
		fmt.Println("size:", v.Size_)
	}
	return
}

func setPgNumber(poolName string, pgNumber int32) {
	if poolName == "" {
		fmt.Println("pool name can not be empty")
		return
	}

	req := pb.PoolConfigRequest{"", pb.PoolConfigRequest_SET_PGS, poolName, 0, pgNumber, 0}
	_, err := client.PoolConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("set pg numbers error: ", err)
	}
	return
}

func poolHandle() {
	switch *cmd {
	case "create":
		policy_int32 := int32(0)
		if *policy == "zone" {
			policy_int32 = 2
		} else if *policy == "host" {
			policy_int32 = 1
		} else {
			policy_int32 = 0
		}
		createPool(*pool, int32(*size), int32(*pgNumber), pb.DistributePolicy(policy_int32))
	case "delete":
		deletePool(*pool)
	case "list":
		listPools()
	case "setpgs":
		setPgNumber(*pool, int32(*pgNumber))
	default:
		fmt.Println("unsupport cmd, should be create/delete")
		return
	}
	return
}

func addOsd(osdId int32, osdWeight uint64, osdHost string, osdZone string, osdUp, osdIn bool) {
	req := pb.OsdConfigRequest{"", &pb.Osd{osdId, "", osdWeight, osdHost, osdZone, osdUp, osdIn}, pb.OsdConfigRequest_ADD}
	_, err := client.OsdConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("add osd error: ", err)
	}
	return
}

func removeOsd(osdId int32) {
	req := pb.OsdConfigRequest{"", &pb.Osd{osdId, "", 0, "", "", false, false}, pb.OsdConfigRequest_DEL}
	_, err := client.OsdConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("del osd error: ", err)
	}
	return
}

func listOsds() {
	req := pb.OsdConfigRequest{"", &pb.Osd{}, pb.OsdConfigRequest_LIST}
	reply, err := client.OsdConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list osds error: ", err)
		return
	}
	fmt.Println("List Osds Result:")
	fmt.Println("Epoch:", reply.Map.Epoch)
	for _, v := range reply.Map.MemberList {
		fmt.Println("================================")
		fmt.Println("id:", v.Id)
		fmt.Println("addr:", v.Addr)
		fmt.Println("weight:", v.Weight)
		fmt.Println("host:", v.Host)
		fmt.Println("zone:", v.Zone)
		fmt.Println("up:", v.Up)
		fmt.Println("in:", v.In)
	}
	return
}

func osdHandle() {
	switch *cmd {
	case "add":
		addOsd(int32(*id), uint64(*weight), *host, *zone, false, true)
	case "remove":
		removeOsd(int32(*id))
	case "in":
	case "out":
	case "list":
		listOsds()
	default:
		fmt.Println("unsupport cmd, should be add/remove/in/out")
		return
	}
	return
}

func listPgs() {
	req := pb.PgConfigRequest{"", pb.PgConfigRequest_LIST, *pool, 0, 0}
	reply, err := client.PgConfig(context.Background(), &req)
	if err != nil {
		fmt.Println("list osds error: ", err)
		return
	}
	var keys []int
	for k := range reply.Map.Pgmap {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	fmt.Println("List Pgs Result:")
	fmt.Println("Epoch:", reply.Epoch)
	for _, k := range keys {
		fmt.Println("================================")
		fmt.Println("id:", reply.Map.Pgmap[int32(k)].Id)
		fmt.Println("osds:", reply.Map.Pgmap[int32(k)].Replicas)
		if val, ok := reply.StatusMap[int32(k)]; ok {
			//do something here
			fmt.Println("leader:", val.LeaderNodeId)
			fmt.Println("status:", pb.Pg_state_string(val.Status))
			fmt.Println("migrate count:", val.MigratedCnt)
		} else {
			fmt.Println("status:", "")
		}
	}

	return
}

func pgHandle() {
	switch *cmd {
	case "list":
		listPgs()
	default:
		fmt.Println("unsupport cmd, should be add/remove/in/out")
		return
	}
	return
}

func getObjectLayout(poolName string, objectName string) (*pb.LayoutReply, error) {
	req := pb.LayoutRequest{objectName, poolName}
	reply, err := client.GetLayout(context.Background(), &req)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

const MAX_SIZE = 1024 * 1024 * 4

func putObject(pgname string, osd string, oid []byte, filename string) error {
	fmt.Println("put object to server ", osd)
	if *length > MAX_SIZE {
		return errors.New(fmt.Sprintf("the write length should not be 0 or more than %s", MAX_SIZE))
	}
	if *length == 0 {
		info, err := os.Stat(filename)
		if err != nil {
			return errors.New(fmt.Sprintf("Error stat file, err %s", err))
		}
		if info.Size() > MAX_SIZE {
			return errors.New(fmt.Sprintf("file size exceed limit %d", MAX_SIZE))
		} else {
			*length = uint64(info.Size())
		}
	}
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Cannot open file. err:", err)
		return err
	}
	defer file.Close()
	data := make([]byte, *length)
	_, err = file.ReadAt(data, 0)
	if err != nil && err != io.EOF {
		fmt.Println("Cannot read file. err:", err)
		return err
	}

	conn, err := grpc.Dial(osd, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = multiraftbase.GroupID(pgname)

	putReq := multiraftbase.NewPut(oid, data)
	dumy.Request.MustSetInner(putReq)

	ctx := context.Background()
	res, err := client.Batch(ctx, &dumy)
	if err != nil {
		return err
	}

	fmt.Printf("putobject()! res=%s", res.Error.GoError())
	return nil
}

func getObject(pgname string, osd string, oid []byte, filename string) error {
	if *length > MAX_SIZE {
		return errors.New(fmt.Sprintf("the read length should not be 0 or more than %s", MAX_SIZE))
	}
	if *length == 0 {
		*length = MAX_SIZE
	}
	conn, err := grpc.Dial(osd, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = multiraftbase.GroupID(pgname)

	getReq := multiraftbase.NewGet(oid)
	dumy.Request.MustSetInner(getReq)

	ctx := context.Background()
	res, err := client.Batch(ctx, &dumy)
	if err != nil {
		fmt.Printf("Error send rpc request!， admin getObject", err)
		return err
	}
	getRes := res.Responses.GetValue().(*multiraftbase.GetResponse)

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Cannot open file. err:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(getRes.Value, 0)
	if err != nil {
		fmt.Println("Cannot write data to file. err:", err)
		return err
	}

	return nil
}

func putObjectFromBuf(pgname string, osd string, oid []byte, buf []byte) error {
	tm := time.Now()
	Bc.log.Printf("try get conn to osd %v ,%v", string(oid), osd)
	var conn *grpc.ClientConn
	var err error
	value, ok := Bc.osdConnMap.Load(osd)
	if ok {
		conn = value.(*grpc.ClientConn)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		newConn, err := grpc.DialContext(ctx, osd, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
			return err
		}
		realConn, loaded := Bc.osdConnMap.LoadOrStore(osd, newConn)
		if loaded {
			newConn.Close()
		}
		conn = realConn.(*grpc.ClientConn)
	}

	Bc.log.Printf("got conn to osd %v ,%v, cost %v", string(oid), osd, time.Now().Sub(tm).Seconds())
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = multiraftbase.GroupID(pgname)

	putReq := multiraftbase.NewPut(oid, buf)
	dumy.Request.MustSetInner(putReq)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.Batch(ctx, &dumy)
	if err != nil {
		fmt.Printf("putobject()! res=%v", err, pgname, osd, string(oid))
		return err
	}

	return nil
}

func deleteObject(pgname string, osd string, oid []byte) error {
	if *length > MAX_SIZE {
		return errors.New(fmt.Sprintf("the read length should not be 0 or more than %s", MAX_SIZE))
	}
	if *length == 0 {
		*length = MAX_SIZE
	}
	conn, err := grpc.Dial(osd, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Fail to dial: %v", err)
	}
	defer conn.Close()
	client := multiraftbase.NewInternalClient(conn)

	dumy := multiraftbase.BatchRequest{}
	dumy.GroupID = multiraftbase.GroupID(pgname)

	deleteReq := &multiraftbase.DeleteRequest{}
	deleteReq.Key = oid
	dumy.Request.MustSetInner(deleteReq)

	ctx := context.Background()
	_, err = client.Batch(ctx, &dumy)
	if err != nil {
		fmt.Printf("Error send rpc request! admin deleteObject", err)
		return err
	}
	//deleteRes := res.Responses.GetValue().(*multiraftbase.DeleteResponse)

	return nil
}

func objectHandle() {
	switch *cmd {
	case "search":
		result, err := getObjectLayout(*pool, *oid)
		if err != nil {
			fmt.Println("get object layout error: ", err)
			return
		}
		fmt.Println("Layout Info:")
		fmt.Println("PG name:", result.PgName)
		fmt.Println("OSDS:")
		for _, osd := range result.Osds {
			fmt.Println(fmt.Sprintf("id:%d addr:%s weight:%d host:%s zone:%s up:%v in:%v", osd.Id, osd.Addr, osd.Weight, osd.Host, osd.Zone, osd.Up, osd.In))
		}
	case "put":
		result, err := getObjectLayout(*pool, *oid)
		if err != nil {
			fmt.Println("get object layout error: ", err)
			return
		}
		fmt.Println("Layout Info:")
		fmt.Println("PG name:", result.PgName)
		fmt.Println("OSDS:")
		for _, osd := range result.Osds {
			fmt.Println(fmt.Sprintf("id:%d addr:%s weight:%d host:%s zone:%s up:%v in:%v", osd.Id, osd.Addr, osd.Weight, osd.Host, osd.Zone, osd.Up, osd.In))
		}
		err = putObject(result.PgName, result.Osds[0].Addr, []byte(*oid), *filename)
		if err != nil {
			fmt.Println("Error putting object from osd, err:", err)
			return
		}
	case "get":
		result, err := getObjectLayout(*pool, *oid)
		if err != nil {
			fmt.Println("get object layout error: ", err)
			return
		}
		fmt.Println("Layout Info:")
		fmt.Println("PG name:", result.PgName)
		fmt.Println("OSDS:")
		for _, osd := range result.Osds {
			fmt.Println(fmt.Sprintf("id:%d addr:%s weight:%d host:%s zone:%s up:%v in:%v", osd.Id, osd.Addr, osd.Weight, osd.Host, osd.Zone, osd.Up, osd.In))
		}
		err = getObject(result.PgName, result.Osds[0].Addr, []byte(*oid), *filename)
		if err != nil {
			fmt.Println("Error putting object from osd, err:", err)
			return
		}
	case "delete":
		result, err := getObjectLayout(*pool, *oid)
		if err != nil {
			fmt.Println("get object layout error: ", err)
			return
		}
		fmt.Println("Layout Info:")
		fmt.Println("PG name:", result.PgName)
		fmt.Println("OSDS:")
		for _, osd := range result.Osds {
			fmt.Println(fmt.Sprintf("id:%d addr:%s weight:%d host:%s zone:%s up:%v in:%v", osd.Id, osd.Addr, osd.Weight, osd.Host, osd.Zone, osd.Up, osd.In))
		}
		err = deleteObject(result.PgName, result.Osds[0].Addr, []byte(*oid))
		if err != nil {
			fmt.Println("Error removing object from osd, err:", err)
			return
		}

	default:
		fmt.Println("unsupport cmd, should be put/get/delete/search")
		return
	}
	return
}

type benchControl struct {
	stop         chan struct{}
	wg           sync.WaitGroup
	lock         sync.RWMutex //protect totalCount
	runName      string
	totalCount   int
	size         int //write object length
	startTime    time.Time
	endTime      time.Time
	threadNumber int
	pool         string
	buffer       []byte
	log          *log.Logger
	osdConnMap   sync.Map //map[string]*ClientConn
}

func (bc *benchControl) printStatics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	lastCount := 0
	fmt.Println(fmt.Sprintf("Start Bench Test!!!"))
	fmt.Println(fmt.Sprintf("Method:%s    Thread:%d    Size:%d    RunName:%s", *cmd, bc.threadNumber, bc.size, bc.runName))
	for {
		select {
		case <-ticker.C:
			bc.lock.RLock()
			ops := bc.totalCount - lastCount
			bc.lock.RUnlock()
			bandwidth := bc.size * ops / 1024
			lastCount = bc.totalCount
			fmt.Println(fmt.Sprintf("Current Ops:%d/s  BandWidth:%dK/s TotalCount:%d", ops, bandwidth, bc.totalCount))
		case <-bc.stop:
			return
		}
	}
	return
}

func (bc *benchControl) generateObjectKey(id int) string {
	return "BenchMark_" + bc.runName + "_" + strconv.Itoa(id)
}

func (bc *benchControl) worker(threadId int) {
	bc.wg.Add(1)
	defer bc.wg.Done()
	if *cmd == "write" {
		for {
			//fmt.Println("enter worker", threadId)
			select {
			case <-bc.stop:
				fmt.Println("terminal worker", threadId)
				return
			default:
				bc.lock.Lock()
				newId := bc.totalCount + 1
				bc.totalCount = newId
				bc.lock.Unlock()
				key := bc.generateObjectKey(newId)
				bc.log.Printf("try get lay out of %v", key)
				tm := time.Now()
				result, err := getObjectLayout(bc.pool, key)
				if err != nil {
					fmt.Println("get object layout error: ", err)
					panic("")
				}
				bc.log.Printf("got lay out of %v, cost %v s", key, time.Now().Sub(tm).Seconds())
				//fmt.Println("Layout Info:")
				//fmt.Println("PG name:", result.PgName)
				//fmt.Println("OSDS:")
				bc.log.Printf("try put kv of %v", key)
				tm = time.Now()
				err = putObjectFromBuf(result.PgName, result.Osds[0].Addr, []byte(key), bc.buffer)
				if err != nil {
					fmt.Println("Error put object err:", err, result.PgName, result.Osds[0].Addr, []byte(key))
					panic("")
				}
				bc.log.Printf("finish put kv of %v, cost %v s", key, time.Now().Sub(tm).Seconds())
			}
		}
	}

}

func (bc *benchControl) run() {
	for i := 0; i < bc.threadNumber; i++ {
		go bc.worker(i)
	}
	return
}

func benchHandle() {
	if *size > 4194304 {
		fmt.Println("bench size exceed limit, can not be larger than 4M")
		return
	}
	bc := &benchControl{}
	Bc = bc
	f, err := os.OpenFile("./bench.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		bc.log = log.New(os.Stdout, "[bench]", log.LstdFlags)
		fmt.Println(0, "Failed to open log file %s, use stdout!")
	} else {
		defer f.Close()
		bc.log = log.New(f, "[bench]", log.LstdFlags)
	}

	switch *cmd {
	case "write":
		if *bRunName != "" {
			bc.runName = *bRunName
		} else {
			bc.runName = string(helper.GenerateRandomNumberId())
		}
		bc.stop = make(chan struct{})
		bc.startTime = time.Now()
		bc.size = *bSize
		bc.threadNumber = *bThread
		bc.pool = *pool
		bc.buffer = helper.GenerateRandomIdByLength(bc.size)

	case "read":
		if *bRunName == "" {
			fmt.Println("bench read must specify run name")
			return
		}
		bc.stop = make(chan struct{})
		bc.startTime = time.Now()
		bc.threadNumber = *bThread
	default:
		fmt.Println("unsupport bench command, [write/read/clear] is allowed")
		return
	}
	fmt.Println("enter benchHandle")
	go bc.run()
	go bc.printStatics()
	fmt.Println("wait stop signal")
	signal.Ignore()
	signalQueue := make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	for {
		s := <-signalQueue
		switch s {
		default:
			close(bc.stop)
			bc.wg.Wait()
			return
		}
	}
	return
}

func getMonMap(addr string) *pb.MonConfigReply {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewMonitorClient(conn)
	req := pb.MonConfigRequest{Method: "GET"}
	rep, err := cli.GetMonMap(context.Background(), &req)
	if err != nil {
		log.Fatalf("fail to get mon map: %v", err)
	}
	return rep
}

func main() {
	flag.Parse()

	var monLeader string
	res := getMonMap(*serverAddr)
	for _, v := range res.Map.MonMap {
		if v.Id == res.LeadId {
			monLeader = v.Addr
		}
	}
	if monLeader == "" {
		log.Fatalf("fail to get montors leader from map.")
	}
	fmt.Println("leader is ", monLeader)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial(monLeader, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client = pb.NewMonitorClient(conn)

	switch *target {
	case "pool":
		poolHandle()
	case "pg":
		pgHandle()
	case "osd":
		osdHandle()
	case "object":
		objectHandle()
	case "bench":
		benchHandle()
	default:
		fmt.Printf("unsupport target type %v", target)
	}
	return
}
