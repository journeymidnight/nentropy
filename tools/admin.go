package main

import (
	"flag"
	"fmt"
	"github.com/journeymidnight/nentropy/osd/multiraftbase"
	pb "github.com/journeymidnight/nentropy/protos"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
	"sort"
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
	client             pb.MonitorClient
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
	req := pb.PgConfigRequest{"", pb.PgConfigRequest_LIST, *pool, 0}
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

const MAX_SIZE = 1024 * 2048

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
			return errors.New(fmt.Sprintf("Error stat file, err %s", err))
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
	n, err := file.ReadAt(data, 0)
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

	value := multiraftbase.Value{RawBytes: data, Offset: *offset, Len: uint64(n)}
	putReq := multiraftbase.NewPut(oid, value)
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

	getReq := multiraftbase.NewGet(oid, *offset, uint64(*length))
	dumy.Request.MustSetInner(getReq)

	ctx := context.Background()
	res, err := client.Batch(ctx, &dumy)
	if err != nil {
		fmt.Printf("Error send rpc request!")
		return err
	}
	getRes := res.Responses.GetValue().(*multiraftbase.GetResponse)

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Cannot open file. err:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(getRes.Value.RawBytes, 0)
	if err != nil {
		fmt.Println("Cannot write data to file. err:", err)
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
		fmt.Printf("Error send rpc request!")
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
			fmt.Println("Error getting object from osd, err:", err)
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
	default:
		fmt.Printf("unsupport target type %v", target)
	}
	return
}
