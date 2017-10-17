package main

import (
	"flag"
	"fmt"
	pb "github.com/journeymidnight/nentropy/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
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
	object             = flag.String("o", "", "The object name sent to monitor")
	pgNumber           = flag.Int("pg_number", 64, "set pg numbers for pool, default: 64")
	policy             = flag.String("policy", "osd", "set pg policy [zone/host/osd] default: osd")
	size               = flag.Int("size", 3, "set pg replicate numbers, default: 3")
	id                 = flag.Int("id", 0, "id for target")
	weight             = flag.Int("weight", 1, "set osd weight 1 per T")
	host               = flag.String("host", "localhost", "set osd host name, default: localhost")
	zone               = flag.String("zone", "default", "set osd zone name, default: default")
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
		fmt.Println("osds:", reply.Map.Pgmap[int32(k)].OsdIds)
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

func objectHandle() {
	switch *cmd {
	case "search":
		result, err := getObjectLayout(*pool, *object)
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
	default:
		fmt.Println("unsupport cmd, should be put/get/delete/search")
		return
	}
	return
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(*serverAddr, opts...)
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
