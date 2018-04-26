package memberlist

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/pborman/uuid"
	"log"
	"net"
	"os"
	"strconv"
)

const (
	MEMBER_LIST_CHAN_EVENT_NUM = 16
)

type MemberEventType int

const (
	MemberJoin MemberEventType = iota
	MemberLeave
	MemberUpdate
)

type Member struct {
	IsMon    bool
	IsLeader bool
	Name     string
	Addr     string
	Port     uint16
	RaftPort uint16
	ID       uint64
}

type NotifyMemberEvent func(MemberEventType, Member) error

var notifyMemberEvent NotifyMemberEvent
var List *memberlist.Memberlist
var eventCh chan memberlist.NodeEvent

type MemberDelegate struct {
	meta        []byte
	msgs        [][]byte
	broadcasts  [][]byte
	state       []byte
	remoteState []byte
}

func (m *MemberDelegate) NodeMeta(limit int) []byte {
	return m.meta
}

func (m *MemberDelegate) NotifyMsg(msg []byte) {
	cp := make([]byte, len(msg))
	copy(cp, msg)
	m.msgs = append(m.msgs, cp)
}

func (m *MemberDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	b := m.broadcasts
	m.broadcasts = nil
	return b
}

func (m *MemberDelegate) LocalState(join bool) []byte {
	return m.state
}

func (m *MemberDelegate) MergeRemoteState(s []byte, join bool) {
	m.remoteState = s
}

type MonitorMergeDelegate struct {
	invoked bool
}

func (c *MonitorMergeDelegate) NotifyMerge(nodes []*memberlist.Node) error {
	c.invoked = true

	return fmt.Errorf("Custom merge canceled")
}

func recvChanEvent(myName string) {
	for {
		select {
		case e := <-eventCh:
			if e.Event == memberlist.NodeJoin {
				if myName == e.Node.Name {
					continue
				}
				member := Member{}
				if err := json.Unmarshal(e.Node.Meta, &member); err != nil {
					helper.Fatal("Failed to unmarshal meta data. err:", err)
				}
				if notifyMemberEvent != nil {
					notifyMemberEvent(MemberJoin, member)
				}
				helper.Println(5, "Node:", e.Node.Name, " Join!")
			} else if e.Event == memberlist.NodeLeave {
				member := Member{}
				if err := json.Unmarshal(e.Node.Meta, &member); err != nil {
					helper.Fatal("Failed to unmarshal meta data. err:", err)
				}
				if notifyMemberEvent != nil {
					notifyMemberEvent(MemberLeave, member)
				}
				helper.Println(5, "Node:", e.Node.Name, " Leave!")
			} else {
				helper.Println(0, "The member event is not handled! event:", e.Event)
			}
		}
	}
}

var SetMonLeader func()
var SetMonFollower func()

func GetMyIpAddress(specifyIp string, port int) string {
	if specifyIp != "" {
		return specifyIp + ":" + strconv.Itoa(port)
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
		return ""
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":" + strconv.Itoa(port)
			}
		}
	}
	return ""
}

func Init(isMon bool, isLeader bool, id uint64, advertiseAddr string, memberBindPort int, logger *log.Logger, join string) {
	c := memberlist.DefaultLocalConfig()
	hostname, _ := os.Hostname()
	c.Name = hostname + "-" + uuid.NewUUID().String()
	logger.Println("Memberlist config name:", c.Name)
	c.BindPort = memberBindPort
	c.Logger = logger
	member := Member{}
	member.IsMon = isMon
	member.IsLeader = isLeader
	member.Addr = advertiseAddr
	member.ID = id
	if !isMon {
		member.Name = fmt.Sprintf("osd.%d", id)
	} else {
		member.Name = fmt.Sprintf("%d", id)
	}
	meta, err := json.Marshal(member)
	if err != nil {
		panic("Failed to json member. : " + err.Error())
	}
	helper.Println(5, "Init member:", member)
	mock := &MemberDelegate{meta: meta}
	c.Delegate = mock
	if isMon {
		eventCh = make(chan memberlist.NodeEvent, MEMBER_LIST_CHAN_EVENT_NUM)
		c.Events = &memberlist.ChannelEventDelegate{Ch: eventCh}
	}

	List, err = memberlist.Create(c)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	helper.Printf(5, "Join member addr is %s.", join)
	if join != "" {
		//strs := strings.Split(helper.CONFIG.JoinMemberAddr, ",")
		strs := []string{join}
		_, err := List.Join(strs)
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}

	// Ask for members of the cluster
	for _, member := range List.Members() {
		helper.Printf(0, "Member: %s %s\n", member.Name, member.Addr)
	}

	if isMon {
		go recvChanEvent(c.Name)
	}

	SetMonLeader = func() {
		member.IsLeader = true
		meta, err := json.Marshal(member)
		if err != nil {
			panic("Failed to json member. : " + err.Error())
		}
		mock.meta = meta
		List.UpdateNode(0)
		helper.Println(5, "SetMonPrimary member:", member)
	}

	SetMonFollower = func() {
		member.IsLeader = false
		meta, err := json.Marshal(member)
		if err != nil {
			panic("Failed to json member. : " + err.Error())
		}
		mock.meta = meta
		List.UpdateNode(0)
		helper.Println(5, "SetMonFollower member:", member)
	}

}

func GetMembers() (members []Member) {
	nodes := List.Members()
	for _, node := range nodes {
		member := Member{}
		if err := json.Unmarshal(node.Meta, &member); err != nil {
			helper.Fatal("Failed to unmarshal meta data. err:", err)
		}
		members = append(members, member)
	}
	return
}

func GetMemberByName(name string) *Member {
	for _, v := range GetMembers() {
		if v.Name == name {
			return &v
		}
	}
	return nil
}

func GetOsdMembers() (members []Member) {
	nodes := List.Members()
	for _, node := range nodes {
		member := Member{}
		if err := json.Unmarshal(node.Meta, &member); err != nil {
			helper.Fatal("Failed to unmarshal meta data. err:", err)
		}
		if !member.IsMon {
			members = append(members, member)
		}
	}
	return
}

func GetLeaderMon() *Member {
	for _, v := range GetMembers() {
		//helper.Println(5, "member", v)
		if v.IsLeader == true {
			return &v
		}
	}
	return nil
}

func SetNotifyFunc(callback NotifyMemberEvent) {
	notifyMemberEvent = callback
}
