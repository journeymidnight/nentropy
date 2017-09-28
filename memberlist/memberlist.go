package memberlist

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/pborman/uuid"
	"net"
	"os"
	"strings"
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
	Name string
	Addr net.IP
	Port uint16
	Meta []byte // Metadata from the delegate for this node.
}

type NotifyMemberEvent func(MemberEventType, Member) error

var notifyMemberEvent NotifyMemberEvent
var list *memberlist.Memberlist
var eventCh chan memberlist.NodeEvent

type Meta struct {
	IsMon bool //osd, mon
	Ip    string
	Port  int
}

type MemberDelegate struct {
	meta        Meta
	msgs        [][]byte
	broadcasts  [][]byte
	state       []byte
	remoteState []byte
}

func (m *MemberDelegate) NodeMeta(limit int) []byte {
	b, err := json.Marshal(m.meta)
	if err != nil {
		helper.Logger.Println(0, "Failed to encode meta data!")
	}
	return b
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

func recvChanEvent() {
	for {
		select {
		case e := <-eventCh:
			if e.Event == memberlist.NodeJoin {
				member := Member{Name: e.Node.Name, Meta: e.Node.Meta}
				notifyMemberEvent(MemberJoin, member)
				//callback
			} else if e.Event == memberlist.NodeLeave {
				member := Member{Name: e.Node.Name, Meta: e.Node.Meta}
				notifyMemberEvent(MemberLeave, member)
			} else {
				helper.Logger.Println(0, "The member event is not handled! event:", e.Event)
			}
		}
	}
}

func Init(isMon bool, myAddr string) {
	c := memberlist.DefaultLocalConfig()
	hostname, _ := os.Hostname()
	c.Name = hostname + "-" + uuid.NewUUID().String()
	c.BindPort = 0 //helper.CONFIG.MemberBindPort
	if isMon {
		eventCh = make(chan memberlist.NodeEvent, MEMBER_LIST_CHAN_EVENT_NUM)
		c.Events = &memberlist.ChannelEventDelegate{Ch: eventCh}
		c.Delegate = &MemberDelegate{meta: Meta{IsMon: true}}
	} else {
		c.Delegate = &MemberDelegate{meta: Meta{IsMon: false}}
	}

	list, err := memberlist.Create(c)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	if helper.CONFIG.JoinMemberAddr != "" {
		_, err := list.Join([]string{helper.CONFIG.JoinMemberAddr})
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	} else {
		mons := strings.Split(helper.CONFIG.Monitors, ",")
		if len(mons) > 0 {
			var member []string
			for _, mon := range mons {
				addrPort := strings.Split(mon, ":")
				monAddr := addrPort[0]
				if monAddr == myAddr {
					continue
				}
				member = append(member, fmt.Sprint("%s:%d", monAddr, helper.CONFIG.MemberBindPort))
			}
			_, err := list.Join(member)
			if err != nil {
				panic("Failed to join cluster: " + err.Error())
			}
		} else {
			panic("Failed to get monitors!")
		}
	}

	// Ask for members of the cluster
	for _, member := range list.Members() {
		helper.Logger.Printf(0, "Member: %s %s\n", member.Name, member.Addr)
	}

	if isMon {
		go recvChanEvent()
	}
}

func GetMembers() (member []Member) {
	nodes := list.Members()
	for _, node := range nodes {
		member = append(member, Member{Name: node.Name, Addr: node.Addr, Port: node.Port, Meta: node.Meta})
	}
	return
}

func SetNotifyFunc(callback NotifyMemberEvent) {
	notifyMemberEvent = callback
}
