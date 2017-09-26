package memberlist

import (
	member_list "github.com/hashicorp/memberlist"
	"github.com/journeymidnight/nentropy/helper"
	"strings"
)

var List *member_list.Memberlist
var Delegate MonDelegate

type Meta struct {
	NodeType string //osd, mon
	Ip       string
	Port     int
}

func Init() {
	c := member_list.DefaultLocalConfig()
	c.Delegate = &MonDelegate{meta: []byte("mon")}
	List, err := member_list.Create(c)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	mons := strings.Split(helper.CONFIG.Monitors, ",")
	// Join an existing cluster by specifying at least one known member.
	if len(mons) > 0 {
		_, err := List.Join(mons)
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}

	// Ask for members of the cluster
	for _, member := range List.Members() {
		helper.Logger.Printf(5, "Member: %s %s\n", member.Name, member.Addr)
	}
}

type MonDelegate struct {
	meta        []byte
	msgs        [][]byte
	broadcasts  [][]byte
	state       []byte
	remoteState []byte
}

func (m *MonDelegate) NodeMeta(limit int) []byte {
	return m.meta
}

func (m *MonDelegate) NotifyMsg(msg []byte) {
	cp := make([]byte, len(msg))
	copy(cp, msg)
	m.msgs = append(m.msgs, cp)
}

func (m *MonDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	b := m.broadcasts
	m.broadcasts = nil
	return b
}

func (m *MonDelegate) LocalState(join bool) []byte {
	return m.state
}

func (m *MonDelegate) MergeRemoteState(s []byte, join bool) {
	m.remoteState = s
}
