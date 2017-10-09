package main

import (
	"flag"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/memberlist"
	"os"
	"time"
)

var (
	logger *log.Logger
)

func setupConfigOpts() {
	helper.CONFIG = helper.DefaultOption

	flag.IntVar(&helper.CONFIG.MonPort, "monPort", helper.DefaultOption.MonPort,
		"Port used by mon for internal communication.")
	flag.StringVar(&helper.CONFIG.Monitors, "mons", helper.DefaultOption.Monitors,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.IntVar(&helper.CONFIG.MemberBindPort, "memberBindPort", helper.DefaultOption.MemberBindPort,
		"Port used by memberlist for internal communication.")
	flag.StringVar(&helper.CONFIG.JoinMemberAddr, "joinMemberAddr", helper.DefaultOption.JoinMemberAddr,
		"a valid member addr to join.")
	flag.Uint64Var(&helper.CONFIG.RaftId, "idx", helper.DefaultOption.RaftId,
		"RAFT ID that this server will use to join RAFT cluster.")

	flag.Parse()
	if !flag.Parsed() {
		logger.Fatal(0, "Unable to parse flags")
	}
}

func main() {
	setupConfigOpts() // flag.Parse is called here.
	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger = log.New(os.Stdout, "[nentropy]", log.LstdFlags, helper.CONFIG.LogLevel)
		logger.Print(0, "Failed to open log file "+helper.CONFIG.LogPath)
	} else {
		defer f.Close()
		logger = log.New(f, "[nentropy]", log.LstdFlags, helper.CONFIG.LogLevel)
	}
	helper.Logger = logger

	memberlist.Init(false, helper.CONFIG.RaftId, helper.CONFIG.MyAddr, logger.Logger)

	for {
		time.Sleep(time.Second)
	}
}
