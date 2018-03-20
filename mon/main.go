/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/storage/engine"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	logger *log.Logger
	cfg    *Config

	eng engine.Engine
)

func getMonDataDir() (string, error) {
	dir, err := helper.GetDataDir(config.BaseDir, config.RaftId, true)
	if err != nil {
		return "", err
	}
	return dir + "/sys-data", nil
}

func initStorage() {
	dir, err := getMonDataDir()
	if err != nil {
		helper.Fatal("Error creating data dir! err:", err)
	}
	opt := engine.KVOpt{Dir: dir}
	eng, err = engine.NewBadgerDB(&opt)
	helper.Checkf(err, "Error while creating badger KV WAL store")
}

func disposeStorage() {
	eng.Close()
}

func newGrpcServer() *grpc.Server {
	var opts []grpc.ServerOption
	// By default Go GRPC traces all requests.
	// grpc.EnableTracing = false
	return grpc.NewServer(opts...)
}

func getIpAndPort(mons string, id int) (string, string, []string) {
	myAddr := ""
	peers := strings.Split(mons, ",")
	for i, v := range peers {
		if (i + 1) == id {
			myAddr = v
		}
	}
	if myAddr == "" {
		panic("Cannot parse my addr.")
	}
	s := strings.Split(myAddr, ":")
	if len(s) != 2 {
		panic("No ip or port for myself")
	}
	return s[0], s[1], peers
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg = MakeConfig()
	logger = helper.Logger

	ip, port, peers := getIpAndPort(cfg.Monitors, int(cfg.RaftId))
	helper.Println(5, "ip:", ip, " port:", port, " peers:", peers)
	initStorage()
	defer disposeStorage()

	grpcSrv := newGrpcServer()

	go StartRaftNodes(eng, grpcSrv, peers, ip+":"+port)

	helper.Println(5, "raftid, advertiseaddr", cfg.RaftId, cfg.AdvertiseAddr)
	memberlist.Init(true, false, cfg.RaftId, cfg.AdvertiseAddr, cfg.MemberBindPort, logger.Logger, cfg.JoinMemberAddr)
	memberlist.SetNotifyFunc(NotifyMemberEvent)

	runServer(grpcSrv)

	laddr := "0.0.0.0"
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%s", laddr, port))
	if err != nil {
		helper.Fatalf("While running server: %v", err)
		return
	}
	go grpcSrv.Serve(ln)

	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)
	defer close(sdCh)
	// sigint : Ctrl-C, sigquit : Ctrl-\ (backslash), sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	for {
		select {
		case _, ok := <-sdCh:
			if !ok {
				return
			}
			os.Exit(1) // temporarily add
		}
	}
}
