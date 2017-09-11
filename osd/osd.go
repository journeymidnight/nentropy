package main

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"os"
	"os/signal"
	"syscall"
)

var logger *log.Logger

func main() {
	helper.SetupConfig()
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容，55
		}
	}()
	if err := helper.CreatePidfile(helper.CONFIG.PidFile); err != nil {
		fmt.Printf("can not create pid file %s\n", helper.CONFIG.PidFile)
		return
	}
	defer helper.RemovePidfile(helper.CONFIG.PidFile)

	/* log  */
	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.CONFIG.LogPath)
	}
	defer f.Close()
	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger

	/* redirect stdout stderr to log  */
	syscall.Dup2(int(f.Fd()), 2)
	syscall.Dup2(int(f.Fd()), 1)

	list, err := memberlist.Create(memberlist.DefaultLocalConfig())
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	_, err = list.Join([]string{"192.168.35.101"})
	if err != nil {
		panic("Failed to join cluster: " + err.Error())
	}

	// Ask for members of the cluster
	for _, member := range list.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}

	signal.Ignore()
	signalQueue := make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			return
		}
	}

}
