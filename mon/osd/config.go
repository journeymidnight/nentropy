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
	"flag"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/journeymidnight/nentropy/base"
	"github.com/journeymidnight/nentropy/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"time"
)

var DefaultConfig = Config{
	DebugMode:           false,
	LogLevel:            5,
	WALDir:              "w",
	JoinMon:             false,
	id:                  0,
	node_type:           "",
	MonPort:             7900,
	NumPendingProposals: 2000,
	Tracing:             0.0,
	Monitors:            "",
	MyAddr:              "",
	MaxPendingCount:     1000,
	MemberBindPort:      7946,
	JoinMemberAddr:      "",
	rpcPort:             0,
	enginesCreated:      false,
}

func (c *Config) parseCmdArgs() {

	flag.IntVar(&c.MonPort, "monPort", DefaultConfig.MonPort,
		"Port used by mon for internal communication.")
	flag.StringVar(&c.Monitors, "mons", DefaultConfig.Monitors,
		"IP_ADDRESS:PORT of any healthy peer.")
	flag.IntVar(&c.MemberBindPort, "memberBindPort", 0,
		"Port used by memberlist for internal communication.")
	flag.StringVar(&c.JoinMemberAddr, "joinMemberAddr", DefaultConfig.JoinMemberAddr,
		"a valid member addr to join.")
	flag.IntVar(&c.id, "nodeId", DefaultConfig.id,
		"a unique numbers in cluster [1-256]")
	flag.StringVar(&c.node_type, "nodeType", DefaultConfig.node_type,
		"specify node type [osd/mon].")
	flag.StringVar(&c.AdvertiseAddr, "advertiseAddr", "",
		"specify rpc listen address, like [10.11.11.11:8888]")

	flag.Parse()
	if !flag.Parsed() {
		logger.Fatal(0, "Unable to parse flags")
	}
	//TODO: add argument check here

	c.LogPath = fmt.Sprintf("/var/log/nentropy/%s.%d.log", c.node_type, c.id)
	c.PidFile = fmt.Sprintf("/var/run/nentropy/%s.%d.pid", c.node_type, c.id)
	c.PanicLogPath = fmt.Sprintf("/var/log/nentropy/%s.%d.panic.log", c.node_type, c.id)

}

type Config struct {
	*base.Config
	base.RaftConfig
	id                  int    //[1-256]
	node_type           string //osd or mon
	MonPort             int
	JoinMon             bool
	Tracing             float64
	Monitors            string
	MyAddr              string
	rpcPort             int
	MaxPendingCount     uint64
	NumPendingProposals uint64
	Logger              *log.Logger
	AdvertiseAddr       string
	MemberBindPort      int
	JoinMemberAddr      string
	LogPath             string
	PanicLogPath        string
	PidFile             string
	DebugMode           bool
	LogLevel            int
	WALDir              string
	enginesCreated      bool
}

// MakeConfig returns a Context with default values.
func MakeConfig() *Config {

	cfg := Config{
		Config: new(base.Config),
	}
	cfg.parseCmdArgs()
	cfg.Config.InitDefaults()
	cfg.RaftConfig.SetDefaults()
	return &cfg
}

func (e *Engines) Close() {
	for _, eng := range *e {
		eng.Close()
	}
	*e = nil
}

// CreateEngines creates Engines based on the specs in cfg.Stores.
func (cfg *Config) CreateEngines(ctx context.Context) (Engines, error) {
	engines := Engines(nil)
	defer engines.Close()

	if cfg.enginesCreated {
		return Engines{}, errors.Errorf("engines already created")
	}
	cfg.enginesCreated = true

	var details []string

	details = append(details, fmt.Sprintf("RocksDB cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
	cache := engine.NewRocksDBCache(cfg.CacheSize)
	defer cache.Release()

	var physicalStores int
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory {
			physicalStores++
		}
	}
	openFileLimitPerStore, err := setOpenFileLimit(physicalStores)
	if err != nil {
		return Engines{}, err
	}

	skipSizeCheck := cfg.TestingKnobs.Store != nil &&
		cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs).SkipMinSizeCheck
	for i, spec := range cfg.Stores.Specs {
		var sizeInBytes = spec.SizeInBytes
		if spec.InMemory {
			if spec.SizePercent > 0 {
				sysMem, err := GetTotalMemory(ctx)
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
			details = append(details, fmt.Sprintf("store %d: in-memory, size %s",
				i, humanizeutil.IBytes(sizeInBytes)))
			engines = append(engines, engine.NewInMem(spec.Attributes, sizeInBytes))
		} else {
			if spec.SizePercent > 0 {
				fileSystemUsage := gosigar.FileSystemUsage{}
				if err := fileSystemUsage.Get(spec.Path); err != nil {
					return Engines{}, err
				}
				sizeInBytes = int64(float64(fileSystemUsage.Total) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}

			details = append(details, fmt.Sprintf("store %d: RocksDB, max size %s, max open file limit %d",
				i, humanizeutil.IBytes(sizeInBytes), openFileLimitPerStore))
			rocksDBConfig := engine.RocksDBConfig{
				Attrs:                   spec.Attributes,
				Dir:                     spec.Path,
				MaxSizeBytes:            sizeInBytes,
				MaxOpenFiles:            openFileLimitPerStore,
				WarnLargeBatchThreshold: 500 * time.Millisecond,
				Settings:                cfg.Settings,
			}

			eng, err := engine.NewRocksDB(rocksDBConfig, cache)
			if err != nil {
				return Engines{}, err
			}
			engines = append(engines, eng)
		}
	}

	log.Infof(ctx, "%d storage engine%s initialized",
		len(engines), util.Pluralize(int64(len(engines))))
	for _, s := range details {
		log.Info(ctx, s)
	}
	enginesCopy := engines
	engines = nil
	return enginesCopy, nil
}

const (
	GrpcMaxSize = 256 << 20
)
