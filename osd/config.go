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
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/log"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/journeymidnight/nentropy/util/tracing"
	"golang.org/x/net/context"
)

const (
	GrpcMaxSize = 256 << 20
)

type Config struct {
	AmbientCtx log.AmbientContext
	helper.Config
	helper.RaftConfig
	Tracer *tracing.Tracer
}

var config Config

// MakeConfig returns a Context with default values.
func MakeConfig() *Config {
	config.Config.InitConfig()
	config.RaftConfig.SetDefaults()
	return &config
}

func (cfg *Config) CreateSysEngine(ctx context.Context) (engine.Engine, error) {
	return nil, nil
}
