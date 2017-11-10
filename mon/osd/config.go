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
	"github.com/journeymidnight/nentropy/log"
)

type Options struct {
	MonPort             int
	JoinMon             bool
	NumPendingProposals int
	Tracing             float64
	Monitors            string
	MyAddr              string
	RaftId              uint64
	MaxPendingCount     uint64
	Logger              *log.Logger
}

var Config Options

const (
	GrpcMaxSize = 256 << 20
)
