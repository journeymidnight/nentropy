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

type Options struct {
	WALDir              string
	WorkerPort          int
	Join                bool
	NumPendingProposals int
	Tracing             float64
	PeerAddr            string
	MyAddr              string
	RaftId              uint64
	MaxPendingCount     uint64
}

// TODO(tzdybal) - remove global
var Config Options

var DefaultConfig = Options{
	WALDir:              "w",
	Join:                false,
	WorkerPort:          12345,
	NumPendingProposals: 2000,
	Tracing:             0.0,
	PeerAddr:            "",
	MyAddr:              "",
	RaftId:              1,
	MaxPendingCount:     1000,
}
