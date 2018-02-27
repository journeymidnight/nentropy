// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package client

import (
	"golang.org/x/net/context"

	"github.com/journeymidnight/nentropy/osd/multiraftbase"
)

// Sender is the interface used to call into a Cockroach instance.
// If the returned *roachpb.Error is not nil, no response should be returned.
type Sender interface {
	Send(context.Context, multiraftbase.BatchRequest) (*multiraftbase.BatchResponse, *multiraftbase.Error)
}

// SenderFunc is an adapter to allow the use of ordinary functions
// as Senders.
type SenderFunc func(context.Context, multiraftbase.BatchRequest) (*multiraftbase.BatchResponse, *multiraftbase.Error)

// Send calls f(ctx, c).
func (f SenderFunc) Send(
	ctx context.Context, ba multiraftbase.BatchRequest,
) (*multiraftbase.BatchResponse, *multiraftbase.Error) {
	return f(ctx, ba)
}
