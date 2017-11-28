// Copyright 2016 The Cockroach Authors.
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

package helper

import (
	"golang.org/x/net/context"
)

// AmbientContext is a helper type used to "annotate" context.Contexts with log
// tags and a Tracer or EventLog. It is intended to be embedded into various
// server components.
type AmbientContext struct {
	// Cached annotated version of context.{TODO,Background}, to avoid annotating
	// these contexts repeatedly.
	backgroundCtx context.Context
}

// AnnotateCtx annotates a given context with the information in AmbientContext:
//  - the EventLog is embedded in the context if the context doesn't already
//    have an event log or an open trace.
//  - the log tags in AmbientContext are added (if ctx doesn't already have
//  them). If the tags already exist, the values from the AmbientContext
//  overwrite the existing values, but the order of the tags might change.
//
// For background operations, context.Background() should be passed; however, in
// that case it is strongly recommended to open a span if possible (using
// AnnotateCtxWithSpan).
func (ac *AmbientContext) AnnotateCtx(ctx context.Context) context.Context {
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			return ac.backgroundCtx
		}
		return ctx
	default:
		return ctx
	}
}

// ResetAndAnnotateCtx annotates a given context with the information in
// AmbientContext, but unlike AnnotateCtx, it drops all log tags in the
// supplied context before adding the ones from the AmbientContext.
func (ac *AmbientContext) ResetAndAnnotateCtx(ctx context.Context) context.Context {
	switch ctx {
	case context.TODO(), context.Background():
		// NB: context.TODO and context.Background are identical except for their
		// names.
		if ac.backgroundCtx != nil {
			return ac.backgroundCtx
		}
		return ctx
	default:
		return ctx
	}
}

// TODO(radu): remove once they start getting used.
var _ = AmbientContext{}.Tracer
var _ = (*AmbientContext).AddLogTagInt
var _ = (*AmbientContext).AddLogTagInt64
var _ = (*AmbientContext).AddLogTagStr
