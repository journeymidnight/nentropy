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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package multiraft

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/journeymidnight/nentropy/rpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/memberlist"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/journeymidnight/nentropy/storage/engine"
	"github.com/pkg/errors"
	"sync"
)

const (
	// Outgoing messages are queued per-node on a channel of this size.
	//
	// TODO(peter): The normal send buffer size is larger than we would like. It
	// is a temporary patch for the issue discussed in #8630 where
	// Store.HandleRaftRequest can block applying a preemptive snapshot for a
	// long enough period of time that grpc flow control kicks in and messages
	// are dropped on the sending side.
	raftSendBufferSize = 10000

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	//
	// TODO(tamird): make culling of outbound streams more evented, so that we
	// need not rely on this timeout to shut things down.
	raftIdleTimeout       = time.Minute
	maxBackoff            = time.Second
	NetworkTimeout        = 3 * time.Second
	defaultWindowSize     = 65535
	initialWindowSize     = defaultWindowSize * 32 // for an RPC
	initialConnWindowSize = initialWindowSize * 16 // for a connection
)

// RaftMessageResponseStream is the subset of the
// MultiRaft_RaftMessageServer interface that is needed for sending responses.
type RaftMessageResponseStream interface {
	Context() context.Context
	Send(*multiraftbase.RaftMessageResponse) error
}

// lockedRaftMessageResponseStream is an implementation of
// RaftMessageResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedRaftMessageResponseStream struct {
	multiraftbase.MultiRaft_RaftMessageBatchServer
	sendMu syncutil.Mutex
}

func (s *lockedRaftMessageResponseStream) Send(resp *multiraftbase.RaftMessageResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.MultiRaft_RaftMessageBatchServer.Send(resp)
}

// SnapshotResponseStream is the subset of the
// MultiRaft_RaftSnapshotServer interface that is needed for sending responses.
type SnapshotResponseStream interface {
	Context() context.Context
	Send(*multiraftbase.SnapshotResponse) error
	Recv() (*multiraftbase.SnapshotRequest, error)
}

// RaftMessageHandler is the interface that must be implemented by
// arguments to RaftTransport.Listen.
type RaftMessageHandler interface {
	// HandleRaftRequest is called for each incoming Raft message. If it returns
	// an error it will be streamed back to the sender of the message as a
	// RaftMessageResponse. If the stream parameter is nil the request should be
	// processed synchronously. If the stream is non-nil the request can be
	// processed asynchronously and any error should be sent on the stream.
	HandleRaftRequest(ctx context.Context, req *multiraftbase.RaftMessageRequest,
		respStream RaftMessageResponseStream) *multiraftbase.Error

	// HandleRaftResponse is called for each raft response. Note that
	// not all messages receive a response. An error is returned if and only if
	// the underlying Raft connection should be closed.
	HandleRaftResponse(context.Context, *multiraftbase.RaftMessageResponse) error

	// HandleSnapshot is called for each new incoming snapshot stream, after
	// parsing the initial SnapshotRequest_Header on the stream.
	HandleSnapshot(header *multiraftbase.SnapshotRequest_Header, respStream SnapshotResponseStream) error
}

// NodeAddressResolver is the function used by RaftTransport to map node IDs to
// network addresses.
type NodeAddressResolver func(id string) (string, error)

type raftTransportStats struct {
	nodeID        multiraftbase.NodeID //like osd.1 or mon.1
	queue         int
	queueMax      int32
	clientSent    int64
	clientRecv    int64
	clientDropped int64
	serverSent    int64
	serverRecv    int64
}

type raftTransportStatsSlice []*raftTransportStats

func (s raftTransportStatsSlice) Len() int           { return len(s) }
func (s raftTransportStatsSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s raftTransportStatsSlice) Less(i, j int) bool { return s[i].nodeID < s[j].nodeID }

// RaftTransport handles the rpc messages for raft.
//
// The raft transport is asynchronous with respect to the caller, and
// internally multiplexes outbound messages. Internally, each message is
// queued on a per-destination queue before being asynchronously delivered.
//
// Callers are required to construct a RaftSender before being able to
// dispatch messages, and must provide an error handler which will be invoked
// asynchronously in the event that the recipient of any message closes its
// inbound RPC stream. This callback is asynchronous with respect to the
// outbound message which caused the remote to hang up; all that is known is
// which remote hung up.
type RaftTransport struct {
	helper.AmbientContext
	resolver   NodeAddressResolver
	rpcContext *rpc.Context
	queues     sync.Map // map[string]*chan *RaftMessageRequest
	stats      sync.Map // map[string]*raftTransportStats
	handler    RaftMessageHandler
	conns      sync.Map
}

// GossipAddressResolver is a thin wrapper around gossip's GetNodeIDAddress
// that allows its return value to be used as the net.Addr interface.
func GossipAddressResolver() NodeAddressResolver {
	return func(nodeID string) (string, error) {
		member := memberlist.GetMemberByName(nodeID)
		if member != nil {
			return memberlist.GetMemberByName(nodeID).Addr, nil
		} else {
			return "", errors.New("Can not get node address.")
		}

	}
}

// NewRaftTransport creates a new RaftTransport.
func NewRaftTransport(
	ambient helper.AmbientContext,
	resolver NodeAddressResolver,
	grpcServer *grpc.Server,
	rpcContext *rpc.Context,
) *RaftTransport {

	t := &RaftTransport{
		AmbientContext: ambient,
		resolver:       resolver,
		rpcContext:     rpcContext,
	}

	if grpcServer != nil {
		multiraftbase.RegisterMultiRaftServer(grpcServer, t)
	}
	if t.rpcContext != nil {
		ctx := context.Background()
		t.rpcContext.Stopper.RunWorker(ctx, func(ctx context.Context) {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			lastStats := make(map[multiraftbase.NodeID]raftTransportStats)
			lastTime := timeutil.Now()
			var stats raftTransportStatsSlice
			for {
				select {
				case <-ticker.C:
					stats = stats[:0]
					t.stats.Range(func(k, v interface{}) bool {
						s, ok := v.(*raftTransportStats)
						if !ok {
							return false
						}
						s.queue = 0
						stats = append(stats, s)
						return true
					})

					t.queues.Range(func(k, v interface{}) bool {
						value, ok := v.(*chan *multiraftbase.RaftMessageRequest)
						if !ok {
							return false
						}
						ch := *value
						key, ok := k.(multiraftbase.NodeID)
						if !ok {
							return false
						}
						t.getStats(key).queue += len(ch)
						return true
					})

					now := timeutil.Now()
					elapsed := now.Sub(lastTime).Seconds()
					sort.Sort(stats)

					var buf bytes.Buffer
					// NB: The header is 80 characters which should display in a single
					// line on most terminals.
					fmt.Fprintf(&buf,
						"         qlen   qmax   qdropped client-sent client-recv server-sent server-recv\n")
					for _, s := range stats {
						last := lastStats[s.nodeID]
						cur := raftTransportStats{
							nodeID:        s.nodeID,
							queue:         s.queue,
							queueMax:      atomic.LoadInt32(&s.queueMax),
							clientDropped: atomic.LoadInt64(&s.clientDropped),
							clientSent:    atomic.LoadInt64(&s.clientSent),
							clientRecv:    atomic.LoadInt64(&s.clientRecv),
							serverSent:    atomic.LoadInt64(&s.serverSent),
							serverRecv:    atomic.LoadInt64(&s.serverRecv),
						}
						fmt.Fprintf(&buf, "  %3d: %6d %6d %10d %11.1f %11.1f %11.1f %11.1f\n",
							cur.nodeID, cur.queue, cur.queueMax, cur.clientDropped,
							float64(cur.clientSent-last.clientSent)/elapsed,
							float64(cur.clientRecv-last.clientRecv)/elapsed,
							float64(cur.serverSent-last.serverSent)/elapsed,
							float64(cur.serverRecv-last.serverRecv)/elapsed)
						lastStats[s.nodeID] = cur
					}
					lastTime = now
					//helper.Printf(5, "stats:\n%s", buf.String())
				case <-ctx.Done():
					return
				}
			}
		})
	}

	return t
}

func (t *RaftTransport) queuedMessageCount() int64 {
	var n int64
	t.queues.Range(func(k, v interface{}) bool {
		value, ok := v.(*chan *multiraftbase.RaftMessageRequest)
		if !ok {
			return false
		}
		ch := *value
		n += int64(len(ch))
		return true
	})
	return n
}

func (t *RaftTransport) getHandler() (RaftMessageHandler, bool) {
	if t.handler != nil {
		return t.handler, true
	} else {
		return nil, false
	}
}

// handleRaftRequest proxies a request to the listening server interface.
func (t *RaftTransport) handleRaftRequest(
	ctx context.Context, req *multiraftbase.RaftMessageRequest, respStream RaftMessageResponseStream,
) *multiraftbase.Error {
	handler, ok := t.getHandler()
	if !ok {
		helper.Printf(5, "unable to accept Raft message from %+v: no handler registered for %+v",
			req.FromReplica, req.ToReplica)
		return multiraftbase.NewError(multiraftbase.NewNodeNotReadyError(req.ToReplica.NodeID))
	}

	return handler.HandleRaftRequest(ctx, req, respStream)
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(req *multiraftbase.RaftMessageRequest, err *multiraftbase.Error) *multiraftbase.RaftMessageResponse {
	resp := &multiraftbase.RaftMessageResponse{
		GroupID: req.GroupID,
		// From and To are reversed in the response.
		ToReplica:   req.FromReplica,
		FromReplica: req.ToReplica,
	}
	if err != nil {
		resp.Union.Error = err
	}
	return resp
}

func (t *RaftTransport) getStats(nodeID multiraftbase.NodeID) *raftTransportStats {
	value, ok := t.stats.Load(nodeID)
	if ok == false {
		stats := &raftTransportStats{nodeID: nodeID}
		value, _ = t.stats.LoadOrStore(nodeID, stats)
	}
	ch, ok := value.(*raftTransportStats)
	if !ok {
		return nil
	}
	return ch
}

// RaftMessageBatch proxies the incoming requests to the listening server interface.
func (t *RaftTransport) RaftMessageBatch(stream multiraftbase.MultiRaft_RaftMessageBatchServer) error {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	if err := t.rpcContext.Stopper.RunTask(
		stream.Context(), "storage.RaftTransport: processing batch",
		func(ctx context.Context) {
			t.rpcContext.Stopper.RunWorker(ctx, func(ctx context.Context) {
				errCh <- func() error {
					var stats *raftTransportStats
					stream := &lockedRaftMessageResponseStream{MultiRaft_RaftMessageBatchServer: stream}
					for {
						batch, err := stream.Recv()
						if err != nil {
							helper.Println(5, "Grpc Server stream received a error.", err)
							return err
						}

						if len(batch.Requests) == 0 {
							continue
						}

						if stats == nil {
							stats = t.getStats(batch.Requests[0].FromReplica.NodeID)
						}

						for i := range batch.Requests {
							req := &batch.Requests[i]

							atomic.AddInt64(&stats.serverRecv, 1)
							if pErr := t.handleRaftRequest(ctx, req, stream); pErr != nil {
								atomic.AddInt64(&stats.serverSent, 1)
								if err := stream.Send(newRaftMessageResponse(req, pErr)); err != nil {
									return err
								}
							}
						}
					}
				}()
			})
		}); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		helper.Println(5, "RaftMessageBatch return, err:", err)
		return err
	}
}

// Listen registers a raftMessageHandler to receive proxied messages.
func (t *RaftTransport) Listen(handler RaftMessageHandler) {
	t.handler = handler
}

// Stop unregisters a raftMessageHandler.
func (t *RaftTransport) Stop() {
	t.handler = nil
}

// connectAndProcess connects to the node and then processes the
// provided channel containing a queue of raft messages until there is
// an unrecoverable error with the underlying connection. A circuit
// breaker is used to allow fast failures in SendAsync which will drop
// incoming raft messages and report unreachable status to the raft group.
func (t *RaftTransport) connectAndProcess(
	ctx context.Context,
	nodeID multiraftbase.NodeID,
	ch chan *multiraftbase.RaftMessageRequest,
	stats *raftTransportStats,
) {
	if err := func() error {
		addr, err := t.resolver(string(nodeID))
		if err != nil {
			helper.Printf(5, "Cannot get nodeID %s address: %s", nodeID, addr)
			return err
		}
		conn, err := t.rpcContext.GRPCDial(addr, grpc.WithBlock())
		if err != nil {
			helper.Printf(5, "Error dialing to host %s!", addr)
			return err
		}
		client := multiraftbase.NewMultiRaftClient(conn)
		batchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.RaftMessageBatch(batchCtx)
		if err != nil {
			helper.Println(5, "Error calling RaftMessageBatch! err:", err)
			return err
		}
		return t.processQueue(nodeID, ch, stats, stream)
	}(); err != nil {
		helper.Printf(5, "Grpc stream to node %d failed, %s", nodeID, err)
	}
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *RaftTransport) processQueue(
	nodeID multiraftbase.NodeID,
	ch chan *multiraftbase.RaftMessageRequest,
	stats *raftTransportStats,
	stream multiraftbase.MultiRaft_RaftMessageBatchClient,
) error {
	helper.Printf(5, "Enter processQueue in RaftTransport")
	errCh := make(chan error, 1)
	// Starting workers in a task prevents data races during shutdown.
	if err := t.rpcContext.Stopper.RunTask(
		stream.Context(), "storage.RaftTransport: processing queue",
		func(ctx context.Context) {
			t.rpcContext.Stopper.RunWorker(ctx, func(ctx context.Context) {
				errCh <- func() error {
					for {
						resp, err := stream.Recv()
						if err != nil {
							helper.Println(5, "Client stream receiver err :", err)
							return err
						}
						atomic.AddInt64(&stats.clientRecv, 1)
						handler, ok := t.getHandler()
						if !ok {
							continue
						}
						if err := handler.HandleRaftResponse(ctx, resp); err != nil {
							helper.Printf(5, "Error handling raft response. err:", err)
							return err
						}
					}
				}()
			})
		}); err != nil {
		return err
	}

	var raftIdleTimer timeutil.Timer
	defer raftIdleTimer.Stop()
	batch := &multiraftbase.RaftMessageRequestBatch{}
	for {
		raftIdleTimer.Reset(raftIdleTimeout)
		select {
		case <-t.rpcContext.Stopper.ShouldStop():
			return nil
		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			return nil
		case err := <-errCh:
			return err
		case req := <-ch:
			batch.Requests = append(batch.Requests, *req)

			// Pull off as many queued requests as possible.
			//
			// TODO(peter): Think about limiting the size of the batch we send.
			for done := false; !done; {
				select {
				case req = <-ch:
					batch.Requests = append(batch.Requests, *req)
				default:
					done = true
				}
			}
			err := stream.Send(batch)
			batch.Requests = batch.Requests[:0]

			atomic.AddInt64(&stats.clientSent, 1)
			if err != nil {
				return err
			}
		}
	}
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *RaftTransport) getQueue(nodeID multiraftbase.NodeID) (chan *multiraftbase.RaftMessageRequest, bool) {
	value, exist := t.queues.Load(nodeID)
	var ok bool
	if !exist {
		ch := make(chan *multiraftbase.RaftMessageRequest, raftSendBufferSize)
		helper.Println(20, "New a channel for raft req, ch:", ch)
		value, ok = t.queues.LoadOrStore(nodeID, ch)
	}
	ch, ok := value.(chan *multiraftbase.RaftMessageRequest)
	if !ok {
		return nil, false
	}
	return ch, exist
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full and calls s.onError when the
// recipient closes the stream.
func (t *RaftTransport) SendAsync(req *multiraftbase.RaftMessageRequest) bool {
	if req.GroupID == "" && len(req.Heartbeats) == 0 && len(req.HeartbeatResps) == 0 {
		// Coalesced heartbeats are addressed to range 0; everything else
		// needs an explicit range ID.
		panic("only messages with coalesced heartbeats or heartbeat responses may be sent to range ID 0")
	}
	if req.Message.Type == raftpb.MsgSnap {
		panic("snapshots must be sent using SendSnapshot")
	}
	toNodeID := req.ToReplica.NodeID

	stats := t.getStats(toNodeID)
	helper.Println(20, "Enter SendAsync(). toNodeID:", toNodeID)
	ch, existingQueue := t.getQueue(toNodeID)
	if !existingQueue {
		// Starting workers in a task prevents data races during shutdown.
		ctx := t.AnnotateCtx(context.Background())
		if err := t.rpcContext.Stopper.RunTask(
			ctx, "storage.RaftTransport: sending message",
			func(ctx context.Context) {
				t.rpcContext.Stopper.RunWorker(ctx, func(ctx context.Context) {
					t.connectAndProcess(ctx, toNodeID, ch, stats)
					t.queues.Delete(toNodeID)
				})
			}); err != nil {
			helper.Printf(20, "Error running rpc client.")
			t.queues.Delete(toNodeID)
			return false
		}
	}

	select {
	case ch <- req:
		l := int32(len(ch))
		if v := atomic.LoadInt32(&stats.queueMax); v < l {
			atomic.CompareAndSwapInt32(&stats.queueMax, v, l)
		}
		return true
	default:
		atomic.AddInt64(&stats.clientDropped, 1)
		return false
	}
}

// SendSnapshot streams the given outgoing snapshot. The caller is responsible
// for closing the OutgoingSnapshot.
func (t *RaftTransport) SendSnapshot(
	ctx context.Context,
	header multiraftbase.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() engine.Batch,
	sent func(),
) error {
	var stream multiraftbase.MultiRaft_RaftSnapshotClient
	nodeID := header.RaftMessageRequest.ToReplica.NodeID
	if err := func() error {
		addr, err := t.resolver(string(nodeID))
		if err != nil {
			return err
		}
		conn, err := t.rpcContext.GRPCDial(addr, grpc.WithBlock())
		if err != nil {
			return err
		}
		client := multiraftbase.NewMultiRaftClient(conn)
		stream, err = client.RaftSnapshot(ctx)
		return err
	}(); err != nil {
		return err
	}

	// Note that we intentionally do not open the breaker if we encounter an
	// error while sending the snapshot. Snapshots are much less common than
	// regular Raft messages so if there is a real problem with the remote we'll
	// probably notice it really soon. Conversely, snapshots are a bit weird
	// (much larger than regular messages) so if there is an error here we don't
	// want to disrupt normal messages.
	defer func() {
		if err := stream.CloseSend(); err != nil {
			helper.Printf(5, "failed to close snapshot stream: %s", err)
		}
	}()
	return sendSnapshot(ctx, stream, header, snap, sent)
}

// RaftSnapshot handles incoming streaming snapshot requests.
func (t *RaftTransport) RaftSnapshot(stream multiraftbase.MultiRaft_RaftSnapshotServer) error {
	errCh := make(chan error, 1)
	if err := t.rpcContext.Stopper.RunAsyncTask(
		stream.Context(), "storage.RaftTransport: processing snapshot",
		func(ctx context.Context) {
			errCh <- func() error {
				req, err := stream.Recv()
				if err != nil {
					return err
				}
				if req.Header == nil {
					return stream.Send(&multiraftbase.SnapshotResponse{
						Status:  multiraftbase.SnapshotResponse_ERROR,
						Message: "client error: no header in first snapshot request message"})
				}
				rmr := req.Header.RaftMessageRequest
				handler, ok := t.getHandler()
				if !ok {
					helper.Printf(5, "unable to accept Raft message from %+v: no handler registered for %+v",
						rmr.FromReplica, rmr.ToReplica)
					return multiraftbase.NewStoreNotFoundError(rmr.ToReplica.StoreID)
				}
				return handler.HandleSnapshot(req.Header, stream)
			}()
		}); err != nil {
		return err
	}
	select {
	case <-t.rpcContext.Stopper.ShouldStop():
		return nil
	case err := <-errCh:
		return err
	}
}
