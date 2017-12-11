package rpc

import (
	"fmt"
	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/util/envutil"
	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"math"
	"net"
	"sync"
	"time"
)

const (
	defaultWindowSize     = 65535
	initialWindowSize     = defaultWindowSize * 32 // for an RPC
	initialConnWindowSize = initialWindowSize * 16 // for a connection
)

type connMeta struct {
	sync.Once
	conn    *grpc.ClientConn
	dialErr error
}

// SourceAddr provides a way to specify a source/local address for outgoing
// connections. It should only ever be set by testing code, and is not thread
// safe (so it must be initialized before the server starts).
var SourceAddr = func() net.Addr {
	const envKey = "NENTROPY_SOURCE_IP_ADDRESS"
	if sourceAddr, ok := envutil.EnvString(envKey, 0); ok {
		sourceIP := net.ParseIP(sourceAddr)
		if sourceIP == nil {
			panic(fmt.Sprintf("unable to parse %s '%s' as IP address", envKey, sourceAddr))
		}
		return &net.TCPAddr{
			IP: sourceIP,
		}
	}
	return nil
}()

// Context contains the fields required by the rpc framework.
type Context struct {
	*helper.Config

	Stopper   *stop.Stopper
	masterCtx context.Context

	conns sync.Map
}

// NewContext creates an rpc Context with the supplied values.
func NewContext(
	baseCtx *helper.Config, stopper *stop.Stopper) *Context {
	ctx := &Context{
		Config: baseCtx,
	}
	var cancel context.CancelFunc
	ctx.masterCtx, cancel = context.WithCancel(context.Background())
	ctx.Stopper = stopper

	stopper.RunWorker(ctx.masterCtx, func(context.Context) {
		<-stopper.ShouldQuiesce()

		cancel()
		ctx.conns.Range(func(k, v interface{}) bool {
			meta := v.(*connMeta)
			meta.Do(func() {
				// Make sure initialization is not in progress when we're removing the
				// conn. We need to set the error in case we win the race against the
				// real initialization code.
				if meta.dialErr == nil {
					meta.dialErr = errors.New("Node Unavailable")
				}
			})
			ctx.removeConn(k.(string), meta)
			return true
		})
	})

	return ctx
}

func (ctx *Context) removeConn(key string, meta *connMeta) {
	ctx.conns.Delete(key)
	if conn := meta.conn; conn != nil {
		if err := conn.Close(); err != nil {
			helper.Logger.Printf(5, "failed to close client connection: %s", err)
		}
	}
}

// GRPCDial calls grpc.Dial with the options appropriate for the context.
func (ctx *Context) GRPCDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	value, ok := ctx.conns.Load(target)
	if !ok {
		meta := &connMeta{}
		value, _ = ctx.conns.LoadOrStore(target, meta)
	}

	meta := value.(*connMeta)
	meta.Do(func() {
		var dialOpt grpc.DialOption
		dialOpt = grpc.WithInsecure()
		var dialOpts []grpc.DialOption
		dialOpts = append(dialOpts, dialOpt)
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		))
		dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Send periodic pings on the connection.
			Time: helper.NetworkTimeout,
			// If the pings don't get a response within the timeout, we might be
			// experiencing a network partition. gRPC will close the transport-level
			// connection and all the pending RPCs (which may not have timeouts) will
			// fail eagerly. gRPC will then reconnect the transport transparently.
			Timeout: helper.NetworkTimeout,
			// Do the pings even when there are no ongoing RPCs.
			PermitWithoutStream: true,
		}))
		dialOpts = append(dialOpts,
			grpc.WithInitialWindowSize(initialWindowSize),
			grpc.WithInitialConnWindowSize(initialConnWindowSize))
		dialOpts = append(dialOpts, opts...)

		if SourceAddr != nil {
			dialOpts = append(dialOpts, grpc.WithDialer(
				func(addr string, timeout time.Duration) (net.Conn, error) {
					dialer := net.Dialer{
						Timeout:   timeout,
						LocalAddr: SourceAddr,
					}
					return dialer.Dial("tcp", addr)
				},
			))
		}

		helper.Logger.Println(5, fmt.Sprintf("dialing %s", target))

		meta.conn, meta.dialErr = grpc.DialContext(ctx.masterCtx, target, dialOpts...)
	})

	return meta.conn, meta.dialErr
}

// NewServer is a thin wrapper around grpc.NewServer that registers a heartbeat
// service.
func NewServer() *grpc.Server {
	opts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		// Adjust the stream and connection window sizes. The gRPC defaults are too
		// low for high latency connections.
		grpc.InitialWindowSize(initialWindowSize),
		grpc.InitialConnWindowSize(initialConnWindowSize),
		// The default number of concurrent streams/requests on a client connection
		// is 100, while the server is unlimited. The client setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the client or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
		// By default, gRPC disconnects clients that send "too many" pings,
		// but we don't really care about that, so configure the server to be
		// as permissive as possible.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Nanosecond,
			PermitWithoutStream: true,
		}),
	}
	s := grpc.NewServer(opts...)
	return s
}
