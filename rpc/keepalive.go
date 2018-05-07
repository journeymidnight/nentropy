package rpc

import (
	"time"

	"github.com/journeymidnight/nentropy/helper"
	"google.golang.org/grpc/keepalive"
)

// To prevent unidirectional network partitions from keeping an unhealthy
// connection alive, we use both client-side and server-side keepalive pings.
var ClientKeepalive = keepalive.ClientParameters{
	// Send periodic pings on the connection.
	Time: helper.NetworkTimeout,
	// If the pings don't get a response within the timeout, we might be
	// experiencing a network partition. gRPC will close the transport-level
	// connection and all the pending RPCs (which may not have timeouts) will
	// fail eagerly. gRPC will then reconnect the transport transparently.
	Timeout: helper.NetworkTimeout,
	// Do the pings even when there are no ongoing RPCs.
	PermitWithoutStream: true,
}

var ServerKeepalive = keepalive.ServerParameters{
	// Send periodic pings on the connection.
	Time: helper.NetworkTimeout,
	// If the pings don't get a response within the timeout, we might be
	// experiencing a network partition. gRPC will close the transport-level
	// connection and all the pending RPCs (which may not have timeouts) will
	// fail eagerly.
	Timeout: helper.NetworkTimeout,
}

// By default, gRPC disconnects clients that send "too many" pings,
// but we don't really care about that, so configure the server to be
// as permissive as possible.
var ServerEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}

// These aggressively low keepalive timeouts ensure that tests which use
// them don't take too long.
var ClientTestingKeepalive = keepalive.ClientParameters{
	Time:                200 * time.Millisecond,
	Timeout:             300 * time.Millisecond,
	PermitWithoutStream: true,
}
var ServerTestingKeepalive = keepalive.ServerParameters{
	Time:    200 * time.Millisecond,
	Timeout: 300 * time.Millisecond,
}
