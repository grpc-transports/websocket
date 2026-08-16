package wstransport

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// errClientHandshake is returned by ServerCredentials' ClientHandshake: these
// credentials only exist to surface a server-side upgrade value. Clients dial
// with insecure credentials plus [DialOption] — wss:// carries the security.
var errClientHandshake = errors.New("wstransport: ServerCredentials are server-side only; dial with insecure credentials")

// AuthInfo is the credentials.AuthInfo that [ServerCredentials] attaches to
// every accepted connection. Value is exactly what [ServerConfig.OnUpgrade]
// returned for that connection (nil when no hook was configured), so a service
// method recovers per-connection HTTP state with:
//
//	p, _ := peer.FromContext(ctx)
//	if ai, ok := p.AuthInfo.(wstransport.AuthInfo); ok {
//	    session := ai.Value.(*mySession)
//	}
//
// Use [FromContext] to do the same in one call.
type AuthInfo struct {
	// Value is the value ServerConfig.OnUpgrade attached to the connection.
	Value any
}

// AuthType implements credentials.AuthInfo.
//
// The name reflects where the information comes from — the WebSocket upgrade
// request — not a cryptographic handshake: transport security is provided by
// the wss:// layer underneath, which grpc-go never sees.
func (AuthInfo) AuthType() string { return "wstransport-upgrade" }

// FromContext returns the value [ServerConfig.OnUpgrade] attached to the
// connection carrying ctx's RPC. It reports false when the server was not
// built with [ServerCredentials], or when the peer is not a connection of this
// transport.
func FromContext(ctx context.Context) (any, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	ai, ok := p.AuthInfo.(AuthInfo)
	if !ok {
		return nil, false
	}
	return ai.Value, true
}

// ServerCredentials returns credentials.TransportCredentials that perform no
// handshake of their own and instead publish the [ServerConfig.OnUpgrade]
// value of each accepted connection as [AuthInfo], which grpc-go stores in the
// peer of every RPC on that connection. Pass them to grpc.NewServer:
//
//	h, lis := wstransport.HandlerListener(wstransport.ServerConfig{
//	    OnUpgrade: authenticate, // returns a *mySession, or an error to refuse
//	})
//	gs := grpc.NewServer(grpc.Creds(wstransport.ServerCredentials()))
//
// This is optional sugar: a caller who wants their own AuthInfo type can write
// their own credentials and read the value off the accepted conn with
// [ConnValue].
//
// These credentials do not authenticate anything by themselves and do not
// encrypt: serve wss:// (see [ServerConfig.TLSConfig]) for transport security.
func ServerCredentials() credentials.TransportCredentials { return upgradeCreds{} }

// upgradeCreds implements credentials.TransportCredentials over [ConnValue].
type upgradeCreds struct{}

// ServerHandshake passes the connection straight through, attaching whatever
// OnUpgrade left on it.
func (upgradeCreds) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	v, _ := ConnValue(c)
	return c, AuthInfo{Value: v}, nil
}

// ClientHandshake always fails: these credentials are server-side only.
func (upgradeCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errClientHandshake
}

func (upgradeCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "websocket"}
}

func (c upgradeCreds) Clone() credentials.TransportCredentials { return c }

// OverrideServerName is a no-op: there is no server name to verify here (the
// WebSocket/TLS layer below already did).
func (upgradeCreds) OverrideServerName(string) error { return nil }
