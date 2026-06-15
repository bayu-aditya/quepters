// Package grpc implements an OutputConnector that forwards event data to the
// downstream quepters.QueptersService gRPC server.
package grpc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/bayu-aditya/quepters/gen/quepters"
	"github.com/bayu-aditya/quepters/internal/config"
)

// Client is a gRPC OutputConnector.
type Client struct {
	conn   *grpc.ClientConn
	client pb.QueptersServiceClient
}

// New dials the downstream gRPC server. The host may carry an http:// or
// https:// scheme (https selects TLS); any scheme is otherwise stripped for the
// gRPC dialer.
func New(cfg config.GRPCConnector) (*Client, error) {
	target, secure := normalizeTarget(cfg.Host)

	var creds credentials.TransportCredentials
	if secure {
		creds = credentials.NewTLS(nil)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial grpc %q: %w", target, err)
	}

	return &Client{conn: conn, client: pb.NewQueptersServiceClient(conn)}, nil
}

// normalizeTarget strips a URL scheme from host and reports whether TLS should
// be used (https).
func normalizeTarget(host string) (target string, secure bool) {
	switch {
	case strings.HasPrefix(host, "https://"):
		return strings.TrimPrefix(host, "https://"), true
	case strings.HasPrefix(host, "http://"):
		return strings.TrimPrefix(host, "http://"), false
	default:
		return host, false
	}
}

// Forward sends data to the downstream Handler RPC and returns its Ack.
func (c *Client) Forward(ctx context.Context, data []byte) (bool, error) {
	out, err := c.client.Handler(ctx, &pb.HandlerInput{EventData: data})
	if err != nil {
		return false, fmt.Errorf("grpc handler: %w", err)
	}
	return out.GetAck(), nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
