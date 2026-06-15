// Package connector defines the input/output abstractions that the adapter
// dispatch logic is built on. New transports are added by implementing
// InputConnector or OutputConnector; the core never needs to change.
package connector

import "context"

//go:generate mockery

// ProcessFunc handles a single message payload. It returns ack=true when the
// message was successfully forwarded and should be acknowledged, or ack=false
// (or a non-nil error) when it should be negatively acknowledged for redelivery.
type ProcessFunc func(ctx context.Context, data []byte) (ack bool, err error)

// InputConnector consumes messages from a source and drives a ProcessFunc for
// each one, acknowledging or negatively acknowledging based on the result.
type InputConnector interface {
	// Consume blocks, pulling messages and invoking process for each, until ctx
	// is cancelled. Messages already pulled when ctx is cancelled are still
	// processed so they can complete during graceful shutdown.
	Consume(ctx context.Context, process ProcessFunc) error
	// Close releases the connector's resources.
	Close() error
}

// OutputConnector forwards event data to a downstream sink and reports whether
// the sink acknowledged it.
type OutputConnector interface {
	// Forward sends data downstream and returns whether it was acknowledged.
	Forward(ctx context.Context, data []byte) (ack bool, err error)
	// Close releases the connector's resources.
	Close() error
}
