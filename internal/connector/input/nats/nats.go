// Package nats implements an InputConnector backed by a NATS JetStream pull
// consumer. It binds to an existing durable consumer and never creates one.
package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"

	"github.com/bayu-aditya/quepters/internal/config"
	"github.com/bayu-aditya/quepters/internal/connector"
)

const (
	// fetchBatch is the maximum number of messages pulled per Fetch call.
	fetchBatch = 64
	// fetchMaxWait bounds how long a Fetch waits for the batch to fill, keeping
	// the consume loop responsive to context cancellation.
	fetchMaxWait = 5 * time.Second
)

// Consumer is a JetStream pull-consumer InputConnector.
type Consumer struct {
	nc       *nats.Conn
	consumer jetstream.Consumer
	log      *logrus.Entry
}

// New connects to NATS, locates the existing durable consumer named by
// endpoint.SubscriptionID and returns a ready InputConnector. When
// endpoint.Stream is empty the owning stream is discovered automatically.
func New(ctx context.Context, conn config.NATSConnector, endpoint config.NATSEndpoint, log *logrus.Logger) (*Consumer, error) {
	nc, err := nats.Connect(normalizeURL(conn.Host), nats.UserInfo(conn.User, conn.Pass))
	if err != nil {
		return nil, fmt.Errorf("connect nats %q: %w", conn.Host, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	stream := endpoint.Stream
	if stream == "" {
		stream, err = discoverStream(ctx, js, endpoint.SubscriptionID)
		if err != nil {
			nc.Close()
			return nil, err
		}
	}

	cons, err := js.Consumer(ctx, stream, endpoint.SubscriptionID)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("bind consumer %q on stream %q: %w", endpoint.SubscriptionID, stream, err)
	}

	return &Consumer{
		nc:       nc,
		consumer: cons,
		log:      log.WithField("subscription_id", endpoint.SubscriptionID),
	}, nil
}

// normalizeURL ensures a NATS scheme is present.
func normalizeURL(host string) string {
	if strings.Contains(host, "://") {
		return host
	}
	return "nats://" + host
}

// discoverStream scans the server's streams for the one that owns consumer.
func discoverStream(ctx context.Context, js jetstream.JetStream, consumer string) (string, error) {
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		if _, err := js.Consumer(ctx, name, consumer); err == nil {
			return name, nil
		}
	}
	if err := names.Err(); err != nil {
		return "", fmt.Errorf("list streams: %w", err)
	}
	return "", fmt.Errorf("no stream owns consumer %q", consumer)
}

// Consume pulls messages and drives process for each, acknowledging on success
// and negatively acknowledging on failure. It returns when ctx is cancelled.
func (c *Consumer) Consume(ctx context.Context, process connector.ProcessFunc) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		batch, err := c.consumer.Fetch(fetchBatch, jetstream.FetchMaxWait(fetchMaxWait))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.WithError(err).Warn("fetch failed; retrying")
			continue
		}

		// Process the whole fetched batch even if ctx was cancelled mid-batch so
		// that already-pulled messages are not dropped during shutdown.
		for msg := range batch.Messages() {
			c.handle(ctx, process, msg)
		}

		if err := batch.Error(); err != nil && !errors.Is(err, context.Canceled) {
			c.log.WithError(err).Warn("batch ended with error")
		}
	}
}

func (c *Consumer) handle(ctx context.Context, process connector.ProcessFunc, msg jetstream.Msg) {
	ack, err := process(ctx, msg.Data())
	if err != nil || !ack {
		if nakErr := msg.Nak(); nakErr != nil {
			c.log.WithError(nakErr).Warn("nak failed")
		}
		return
	}
	if ackErr := msg.Ack(); ackErr != nil {
		c.log.WithError(ackErr).Warn("ack failed")
	}
}

// Close drains and closes the NATS connection.
func (c *Consumer) Close() error {
	if c.nc != nil && !c.nc.IsClosed() {
		c.nc.Close()
	}
	return nil
}
