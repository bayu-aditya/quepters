package adapter_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bayu-aditya/quepters/internal/adapter"
	"github.com/bayu-aditya/quepters/internal/connector"
	"github.com/bayu-aditya/quepters/internal/metrics"
	"github.com/bayu-aditya/quepters/internal/mocks"
)

func newDeps(t *testing.T) (*mocks.MockInputConnector, *mocks.MockOutputConnector, *metrics.AdapterMetrics, *logrus.Logger) {
	t.Helper()
	log := logrus.New()
	log.SetOutput(io.Discard)
	m := metrics.New(prometheus.NewRegistry()).Adapter("test")
	return mocks.NewMockInputConnector(t), mocks.NewMockOutputConnector(t), m, log
}

// runOnce wires the input mock so that Consume drives process exactly once with
// the given payload, and returns the (ack, err) the adapter produced.
func runOnce(t *testing.T, in *mocks.MockInputConnector, payload []byte) (*bool, *error) {
	t.Helper()
	var gotAck bool
	var gotErr error
	in.EXPECT().Consume(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, process connector.ProcessFunc) error {
			gotAck, gotErr = process(ctx, payload)
			return nil
		})
	return &gotAck, &gotErr
}

func TestAdapter_AcksWhenOutputAcknowledges(t *testing.T) {
	in, out, m, log := newDeps(t)
	out.EXPECT().Forward(mock.Anything, []byte("event")).Return(true, nil)
	ack, err := runOnce(t, in, []byte("event"))

	a := adapter.New("test", in, out, m, log)
	require.NoError(t, a.Run(context.Background(), context.Background()))

	require.NoError(t, *err)
	require.True(t, *ack)
}

func TestAdapter_NaksWhenOutputDeclines(t *testing.T) {
	in, out, m, log := newDeps(t)
	out.EXPECT().Forward(mock.Anything, mock.Anything).Return(false, nil)
	ack, err := runOnce(t, in, []byte("event"))

	a := adapter.New("test", in, out, m, log)
	require.NoError(t, a.Run(context.Background(), context.Background()))

	require.NoError(t, *err)
	require.False(t, *ack, "message should not be acked when output declines")
}

func TestAdapter_NaksWhenOutputErrors(t *testing.T) {
	in, out, m, log := newDeps(t)
	out.EXPECT().Forward(mock.Anything, mock.Anything).Return(false, errors.New("boom"))
	ack, err := runOnce(t, in, []byte("event"))

	a := adapter.New("test", in, out, m, log)
	require.NoError(t, a.Run(context.Background(), context.Background()))

	require.Error(t, *err)
	require.False(t, *ack)
}

func TestAdapter_RunReturnsErrorOnConsumeFailure(t *testing.T) {
	in, out, m, log := newDeps(t)
	consumeErr := errors.New("consume failed")
	in.EXPECT().Consume(mock.Anything, mock.Anything).Return(consumeErr)

	a := adapter.New("test", in, out, m, log)
	err := a.Run(context.Background(), context.Background())
	require.ErrorIs(t, err, consumeErr)
}

func TestAdapter_RunSuppressesErrorOnShutdown(t *testing.T) {
	in, out, m, log := newDeps(t)
	// When consumeCtx is already cancelled, a Consume error is treated as a
	// normal shutdown and not propagated.
	in.EXPECT().Consume(mock.Anything, mock.Anything).Return(errors.New("ctx cancelled"))

	consumeCtx, cancel := context.WithCancel(context.Background())
	cancel()

	a := adapter.New("test", in, out, m, log)
	require.NoError(t, a.Run(consumeCtx, context.Background()))
}

func TestAdapter_Close(t *testing.T) {
	in, out, m, log := newDeps(t)
	in.EXPECT().Close().Return(nil)
	out.EXPECT().Close().Return(nil)

	a := adapter.New("test", in, out, m, log)
	require.NoError(t, a.Close())
}

func TestAdapter_WaitInflightNoMessages(t *testing.T) {
	in, out, m, log := newDeps(t)
	a := adapter.New("test", in, out, m, log)
	require.True(t, a.WaitInflight(time.Second))
}
