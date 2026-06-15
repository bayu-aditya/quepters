# CLAUDE.md

Context for AI assistants working in this repository.

## What this is

quepters is a message broker-to-gRPC bridge: it pulls events from a broker
(NATS JetStream) and forwards each to a downstream gRPC service, acking the
broker message only when gRPC acks. It is designed for extensibility — input
and output transports are pluggable behind two interfaces.

## Core design

Everything hangs off two interfaces in `internal/connector/connector.go`:

```go
type InputConnector interface {
    Consume(ctx context.Context, process ProcessFunc) error
    Close() error
}
type OutputConnector interface {
    Forward(ctx context.Context, data []byte) (ack bool, err error)
    Close() error
}
type ProcessFunc func(ctx context.Context, data []byte) (ack bool, err error)
```

The dispatch core (`internal/adapter`) is transport-agnostic: it calls
`input.Consume`, and for each message runs `output.Forward`, records metrics,
and returns whether to ack. To add a transport, implement one interface and wire
it in `internal/runtime/runtime.go::buildConnectors` — the core does not change.

## Project structure

```
cmd/quepters/            main: flags, logging, servers, signal handling
internal/
  config/                YAML schema, defaults, validation, fsnotify hot reload
  connector/             InputConnector / OutputConnector interfaces (+ //go:generate mockery)
    input/nats/          NATS JetStream pull-consumer InputConnector
    output/grpc/         gRPC OutputConnector (quepters.QueptersService)
  adapter/               dispatch logic: wires one input to one output + metrics
  metrics/               per-adapter Prometheus metric vectors
  health/                HTTP /health handler for k8s probes
  runtime/               builds/launches/reloads adapters; graceful shutdown
  mocks/                 mockery-generated mocks (do not edit by hand)
proto/quepters/          quepters.proto (gRPC contract)
gen/quepters/            buf-generated Go (do not edit by hand)
```

## Key behaviors

- **Ack semantics**: ack only when `HandlerOutput.Ack == true` and no error;
  otherwise nak for redelivery. Lives in `adapter.process`.
- **Graceful shutdown** (`runtime.stop`): cancel the *consume* context (stops
  pulling new messages), wait up to `DefaultGracePeriod` (10s) for in-flight
  messages, then close connectors. Processing uses a separate `procCtx` that
  outlives the consume context so in-flight gRPC calls can finish.
- **Hot reload** (`config.Watch` → `runtime.Reload`): the new adapter set is
  built before the old one is stopped; if the build fails the current config
  keeps running. Watches the parent directory so atomic editor saves are caught.
- **NATS**: binds to an existing durable consumer; never creates one. The owning
  stream is auto-discovered when `stream` is omitted from config.

## Common tasks

```bash
make tools           # one-time: install buf + mockery + protoc plugins
make generate-buf    # regenerate gen/ from proto/   (after editing the .proto)
make generate-mock   # regenerate internal/mocks/    (after editing connector interfaces)
make lint-buf        # lint proto files
make test            # go test -race ./...
make build           # build ./bin/quepters
go vet ./...         # vet
gofmt -w .           # format
```

Run locally: `./bin/quepters --config config.yaml` (copy `config.example.yaml`).

## Gotchas

- The `.proto` field names (`EventData`, `Ack`) and package (`quepters`, no
  version suffix) are an externally-fixed contract. `buf.yaml` `except`s the
  lint rules they violate — keep it that way; do not "fix" the proto.
- Regenerate mocks whenever you change `InputConnector`/`OutputConnector`, or
  tests in `internal/adapter` will not compile.
- `gen/` and `internal/mocks/` are generated; edit the source (`.proto`,
  interfaces) and rerun the generators instead of editing them directly.
- Tools (`buf`, `mockery`) are invoked from `$(go env GOPATH)/bin` in the
  Makefile; ensure `make tools` has been run.
