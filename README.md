# quepters

A message broker-to-gRPC bridge. quepters consumes events from a message broker
and forwards each one to a downstream gRPC service, acknowledging the broker
message only when the gRPC call acknowledges it.

The runtime is built around two interfaces — `InputConnector` and
`OutputConnector` — so new broker or sink types can be added without touching
the dispatch core. Each **adapter** wires one input to one output.

| | Currently supported |
|---|---|
| **Input** | NATS JetStream pull consumer (binds to an existing durable consumer; never auto-creates) |
| **Output** | gRPC call to `quepters.QueptersService/Handler` |

## How it works

```
NATS JetStream  ──pull──▶  Adapter  ──gRPC Handler──▶  Downstream service
   (durable consumer)        │                              │
                             │◀──────── Ack/Nak ────────────┘
```

For every message the adapter forwards the payload as `HandlerInput.EventData`.
If the response `HandlerOutput.Ack` is `true` the message is acked; otherwise
(or on any error) it is naked for redelivery.

## Quickstart

```bash
# 1. Install code-gen tools (one-time)
make tools

# 2. Generate gRPC code and mocks
make generate-buf
make generate-mock

# 3. Create your config
cp config.example.yaml config.yaml   # then edit

# 4. Run
make build
./bin/quepters --config config.yaml
```

The config file is **hot-reloaded** — edits are applied without a restart. An
invalid edit is logged and ignored, leaving the running configuration intact.

## Configuration

Config is loaded from the path given by `--config` (default `config.yaml`). See
[`config.example.yaml`](./config.example.yaml) for a complete, commented example.

| Field | Description | Default |
|---|---|---|
| `log_level` | Logrus level: `trace`/`debug`/`info`/`warn`/`error` | `INFO` |
| `server.http_addr` | Listen address for the HTTP server hosting `/metrics` and `/health` | `:9090` |
| `connectors.<name>.nats` | NATS connection: `host`, `user`, `pass` | — |
| `connectors.<name>.grpc` | gRPC connection: `host` (`http://` plaintext, `https://` TLS) | — |
| `adapters.<name>.input.nats.connector_id` | NATS connector to consume from | — |
| `adapters.<name>.input.nats.subscription_id` | Existing durable consumer name | — |
| `adapters.<name>.input.nats.stream` | Owning stream (auto-discovered if omitted) | discovered |
| `adapters.<name>.output.grpc.connector_id` | gRPC connector to forward to | — |

A connector must define exactly one transport. Each adapter must reference an
existing NATS connector for its input and an existing gRPC connector for its
output; these invariants are validated at load time.

## Observability

- **Structured logging** via logrus (JSON), honoring `log_level`.
- **Prometheus metrics** at `http://<http_addr>/metrics`, labeled per adapter:

  | Metric | Type | Meaning |
  |---|---|---|
  | `quepters_messages_received_total` | counter | messages pulled from the input |
  | `quepters_messages_forwarded_total` | counter | forwarded and acknowledged |
  | `quepters_messages_failed_total` | counter | failed or not acknowledged |
  | `quepters_processing_latency_seconds` | histogram | per-message forwarding latency |
  | `quepters_adapter_status` | gauge | `0`=stopped, `1`=running, `2`=error |

## Health checks

An HTTP `/health` endpoint is served on `server.http_addr` (alongside
`/metrics`). It returns `200 OK` while serving and `503` once shutdown begins.
Point Kubernetes liveness/readiness probes at it, e.g.:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
```

## Graceful shutdown

On `SIGINT`/`SIGTERM` quepters stops pulling new messages, flips `/health` to
`503`, and waits up to **10 seconds** for in-flight messages to finish before
closing connections and exiting.

## Running with Docker

The image is published to `ghcr.io/bayu-aditya/quepters` on every `v*` tag. The
Dockerfile copies a CI-built binary (it does not compile source).

```bash
docker run --rm \
  -v "$PWD/config.yaml:/etc/quepters/config.yaml:ro" \
  -p 9090:9090 \
  ghcr.io/bayu-aditya/quepters:latest
```

To build the image locally you must build the binary first (matching what CI
does):

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/quepters ./cmd/quepters
docker build -t quepters:dev .
```

## Makefile targets

| Target | Description |
|---|---|
| `make test` | Run unit tests with the race detector |
| `make generate-mock` | Regenerate interface mocks (mockery v3) |
| `make generate-buf` | Regenerate gRPC Go code (buf) |
| `make lint-buf` | Lint the proto files (buf) |
| `make build` | Build the binary into `./bin` |
| `make tools` | Install buf and mockery |

Run `make help` to see all targets.

## gRPC contract

```proto
service QueptersService {
  rpc Handler(HandlerInput) returns (HandlerOutput);
}
message HandlerInput  { bytes EventData = 1; }
message HandlerOutput { bool  Ack = 1; }
```

The full contract lives in [`proto/quepters/quepters.proto`](./proto/quepters/quepters.proto).
