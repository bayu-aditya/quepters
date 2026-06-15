// Package config defines the quepters configuration schema and a loader that
// supports hot reload via file-system notifications.
package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration document.
type Config struct {
	LogLevel   string               `yaml:"log_level"`
	Server     Server               `yaml:"server"`
	Connectors map[string]Connector `yaml:"connectors"`
	Adapters   map[string]Adapter   `yaml:"adapters"`
}

// Server holds the listen address for the auxiliary HTTP server, which serves
// both the Prometheus /metrics endpoint and the /health endpoint. The field is
// optional and falls back to the default applied in withDefaults.
type Server struct {
	// HTTPAddr is the address the /metrics and /health HTTP server listens on.
	HTTPAddr string `yaml:"http_addr"`
}

// Connector is a named, reusable connection definition. Exactly one of the
// transport blocks must be set.
type Connector struct {
	NATS *NATSConnector `yaml:"nats"`
	GRPC *GRPCConnector `yaml:"grpc"`
}

// NATSConnector describes how to reach a NATS server.
type NATSConnector struct {
	Host string `yaml:"host"`
	User string `yaml:"user"`
	Pass string `yaml:"pass"`
}

// GRPCConnector describes how to reach a downstream gRPC server.
type GRPCConnector struct {
	Host string `yaml:"host"`
}

// Adapter wires a single input connector to a single output connector.
type Adapter struct {
	Input  Endpoint `yaml:"input"`
	Output Endpoint `yaml:"output"`
}

// Endpoint references a connector and carries the per-adapter binding details.
// Exactly one of the transport blocks must be set.
type Endpoint struct {
	NATS *NATSEndpoint `yaml:"nats"`
	GRPC *GRPCEndpoint `yaml:"grpc"`
}

// NATSEndpoint binds an adapter to an existing JetStream durable consumer.
type NATSEndpoint struct {
	ConnectorID string `yaml:"connector_id"`
	// SubscriptionID is the name of an existing durable pull consumer. quepters
	// never creates consumers; it only binds to this one.
	SubscriptionID string `yaml:"subscription_id"`
	// Stream is optional. When empty, quepters discovers the stream that owns
	// SubscriptionID by scanning the server's streams.
	Stream string `yaml:"stream"`
}

// GRPCEndpoint binds an adapter to a gRPC output connector.
type GRPCEndpoint struct {
	ConnectorID string `yaml:"connector_id"`
}

const (
	defaultLogLevel = "INFO"
	defaultHTTPAddr = ":9090"
)

// Parse decodes a YAML document, applies defaults and validates it.
func Parse(raw []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode yaml: %w", err)
	}
	cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) withDefaults() {
	if c.LogLevel == "" {
		c.LogLevel = defaultLogLevel
	}
	if c.Server.HTTPAddr == "" {
		c.Server.HTTPAddr = defaultHTTPAddr
	}
}

// Validate checks structural invariants: every connector has exactly one
// transport, every adapter references existing connectors of the correct type,
// and required binding fields are present.
func (c *Config) Validate() error {
	if len(c.Adapters) == 0 {
		return fmt.Errorf("no adapters defined")
	}

	for name, conn := range c.Connectors {
		switch {
		case conn.NATS != nil && conn.GRPC != nil:
			return fmt.Errorf("connector %q: only one transport may be set", name)
		case conn.NATS == nil && conn.GRPC == nil:
			return fmt.Errorf("connector %q: a transport (nats or grpc) is required", name)
		case conn.NATS != nil && conn.NATS.Host == "":
			return fmt.Errorf("connector %q: nats.host is required", name)
		case conn.GRPC != nil && conn.GRPC.Host == "":
			return fmt.Errorf("connector %q: grpc.host is required", name)
		}
	}

	for name, ad := range c.Adapters {
		if err := c.validateInput(name, ad.Input); err != nil {
			return err
		}
		if err := c.validateOutput(name, ad.Output); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateInput(adapter string, in Endpoint) error {
	if in.NATS == nil {
		return fmt.Errorf("adapter %q: input.nats is required (only nats input is supported)", adapter)
	}
	if in.NATS.SubscriptionID == "" {
		return fmt.Errorf("adapter %q: input.nats.subscription_id is required", adapter)
	}
	conn, ok := c.Connectors[in.NATS.ConnectorID]
	if !ok {
		return fmt.Errorf("adapter %q: input references unknown connector %q", adapter, in.NATS.ConnectorID)
	}
	if conn.NATS == nil {
		return fmt.Errorf("adapter %q: input connector %q is not a nats connector", adapter, in.NATS.ConnectorID)
	}
	return nil
}

func (c *Config) validateOutput(adapter string, out Endpoint) error {
	if out.GRPC == nil {
		return fmt.Errorf("adapter %q: output.grpc is required (only grpc output is supported)", adapter)
	}
	conn, ok := c.Connectors[out.GRPC.ConnectorID]
	if !ok {
		return fmt.Errorf("adapter %q: output references unknown connector %q", adapter, out.GRPC.ConnectorID)
	}
	if conn.GRPC == nil {
		return fmt.Errorf("adapter %q: output connector %q is not a grpc connector", adapter, out.GRPC.ConnectorID)
	}
	return nil
}
