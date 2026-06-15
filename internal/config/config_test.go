package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bayu-aditya/quepters/internal/config"
)

const valid = `
log_level: "DEBUG"
connectors:
  nats_staging:
    nats:
      host: "192.168.221.224:31175"
      user: "app-backend-staging"
      pass: "foo"
  api_staging:
    grpc:
      host: "http://localhost:8080"
adapters:
  case_1:
    input:
      nats:
        connector_id: "nats_staging"
        subscription_id: "staging-tenant-worker-filter"
    output:
      grpc:
        connector_id: "api_staging"
`

func TestParse_Valid(t *testing.T) {
	cfg, err := config.Parse([]byte(valid))
	require.NoError(t, err)

	require.Equal(t, "DEBUG", cfg.LogLevel)
	require.Len(t, cfg.Adapters, 1)
	ad := cfg.Adapters["case_1"]
	require.Equal(t, "nats_staging", ad.Input.NATS.ConnectorID)
	require.Equal(t, "staging-tenant-worker-filter", ad.Input.NATS.SubscriptionID)
	require.Equal(t, "api_staging", ad.Output.GRPC.ConnectorID)
	require.Equal(t, "192.168.221.224:31175", cfg.Connectors["nats_staging"].NATS.Host)
}

func TestParse_AppliesDefaults(t *testing.T) {
	cfg, err := config.Parse([]byte(`
connectors:
  c:
    grpc:
      host: "localhost:8080"
  n:
    nats:
      host: "localhost:4222"
adapters:
  a:
    input:
      nats:
        connector_id: "n"
        subscription_id: "sub"
    output:
      grpc:
        connector_id: "c"
`))
	require.NoError(t, err)
	require.Equal(t, "INFO", cfg.LogLevel)
	require.Equal(t, ":9090", cfg.Server.HTTPAddr)
}

func TestParse_Errors(t *testing.T) {
	cases := map[string]string{
		"no adapters": `
connectors:
  n:
    nats:
      host: "h"
`,
		"unknown input connector": `
connectors:
  n:
    nats:
      host: "h"
adapters:
  a:
    input:
      nats:
        connector_id: "missing"
        subscription_id: "sub"
    output:
      grpc:
        connector_id: "n"
`,
		"input connector wrong type": `
connectors:
  g:
    grpc:
      host: "h"
adapters:
  a:
    input:
      nats:
        connector_id: "g"
        subscription_id: "sub"
    output:
      grpc:
        connector_id: "g"
`,
		"missing subscription_id": `
connectors:
  n:
    nats:
      host: "h"
  g:
    grpc:
      host: "h"
adapters:
  a:
    input:
      nats:
        connector_id: "n"
    output:
      grpc:
        connector_id: "g"
`,
		"output not grpc": `
connectors:
  n:
    nats:
      host: "h"
adapters:
  a:
    input:
      nats:
        connector_id: "n"
        subscription_id: "sub"
    output:
      grpc:
        connector_id: "n"
`,
		"connector with two transports": `
connectors:
  n:
    nats:
      host: "h"
    grpc:
      host: "h"
adapters:
  a:
    input:
      nats:
        connector_id: "n"
        subscription_id: "sub"
    output:
      grpc:
        connector_id: "n"
`,
	}

	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := config.Parse([]byte(doc))
			require.Error(t, err)
		})
	}
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(valid), 0o600))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Len(t, cfg.Adapters, 1)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.Error(t, err)
}
