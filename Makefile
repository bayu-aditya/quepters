BINARY  := quepters
PKG     := ./cmd/quepters
BIN_DIR := bin

GOBIN := $(shell go env GOPATH)/bin

.PHONY: help build test generate-mock generate-buf lint-buf tidy fmt vet clean tools

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary into ./bin
	go build -o $(BIN_DIR)/$(BINARY) $(PKG)

test: ## Run unit tests with the race detector
	go test -race ./...

generate-mock: ## Regenerate interface mocks with mockery v3
	$(GOBIN)/mockery

generate-buf: ## Regenerate gRPC Go code with buf
	$(GOBIN)/buf generate

lint-buf: ## Lint the proto files with buf
	$(GOBIN)/buf lint

tidy: ## Tidy go.mod / go.sum
	go mod tidy

fmt: ## Format Go source
	gofmt -w .

vet: ## Run go vet
	go vet ./...

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR)

tools: ## Install the code-generation tools (buf, mockery)
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install github.com/vektra/mockery/v3@v3.5.1
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
