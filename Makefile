.PHONY: build build-ui build-server build-collector build-loggen lint proto proto-lint proto-tools test clean

PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
PROTO_LINT_CACHE := $(CURDIR)/.cache/buf

build: build-server build-collector

build-ui:
	npm run build
	test -f out/index.html

build-server: proto build-ui
	mkdir -p build
	go build -trimpath -o build/open-splunk-server ./cmd/open-splunk-server

build-collector: proto
	mkdir -p build
	go build -trimpath -o build/open-splunk-collector ./cmd/open-splunk-collector

build-loggen: proto
	mkdir -p build
	go build -trimpath -o build/open-splunk-loggen ./cmd/open-splunk-loggen

proto: proto-lint
	bash scripts/compile-protos.sh

proto-lint:
	BUF_CACHE_DIR=$(PROTO_LINT_CACHE) npx --no-install buf format --diff --exit-code
	BUF_CACHE_DIR=$(PROTO_LINT_CACHE) npx --no-install buf lint

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	npm ci

lint:
	npm run lint

test: lint
	go test ./...
	npm run typecheck

clean:
	go clean
