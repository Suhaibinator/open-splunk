.PHONY: build release oci build-ui build-server build-collector build-loggen lint proto proto-lint proto-tools release-go-deps test clean

override PROTOC_GEN_GO_VERSION := v1.36.11
override PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
override PROTO_TOOL_BIN := $(CURDIR)/.cache/proto-tools
override PROTO_LINT_CACHE := $(CURDIR)/.cache/buf
override RELEASE_GO_VERSION := 1.26.6
override RELEASE_NODE_VERSION := 24.18.0
override RELEASE_NPM_VERSION := 11.16.0
override RELEASE_GIT_ENV := env \
	-u GIT_ALTERNATE_OBJECT_DIRECTORIES \
	-u GIT_COMMON_DIR \
	-u GIT_DIR \
	-u GIT_INDEX_FILE \
	-u GIT_NAMESPACE \
	-u GIT_OBJECT_DIRECTORY \
	-u GIT_PREFIX \
	-u GIT_WORK_TREE \
	GIT_CONFIG_GLOBAL=/dev/null \
	GIT_CONFIG_NOSYSTEM=1 \
	GIT_NO_REPLACE_OBJECTS=1 \
	GIT_OPTIONAL_LOCKS=0
override GO_TOOL_ENV := env \
	-u GO386 \
	-u GOARCH \
	-u GOARM \
	-u GOARM64 \
	-u GOAMD64 \
	-u GOMIPS \
	-u GOMIPS64 \
	-u GOOS \
	-u GOPPC64 \
	-u GORISCV64 \
	-u GOROOT \
	-u GOWASM \
	CGO_ENABLED=0 \
	GO111MODULE=on \
	GOAMD64=v1 \
	GOARM64=v8.0 \
	GOCACHEPROG= \
	GODEBUG= \
	GOENV=off \
	GOEXPERIMENT= \
	GOFIPS140=off \
	GOFLAGS= \
	GOTOOLCHAIN=local \
	GOWORK=off
OPEN_SPLUNK_DATA_MODE ?= backend
OPEN_SPLUNK_APPLICATION_VERSION ?= 0.1.0
OPEN_SPLUNK_SOURCE_REVISION ?= development
OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION ?=
override BUILDINFO_PACKAGE := github.com/Suhaibinator/open-splunk/internal/buildinfo
override GO_BUILD_LDFLAGS = -X $(BUILDINFO_PACKAGE).applicationVersion=$(OPEN_SPLUNK_APPLICATION_VERSION) -X $(BUILDINFO_PACKAGE).sourceRevision=$(OPEN_SPLUNK_SOURCE_REVISION)

build: build-server build-collector

release: export OPEN_SPLUNK_APPLICATION_VERSION := $(value OPEN_SPLUNK_APPLICATION_VERSION)
release: export OPEN_SPLUNK_SOURCE_REVISION := $(value OPEN_SPLUNK_SOURCE_REVISION)
release: export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION := $(value OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION)
release:
	test "$$($(GO_TOOL_ENV) go env GOVERSION)" = "go$(RELEASE_GO_VERSION)"
	test "$$(env NODE_OPTIONS="" node --version)" = "v$(RELEASE_NODE_VERSION)"
	test "$$(env NODE_OPTIONS="" npm --version)" = "$(RELEASE_NPM_VERSION)"
	@umask 022; set -eu; \
		repo_root="$$(pwd -P)"; \
		launcher_root="$$(mktemp -d "$${TMPDIR:-/tmp}/open-splunk-release-launcher.XXXXXX")"; \
		launcher_root="$$(cd "$$launcher_root" && pwd -P)"; \
		trap 'rm -rf "$$launcher_root"' EXIT HUP INT TERM; \
		git_repo() { \
			$(RELEASE_GIT_ENV) git \
				-c core.fsmonitor=false \
				-c core.hooksPath=/dev/null \
				-C "$$repo_root" \
				"$$@"; \
		}; \
		resolved_root="$$(git_repo rev-parse --show-toplevel)"; \
		resolved_root="$$(cd "$$resolved_root" && pwd -P)"; \
		test "$$resolved_root" = "$$repo_root"; \
		head_revision="$$(git_repo rev-parse --verify HEAD)"; \
		launcher_object="$$(git_repo ls-tree "$$head_revision" -- scripts/build-release.sh | \
			awk '$$1 == "100755" && $$2 == "blob" && $$4 == "scripts/build-release.sh" && NF == 4 { print $$3 }')"; \
		test -n "$$launcher_object"; \
		launcher="$$launcher_root/build-release.sh"; \
		git_repo cat-file blob "$$launcher_object" >"$$launcher"; \
		chmod 0500 "$$launcher"; \
		env -u BASH_ENV -u ENV \
			BASH_ENV= \
			ENV= \
			OPEN_SPLUNK_REPOSITORY_ROOT="$$repo_root" \
			bash "$$launcher"

oci: export OPEN_SPLUNK_APPLICATION_VERSION := $(value OPEN_SPLUNK_APPLICATION_VERSION)
oci: export OPEN_SPLUNK_SOURCE_REVISION := $(value OPEN_SPLUNK_SOURCE_REVISION)
oci: export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION := $(value OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION)
oci: export OPEN_SPLUNK_SERVER_IMAGE := $(value OPEN_SPLUNK_SERVER_IMAGE)
oci: export OPEN_SPLUNK_COLLECTOR_IMAGE := $(value OPEN_SPLUNK_COLLECTOR_IMAGE)
oci: export OPEN_SPLUNK_OCI_PLATFORM := $(value OPEN_SPLUNK_OCI_PLATFORM)
oci: export OPEN_SPLUNK_OCI_NO_CACHE := $(value OPEN_SPLUNK_OCI_NO_CACHE)
oci:
	@umask 077; set -eu; \
		repo_root="$$(pwd -P)"; \
		launcher_root="$$(mktemp -d "$${TMPDIR:-/tmp}/open-splunk-oci-launcher.XXXXXX")"; \
		launcher_root="$$(cd "$$launcher_root" && pwd -P)"; \
		trap 'rm -rf "$$launcher_root"' EXIT HUP INT TERM; \
		git_repo() { \
			$(RELEASE_GIT_ENV) git \
				-c core.fsmonitor=false \
				-c core.hooksPath=/dev/null \
				-C "$$repo_root" \
				"$$@"; \
		}; \
		resolved_root="$$(git_repo rev-parse --show-toplevel)"; \
		resolved_root="$$(cd "$$resolved_root" && pwd -P)"; \
		test "$$resolved_root" = "$$repo_root"; \
		head_revision="$$(git_repo rev-parse --verify HEAD)"; \
		launcher_object="$$(git_repo ls-tree "$$head_revision" -- scripts/build-oci.sh | \
			awk '$$1 == "100755" && $$2 == "blob" && $$4 == "scripts/build-oci.sh" && NF == 4 { print $$3 }')"; \
		test -n "$$launcher_object"; \
		launcher="$$launcher_root/build-oci.sh"; \
		git_repo cat-file blob "$$launcher_object" >"$$launcher"; \
		chmod 0500 "$$launcher"; \
		env -u BASH_ENV -u ENV \
			BASH_ENV= \
			ENV= \
			OPEN_SPLUNK_REPOSITORY_ROOT="$$repo_root" \
			bash "$$launcher"

build-ui: proto
	env \
		LANG=C \
		LC_ALL=C \
		NEXT_TELEMETRY_DISABLED=1 \
		NODE_OPTIONS="" \
		OPEN_SPLUNK_API_BASE_URL="" \
		OPEN_SPLUNK_APPLICATION_VERSION="$(OPEN_SPLUNK_APPLICATION_VERSION)" \
		OPEN_SPLUNK_DATA_MODE="$(OPEN_SPLUNK_DATA_MODE)" \
		OPEN_SPLUNK_SOURCE_REVISION="$(OPEN_SPLUNK_SOURCE_REVISION)" \
		TZ=UTC \
		npm run build
	test -f out/index.html
	grep -Fq 'data-open-splunk-version="$(OPEN_SPLUNK_APPLICATION_VERSION)"' out/index.html
	grep -Fq 'data-open-splunk-revision="$(OPEN_SPLUNK_SOURCE_REVISION)"' out/index.html
	$(GO_TOOL_ENV) go run ./cmd/open-splunk-manifest \
		-application-version "$(OPEN_SPLUNK_APPLICATION_VERSION)" \
		-source-revision "$(OPEN_SPLUNK_SOURCE_REVISION)"
	test -f out/asset-manifest.json

build-server: build-ui
	mkdir -p build
	$(GO_TOOL_ENV) go build -buildvcs=false -trimpath -ldflags "$(GO_BUILD_LDFLAGS)" -o build/open-splunk-server ./cmd/open-splunk-server
	chmod 0755 build/open-splunk-server

build-collector: proto
	mkdir -p build
	$(GO_TOOL_ENV) go build -buildvcs=false -trimpath -ldflags "$(GO_BUILD_LDFLAGS)" -o build/open-splunk-collector ./cmd/open-splunk-collector
	chmod 0755 build/open-splunk-collector

build-loggen: proto
	mkdir -p build
	$(GO_TOOL_ENV) go build -buildvcs=false -trimpath -ldflags "$(GO_BUILD_LDFLAGS)" -o build/open-splunk-loggen ./cmd/open-splunk-loggen
	chmod 0755 build/open-splunk-loggen

proto: proto-lint
	bash scripts/compile-protos.sh

proto-lint:
	BUF_CACHE_DIR="$(PROTO_LINT_CACHE)" npx --no-install buf format --diff --exit-code
	BUF_CACHE_DIR="$(PROTO_LINT_CACHE)" npx --no-install buf lint

proto-tools:
	mkdir -p "$(PROTO_TOOL_BIN)"
	$(GO_TOOL_ENV) GOBIN="$(PROTO_TOOL_BIN)" go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO_TOOL_ENV) GOBIN="$(PROTO_TOOL_BIN)" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	npm ci --include=dev

release-go-deps:
	$(GO_TOOL_ENV) go mod download all
	$(GO_TOOL_ENV) go mod verify

lint:
	npm run lint

test: lint
	$(GO_TOOL_ENV) go test ./...
	npm run test:frontend
	npm run typecheck

clean:
	$(GO_TOOL_ENV) go clean
