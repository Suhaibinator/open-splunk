# Open Splunk

Open Splunk is a single-node log search and analytics application with an SPL-compatible query layer over ClickHouse.

The server is written in Go. Its browser UI is a statically exported Next.js application embedded into the server executable at build time. Log-producing hosts run the separate Go collector and send protobuf batches to the server over gRPC.

## Repository layout

```text
app/                    Next.js App Router source
cmd/
  open-splunk-server/   Self-contained server application
  open-splunk-collector/ Log collector deployed beside applications
  open-splunk-loggen/   Test and benchmark event generator
configs/examples/       Example runtime configuration
deploy/                 Local and production deployment assets
docs/                   Product, architecture, and operational documentation
gen/
  go/                   Generated Go protobuf and gRPC code
  ts/                   Generated TypeScript protobuf code
internal/               Private Go application packages
migrations/
  clickhouse/            ClickHouse schema migrations
  sqlite/                SQLite control-plane migrations
out/                    Generated Next.js static export embedded by Go
proto/open_splunk/v1/   Versioned protobuf source contracts
public/                 Next.js public assets
scripts/                Build and developer automation
go.mod                  Root Go module
Makefile                Canonical local build and test targets
package.json            Root Next.js/TypeScript workspace
next.config.ts          Static-export configuration
webui.go                Go embed boundary for the generated UI
```

The root TypeScript files (`package.json`, `next.config.ts`, and `tsconfig.json`) define the Next.js application. The root Go package embeds `out/`, which is why the generated static export lives at the repository root.

## Initial commands

```sh
go version   # go1.26.6; also pinned in go.mod
node --version # v26.7.0; also pinned in .node-version
npm --version  # 11.19.0; also pinned in package.json
make proto-tools
make proto
go test ./...
make build
```

`make build` performs a backend-mode UI export before compiling the server,
generates and verifies the embedded asset manifest, and links the same build
identity into the server and collector. Direct `go build` is only a development
compile check; use the Make target for a runnable embedded server.

Published artifacts use the stricter `make release` target. It requires the
exact Node.js/npm versions pinned above and
`OPEN_SPLUNK_SOURCE_REVISION` to equal the clean checkout's full Git revision.
The caller must also provide the exact expected public SPL compatibility
identity; the release fails before publication if the embedded server reports a
different identity.
The release launcher materializes raw blobs and executable modes from that
committed `HEAD` into a disposable tree; ignored and untracked worktree files
are omitted and cannot enter the artifact. It installs the lockfile-pinned
frontend and protobuf tools into fresh caches under a scrubbed environment,
forces backend data mode, disables ambient Go workspace/VCS/build controls,
verifies the embedded payload plus the linked server/collector/log-generator
identities, and atomically publishes only the verified files under `build/`.
`make` necessarily parses the physical Makefile, so that file and the commands
resolved from `PATH` are the release-launcher trust boundary. The launcher
executes `scripts/build-release.sh` from committed `HEAD`; no other live
checkout file is used as a release build input:

```sh
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \
OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release
```

The application and authored-SPL version lines are independent release
authorities: the initial SPL v0.2 release is application `0.1.0`; the v0.3
runtime revision changes this invocation to application `0.2.0` with expected
SPL `0.3`.

For the production-shaped, non-root server and collector OCI images plus the
digest-pinned server/ClickHouse Compose stack, follow
[`deploy/README.md`](deploy/README.md). The OCI build is separately anchored to
a clean committed snapshot, serializes equivalent destination references in
the Docker daemon across independent clones, and transactionally restores both
prior tags if pair publication fails. The production Compose file consumes
that prebuilt image and cannot silently rebuild a dirty checkout.

Each `vX.Y.Z` release tag runs one publication pipeline for both consumable
images: `open-splunk-server` and `open-splunk-collector`, each for AMD64 and
ARM64. See [`docs/releasing.md`](docs/releasing.md) for the exact tagging and
publication procedure. For an end-to-end remote collector deployment using that
release's collector image, follow
[`docs/collector-deployment.md`](docs/collector-deployment.md).

The server can also expose a bounded Splunk-compatible HTTP Event Collector
surface. HEC is disabled by default and shares the existing browser/API HTTPS
listener instead of opening port 8088. See the
[`docs/hec-deployment.md`](docs/hec-deployment.md) operator runbook for
enablement, token creation, TLS-safe examples, retry semantics, limits, and
recovery behavior.

For frontend-only design work, build the deterministic demo workspace
explicitly:

```sh
OPEN_SPLUNK_DATA_MODE=demo npm run build
```

`OPEN_SPLUNK_DATA_MODE=demo make build-server` is also available for a
self-contained demonstration binary. The normal `make build` and
`make build-server` targets intentionally default to backend mode.
`OPEN_SPLUNK_API_BASE_URL` is a build-time test override, not a browser
setting. The production Go server intentionally requires browser HTTP and
WebSocket traffic to remain same-origin. Use the serving origin with API routes
exposed at their advertised root paths, or a test double with an explicit
trusted-origin policy; a cross-origin URL or arbitrary path prefix is not
supported by the Go server.

`make proto` compiles every schema under `proto/` into Go protobuf/gRPC code in
`gen/go` and `ts-proto` codecs in `gen/ts`. Run `make proto-tools` once to
install the pinned Go generators and JavaScript dependencies. Generation uses
the lockfile-pinned Buf compiler, so an ambient `protoc` installation is not
required and cannot change generated bytes.

`make proto-lint` runs the schema compatibility/style linter without regenerating code. `make proto` runs it automatically before generation.

In VS Code, **Run Build Task** (`Cmd+Shift+B` on macOS or `Ctrl+Shift+B` elsewhere) runs the default **Generate protobufs (Go, gRPC, TypeScript)** task, which delegates to `make proto`.

The backend includes the protobuf HTTP API, authenticated gRPC ingestion, an
optional HEC compatibility facade, collector WAL and file tailing, the SQLite
control plane, ClickHouse storage, bounded search jobs, and the executable SPL
authored-search subset documented in
[`docs/spl-compatibility-v0.2.md`](docs/spl-compatibility-v0.2.md). Tier-1
calculated-field knowledge expressions deliberately remain on their closed
v0.1 profile, documented in
[`docs/knowledge-compatibility-v0.1.md`](docs/knowledge-compatibility-v0.1.md).
Before upgrading retained searches, follow the
[`v0.2 migration and read-only audit guide`](docs/spl-compatibility-v0.2-migration.md).
The default Go test suite is self-contained. The pinned ClickHouse and full
collector-to-browser tests are opt-in because they start ephemeral Docker
containers; the browser vertical also requires the pinned Playwright browser:

The shipped [`configs/examples/collector.yaml`](configs/examples/collector.yaml)
is the sanitized GradeThis migration profile. It takes the server, private
token file, durable state directory, resolved GradeThis log path, application
host, and environment from explicit environment variables. The backend
vertical validates and runs that exact file through its pre-WAL sanitizer,
proves trusted metadata, and executes representative current GradeThis
searches without an OpenTelemetry log collector.

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' -count=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec -run TestExecutorAndManagerAgainstClickHouse

npm ci
npx --no-install playwright install chromium
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run TestBackendVertical

OPEN_SPLUNK_OCI_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.7.3.19@sha256:f90a77560f72b10802106ee49e9870e41668cbc496e280c3911f6e3b216657f3 \
go test ./integration -run '^TestReleaseOCIComposeContract$' -count=1 -timeout=20m -v
```

See [`integration/README.md`](integration/README.md) for the exact vertical
coverage and browser override.

See [the product and architecture plan](docs/product-architecture-plan.md) for the complete design.
