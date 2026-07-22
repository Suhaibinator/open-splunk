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
npm ci
make proto
npm run build
go test ./...
go build -o build/open-splunk-server ./cmd/open-splunk-server
go build -o build/open-splunk-collector ./cmd/open-splunk-collector
```

`make build` performs the UI export before compiling the server, ensuring the resulting server binary contains the current frontend.

The search workspace uses deterministic demo fixtures by default. Build the embedded UI against real backend search jobs with one build-time switch:

```sh
OPEN_SPLUNK_DATA_MODE=backend make build-server
```

The resulting `build/open-splunk-server` serves the UI and protobuf API from the same origin. Omit the variable (or set it to `demo`) to restore fixture mode. `OPEN_SPLUNK_API_BASE_URL` is available for controlled test builds that need a non-default API origin; it is also resolved at build time and is not a browser setting.

`make proto` compiles every schema under `proto/` into Go protobuf/gRPC code in `gen/go` and `ts-proto` codecs in `gen/ts`. Run `make proto-tools` once to install the pinned Go generators and JavaScript dependencies; `protoc` must also be available on `PATH`.

`make proto-lint` runs the schema compatibility/style linter without regenerating code. `make proto` runs it automatically before generation.

In VS Code, **Run Build Task** (`Cmd+Shift+B` on macOS or `Ctrl+Shift+B` elsewhere) runs the default **Generate protobufs (Go, gRPC, TypeScript)** task, which delegates to `make proto`.

The backend includes the protobuf HTTP API, authenticated gRPC ingestion,
collector WAL and file tailing, the SQLite control plane, ClickHouse storage,
bounded search jobs, and the executable SPL subset documented in
[`docs/spl-compatibility-v0.1.md`](docs/spl-compatibility-v0.1.md). The default
Go test suite is self-contained. The pinned ClickHouse and full collector-to-search
tests are opt-in because they start ephemeral Docker containers:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/queryexec -run TestExecutorAndManagerAgainstClickHouse
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run TestBackendVertical
```

See [the product and architecture plan](docs/product-architecture-plan.md) for the complete design.
