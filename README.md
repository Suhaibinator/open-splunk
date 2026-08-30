# Open Splunk

Open Splunk is a major-version-zero single-node log search and analytics
application with a bounded SPL-like query layer over ClickHouse. Its source
contracts remain under active development, and v0 releases intentionally make
no persisted-state compatibility promise.

The Go server embeds a statically exported Next.js browser application. A
separate Go collector tails application logs and delivers durable protobuf
batches over authenticated gRPC. The server also offers an optional bounded
HTTP Event Collector facade.

## Repository layout

```text
app/                      Next.js App Router source
cmd/
  open-splunk-server/     self-contained server
  open-splunk-collector/  host-side log collector
  open-splunk-loggen/     test/benchmark event generator
configs/examples/         example runtime configuration
deploy/                   production-shaped local deployment
docs/                     current product and operations reference
gen/go/                   generated Go protobuf/gRPC code
gen/ts/                   generated TypeScript protobuf code
integration/              executable shipped-product gates
internal/                 private Go packages
migrations/               ordered SQLite and ClickHouse schema migrations
proto/open_splunk/        current protobuf source contracts
public/                   static browser assets
scripts/                  build and validation automation
```

The root TypeScript workspace builds the static browser export under `out/`.
The root Go package embeds that export in the server executable.

## Run from source

Use the exact Go, Node.js, and npm versions pinned by `go.mod`, `.node-version`,
and `package.json`. Docker with Compose v2 and OpenSSL are also required.
Install the pinned frontend and protobuf tools once after cloning:

```sh
make proto-tools
```

Start the pinned ClickHouse dependency, then build and run the current source
tree as a native development server:

```sh
make dev-clickhouse
make run
```

Neither command requires a clean worktree, Git commit, source hash, or product
version. `make dev-clickhouse` generates reusable local credentials and starts
one plaintext ClickHouse service in an isolated `open-splunk-development`
Compose project; it starts no Open Splunk server.
`make run` builds the embedded browser UI and server with the explicit
`development` identity, applies ClickHouse migrations, and runs in the
foreground. Open `http://127.0.0.1:8080/signin/`; the administrator token is
stored in the owner-only `deploy/.env.development` file and is never printed.

Stop the server with Ctrl-C. ClickHouse and all development data remain for the
next run. Stop its containers without deleting state with:

```sh
make dev-down
```

To choose other loopback ports, set `OPEN_SPLUNK_DEPLOY_HTTP_PORT` or
`OPEN_SPLUNK_DEPLOY_CLICKHOUSE_NATIVE_PORT` on the first `make dev-clickhouse`,
or edit those values in `deploy/.env.development` while the processes are
stopped.

**Destructive development reset:** first run `make dev-down`, then remove the
`open-splunk-development` Compose volumes and the ignored
`data/development`, `exports/development`, and `deploy/.env.development`
paths. This permanently deletes the local
database, ClickHouse data, exports, credentials, and administrator token; the
next `make dev-clickhouse` creates an empty environment:

```sh
docker volume rm \
  open-splunk-development_clickhouse-data
rm -rf -- \
  data/development \
  exports/development \
  deploy/.env.development
```

## Build and test

```sh
make proto
make test
make build
```

`make proto` lints and compiles every schema under `proto/` into
`gen/go/open_splunk` and `gen/ts/open_splunk`. Generation uses pinned Buf and
plugin versions; generated files are never edited manually.

`make build` exports the backend-mode UI, generates/verifies the embedded asset
manifest, and links one build identity into server, collector, and log
generator. The resulting development binary still requires an existing
ClickHouse with the single account described in
[`deploy/README.md`](deploy/README.md). Its relevant flags are shown by
`./build/open-splunk-server -help`. Direct `go build` is only a compile check.
For deterministic UI-only demo work:

```sh
OPEN_SPLUNK_DATA_MODE=demo npm run build
```

Production browser traffic is same-origin. The API is rooted at `/api`, the
search WebSocket at `/api/search/ws`, and the native gRPC service is
`open_splunk.CollectorIngestService/Collect`. See [API contracts](docs/api.md).

## Reproducible artifacts and releases

Local artifact checks retain the full source revision as their identity:

```sh
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" \
make release
```

This is an artifact-verification path, not the local run workflow and not a
publication command. The launcher
requires a clean committed revision, materializes only
committed inputs into a disposable tree, installs pinned tools into fresh
caches, scrubs ambient workspace/build controls, verifies the embedded payload
and linked binary identities, and publishes atomically under `build/`.
Server and collector images must come from the same source revision. See
[Build and publication status](docs/releasing.md).

A non-draft, non-prerelease `v0.x.y`
[GitHub Release](https://github.com/Suhaibinator/open-splunk/releases) triggers
the publication CI. A successful publication attaches Linux AMD64/ARM64 binary
archives and checksums and publishes multi-architecture
[server](https://github.com/Suhaibinator/open-splunk/pkgs/container/open-splunk-server)
and [collector](https://github.com/Suhaibinator/open-splunk/pkgs/container/open-splunk-collector)
images to GHCR. To connect the server image to an existing ClickHouse Compose
service, follow [Deployment](deploy/README.md).

## Product boundaries

The backend includes protobuf HTTP, authenticated native gRPC ingestion, the
optional HEC facade, collector WAL/tailing, SQLite control plane, ClickHouse
event storage, bounded jobs/exports, saved searches and dashboards, field
knowledge, immutable CSV lookups, auditing, and the cumulative authored SPL
profile documented in [SPL](docs/spl.md).

The v0 contract supports persisted state only with the same exact release or
source revision. Retaining data across arbitrary versions or source revisions
is not a compatibility promise. Unknown or inconsistent databases, state
directories, backups, cursors, WAL/checkpoints, snapshots, and retained
artifacts fail closed and are never silently deleted or rewritten. See
[Architecture](docs/architecture.md) and [database mechanics](migrations/README.md).

Native collectors are documented in [Ingestion](docs/ingestion.md). HEC is
disabled by default, shares the existing HTTP listener, and exposes only its
exact unversioned routes; see [HEC](docs/hec.md).

## Styling

`app/layout.tsx` loads exactly one stylesheet, `app/styles/index.css`, and that
file is nothing but an ordered `@import` list, so the cascade order is written
down rather than implied by module order or by the bundler. Every rule is plain
CSS with feature-prefixed, kebab-case class names — there are no CSS modules
and no second way to scope a rule.

Colour, spacing, radius, type, stacking, elevation and motion are named by
tokens in `app/styles/tokens-color.css` and `app/styles/tokens-scale.css`, and
only those two files are meant to carry a literal. **Recolouring the product is
an edit to the token files, not a search across the rules.**

Three gates keep that true, because almost none of it is visible to a compiler:
`npm run test:frontend` runs the structural invariants in
`scripts/style-invariants.test.mjs`, `npm run lint:css` runs stylelint over the
stylesheets, `npm run test:contracts` reads rules back through
`getComputedStyle` in a browser. [Theming](docs/theming.md) is the reference:
where a rule lives, how to restyle a given thing, what each token means, and
what every gate can and cannot see.

## Integration gates

The default Go/frontend suite is self-contained. ClickHouse, shipped-browser,
and HEC load/soak tests are opt-in because they start containers
or long-running workloads. Common entry points include:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec -run TestExecutorAndManagerAgainstClickHouse

npm ci
npx --no-install playwright install chromium
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run TestBackendVertical

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./migrations/clickhouse -run TestMigrationsAgainstClickHouse -v
```

Use the repository-pinned ClickHouse digest unless intentionally comparing a
different pinned image. [Integration testing](integration/README.md) describes
the backend, browser, and load gates; the
[HEC reference](docs/hec.md) documents its protocol-specific load, soak, and
slow-client gates.

## Documentation

Start with the [documentation index](docs/README.md). Topic-specific references
are grouped below:

- Core contracts: [Architecture](docs/architecture.md), [API](docs/api.md), and
  [SPL](docs/spl.md).
- Data and ingestion: [Knowledge and lookups](docs/knowledge.md),
  [native ingestion](docs/ingestion.md),
  [collector configuration](docs/collector-configuration.md),
  [HTTP Event Collector](docs/hec.md), and [auditing](docs/auditing.md).
- Interface: [Theming](docs/theming.md) — the token layer, where a rule lives,
  and the guardrails that keep both true.
- Operations and validation: [deployment](deploy/README.md),
  [database mechanics](migrations/README.md),
  [integration testing](integration/README.md), and
  [build scripts](scripts/README.md).
- Publication and future work:
  [build and publication status](docs/releasing.md) and the
  [roadmap](docs/roadmap.md).

The repository intentionally does not retain release-by-release implementation
plans or cross-revision upgrade guides.
