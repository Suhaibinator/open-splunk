# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together
with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Latest checkpoint: authenticated ClickHouse TLS

Date: 2026-07-31

Committed implementation checkpoint:

- `772837b` — verified server-to-ClickHouse TLS, secure Compose health,
  generated CA/server identity, production-client live coverage, full-tree Go
  lint, and restored fixed-timechart ClickHouse CI coverage.

This test-first deployment-foundation unit removes the remaining normal
container-network transport blocker without weakening the existing local
plaintext boundary:

1. `open-splunk-server` accepts `-clickhouse-ca-cert` and
   `-clickhouse-server-name` only with `-clickhouse-secure`. Secure mode
   requires both values. The CA input is a regular file capped at 1 MiB and is
   parsed as certificate-only PEM with no trailing data. Server names are
   bounded ASCII DNS names or IP addresses; ports, wildcards, controls,
   whitespace, empty labels, and malformed names fail closed.
2. The loader returns an unexported verified TLS profile rather than accepting
   caller-built `tls.Config` values. The profile is the only path that can mint
   ClickHouse client configurations, so custom clocks, entropy, callbacks,
   client certificates, session state, key logging, mutable policy slices, and
   skip-verification cannot enter the application path. Every migrator,
   runtime, deletion, and isolated EXPLAIN lane receives independently owned
   roots and a TLS 1.2 minimum with ordinary Go chain and hostname/SAN
   verification.
3. Normal startup loads trust and builds all three principal options before
   the administrator token, server lock, SQLite, collector recovery, or either
   ClickHouse persistence connection opens. Malformed trust therefore creates
   no control database, key, WAL, or export directory. Embedded-release
   verification remains independent of mounted runtime secrets. The migration
   password is still removed from the process environment after options capture
   and cleared with the short-lived migration options after schema startup.
4. The checked-in digest-pinned Compose stack enables secure native port 9440,
   mounts the server certificate/key and explicit CA read-only, and gives the
   container healthcheck a strict client configuration. Health executes an
   authenticated `SELECT 1` over 9440 with the configured explicit SNI name and
   runtime principal; `up --wait` cannot succeed merely because plaintext 9000
   is alive. Host publication remains loopback-only. Plaintext 9000 remains
   only for container-local bootstrap and explicit loopback diagnostics; the
   application must use 9440.
5. `deploy/generate-env.sh` creates four independent 256-bit credentials, a
   P-256 path-length-zero local CA, and a CA-signed P-256 server certificate
   with `clickhouse`, `localhost`, IPv4 loopback, and IPv6 loopback SANs. The
   one-use CA signing key and request/config intermediates are destroyed. The
   owner-only identity directory is atomically reserved, `.env` publication is
   no-overwrite, concurrent generators cannot replace each other, paths with
   spaces are safely quoted for POSIX sourcing and Compose, and
   shell-significant output paths are rejected.
6. `TestClickHouseTLSServicePrincipalStartupLifecycle` starts only the
   canonical digest-pinned ClickHouse image with secure native publication. It
   live-proves the production trust loader and option builder, startup
   migrations, migration-credential disposal, runtime and deletion privilege
   validation, a runtime query, a deletion operation, and the separate custom
   EXPLAIN TLS dialer. Wrong hostname, wrong CA, and plaintext-to-9440 attempts
   fail.
7. `TestDeploymentComposePersistentCredentialRotation` now uses the actual
   CA-signed identity and credentials emitted by `generate-env.sh`. It proves
   strict health, verified TLS state, TLS 1.0/1.1 rejection, wrong-name,
   wrong-root, and plaintext rejection; validates every principal and schema;
   preserves the named data volume across container replacement; rotates all
   credentials; rejects every old credential; and revalidates the recovered
   stack. Opted-in Docker/Compose tests fail rather than silently skip when a
   prerequisite is missing.
8. CI Go lint now enforces the complete tree with golangci-lint v2.12.2 instead
   of a historical `--new-from-rev` baseline. The backend-vertical job now
   selects the production TLS lifecycle and the previously omitted
   `TestExecutorAndManagerAgainstClickHouse`, including fixed unsplit
   `timechart count`, empty-input schema, and pre-storage-floor bucket cases.
9. Simplification reviewers removed duplicated fixture argument construction,
   consolidated generator test helpers, closed the TLS-policy abstraction,
   bounded non-regular CA inputs, and removed a redundant post-rotation
   negative handshake suite. Independent final security,
   deployment/lifecycle, and CI/coverage reviewers reported no remaining
   P0-P2 findings in the frozen implementation diff.
10. GORM ownership is unchanged: SQLite/control-plane only. ClickHouse schema,
    event persistence, transport, migration, query, statistics, inspection,
    and deletion remain native ClickHouse paths.
11. The architecture plan remains unfinished. The next deployment unit is the
    non-root release OCI image and full-stack Compose wiring, followed by the
    actual GradeThis collector cutover with no OpenTelemetry component in the
    log path. Additional SPL such as unsplit percentile timecharts remains a
    later semantic expansion, not the next release blocker.

Validation on implementation commit `772837b`:

```sh
go mod tidy -diff
go test ./... -count=1
go test -race -shuffle=on \
  ./cmd/open-splunk-server ./internal/testsupport \
  ./migrations/clickhouse -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
make proto

GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache \
  /Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint \
  run ./...

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./cmd/open-splunk-server ./internal/clickhouse \
  ./internal/queryexec ./integration ./migrations/clickhouse \
  -run '^Test(ClickHouseTLSServicePrincipalStartupLifecycle|IndexStatisticsReaderAgainstClickHouse|ExecutorAndManagerAgainstClickHouse|DeploymentComposePersistentCredentialRotation|BackendIndexDataDeletionLifecycle|BackendVertical|Browser(FixedResultRendering|SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=25m -p=1 -v
```

The complete Go unit suite, focused race/shuffle suite, tidy verification,
vet/build, cached v2.12.2 full-tree lint, TypeScript typecheck/lint, all 47
release/materializer tests, all 137 frontend runtime tests, production UI
build, and protobuf format/lint/generation checks passed. Go lint reported
`0 issues`. The exact serial backend-vertical command passed every selected
package and live contract. Docker inspection found no remaining test-owned
container or volume.

Explicit pause point:

1. Authenticated ClickHouse TLS is committed and live-proven through every
   production principal plus isolated EXPLAIN.
2. Compose readiness now proves the generated identity and secure native query
   path; transport failures cannot hide behind plaintext health.
3. The next unit is non-root release OCI/full-stack Compose, not optional SPL.
4. Commit and push this handoff, verify CI, then pause until the user gives
   further instructions.

## Previous checkpoint: native HTTPS browser/API listener

Date: 2026-07-30

Committed implementation checkpoint:

- `e0d8b2e` — native HTTPS for the browser UI, protobuf API, and WebSocket
  surface; fail-closed non-loopback listener policy; shared inbound TLS
  baseline; digest-pinned backend evidence; and adversarial unit/browser/live
  integration coverage.

This test-first deployment-foundation unit removes the browser-listener half
of the normal container-network transport blocker without weakening the local
development boundary:

1. The server accepts paired `-http-tls-cert` and `-http-tls-key` PEM paths.
   Both paths are trimmed and must be present together. Plaintext browser/API
   traffic remains loopback-only; a non-loopback or wildcard bind is accepted
   only when HTTPS is configured, and a wildcard bind still requires explicit
   `-http-allowed-hosts`. The dead prerelease
   `-http-insecure-trusted-network` flag was removed rather than retained as a
   misleading no-op or revived as a bearer-token plaintext bypass.
2. Release-payload verification still exits without requiring mounted runtime
   secrets. After that early exit, the HTTPS certificate/key pair is loaded
   before the administrator token, server lock, SQLite, or ClickHouse opens.
   An unreadable, malformed, or mismatched identity therefore fails before
   either persistence plane can be mutated. The loaded keypair stays in the
   in-memory `tls.Config`; `ListenAndServeTLS("", "")` never reopens mutable
   key files after validation.
3. Browser/API HTTPS and collector gRPC now share one inbound server-identity
   loader and a TLS 1.2 minimum. A small `http.Server` wrapper selects
   loopback plaintext or preloaded HTTPS while preserving the existing
   coordinated HTTP/gRPC lifetime, graceful shutdown, request drain, WebSocket
   close, and second-signal behavior.
4. The existing administrative Host/Origin boundary derives its expected
   scheme from the actual TLS connection, so HTTPS protobuf calls and WSS
   upgrades retain exact same-origin enforcement. No forwarded-header or
   trusted-proxy shortcut was introduced. Browser HTTP/2 remains enabled for
   ordinary HTTPS resources.
5. The backend vertical now creates an ephemeral ECDSA P-256 identity with an
   IP SAN. Go HTTPS clients trust only that generated certificate and preserve
   TLS 1.2 minimum verification. The Playwright harness ignores certificate
   errors only behind an explicit HTTPS-only integration flag, resets that
   flag between browser modes, and still restricts the target to a credential-
   free loopback origin.
6. The Go WSS client clones the verified HTTP client's TLS trust but pins ALPN
   to `http/1.1`, which Gorilla WebSocket requires. A real run exposed that
   reusing HTTP transport state could otherwise carry negotiated `h2` into the
   WebSocket dialer. Focused tests now pin HTTP/1.1, preserve TLS/version/trust
   state, prove the source transport is not mutated, and fail closed for
   missing or untrusted transport shapes.
7. The real vertical proves HTTPS health/static UI/protobuf traffic, WSS
   progress and terminal frames, the compiled browser, plaintext rejection on
   the TLS port, collector and server crash/restart durability, redaction,
   exports, the exact GradeThis corpus, and current GradeThis searches against
   the digest-pinned ClickHouse release.
8. High-value backend fixtures now resolve an empty image override to the
   repository's pinned ClickHouse image and reject tag-only, missing-name,
   short, uppercase, or trailing-text digest forms. Lower-level fixture
   helpers remain flexible for explicitly selected compatibility benchmarks;
   evidence described as digest-pinned cannot silently inherit a mutable
   ambient tag.
9. Simplification review reused the bounded shared Playwright runner, removed
   unused generated-certificate state, shared the inbound TLS baseline, and
   removed the dead plaintext override. Independent final security,
   lifecycle/interoperability, and coverage reviewers found no remaining
   P0-P2 issue in frozen staged diff
   `eede24f98108c6a8b2b744bd1f1fcf6cbf758debc9759b75b4c45c2e657f8bfd`.
10. GORM ownership is unchanged: SQLite/control-plane only. HTTP transport,
    collector gRPC, ClickHouse connections, ingestion, planning, execution,
    and integration remain native.
11. The architecture plan remains unfinished. A normal Compose bridge still
    needs verified server-to-ClickHouse TLS: explicit CA and server-name
    options plus the pinned ClickHouse secure native listener. After that
    checkpoint, build the non-root release OCI targets, full-stack Compose,
    crash-safe bootstrap, dedicated volumes/schema boundary, and actual
    GradeThis no-OTel cutover. Do not publish an OCI image first that cannot
    securely reach ClickHouse on its bridge network.

Validation on implementation commit `e0d8b2e`:

```sh
go mod tidy -diff
go test ./... -count=1
go test -race -shuffle=on ./... -count=1
go test -race -shuffle=on \
  ./cmd/open-splunk-server ./internal/testsupport ./integration -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
make proto

# Executed with this already-cached binary reporting exactly v2.12.2.
HTTPS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache \
  "$HTTPS_LINTER" run ./...

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=15m -v

git show --check --oneline e0d8b2e
```

The complete repository unit suite, full race/shuffle suite, final focused
race suite, tidy verification, vet/build, cached v2.12.2 Go lint, TypeScript
typecheck/lint, all 47 release/materializer tests, all 137 frontend runtime
tests, production UI build, and protobuf format/lint/generation checks passed;
Go lint reported `0 issues`. The final uninterrupted digest-pinned HTTPS/WSS
backend vertical passed in 19.22 seconds (20.36 seconds including package
startup), and Docker inspection found no remaining test-owned Open Splunk
container or volume.

Explicit pause point:

1. Native browser/API HTTPS is committed and live-proven through protobuf,
   WSS, and the compiled browser.
2. Plaintext remains loopback-only; no trusted-network bypass exists.
3. The next deployment-foundation unit is verified ClickHouse client/server
   TLS, not OCI packaging or more optional SPL.
4. Commit and push this handoff, then pause until the user gives further
   instructions.

## Previous checkpoint: exact-field eventstats count

Date: 2026-07-30

Committed and pushed implementation checkpoint:

- `711a6b1` — exact-field `eventstats count(field) AS output`, optional exact
  `BY`, shared occurrence semantics with `stats`, bounded row-preserving
  ClickHouse lowering, and adversarial unit/live integration coverage.

This test-first unit extends the deliberately narrow `eventstats` surface
without widening its execution envelope:

1. The accepted field form has exactly one unquoted exact input in
   `count(field)`, followed by `AS exact_output` and optionally one through
   sixteen distinct exact `BY` fields. `c(field)`, quoted or wildcard inputs,
   empty or multiple inputs, eval expressions, omitted aliases, other
   functions, multiple measures, and options fail at their source ranges.
   Argument-free `eventstats count` remains unchanged.
2. The parser, AST, planner, referenced-field analysis, defensive validators,
   suggestions, and result-shape metadata now distinguish row count from
   field-occurrence count. A forged plan with noncanonical input metadata,
   predicate metadata, percentile metadata, or another aggregate function
   still fails closed.
3. `count(field)` resolves and counts the upstream field before applying the
   output alias. Alias replacement therefore cannot make a measure read its
   own result. A projected-away input remains missing and contributes zero
   instead of being recovered from hidden event storage.
4. Missing values, explicit nulls, empty multivalues, and null multivalue
   members contribute zero. Other scalars and typed containers contribute one.
   A top-level multivalue contributes its immediate non-null member count,
   including duplicates and without recursive traversal. Fixed
   `Array(String)`, stored `Array(Dynamic)`, and calculated homogeneous
   `Array(String)` values are live-proven.
5. Global totals are non-null `UInt64` values attached to every upstream row.
   Grouped totals are attached only to rows with complete eligible `BY`
   tuples; an eligible group whose members all contribute zero receives a
   present zero, while an incomplete tuple leaves the output absent. Existing
   container poisoning and lexical Dynamic grouping rules are unchanged.
6. The occurrence input is projected once into the existing 10,001-row
   sentinel relation. Occurrences aggregate through `UInt128` and convert to
   `UInt64` only at the bounded output. The independent row-count guard still
   rejects 10,001 upstream rows even when every occurrence contribution is
   zero, so a low measure total cannot bypass the stage limit.
7. Global lowering adds one constant-size sum to the existing materialized
   total. Grouped lowering adds the same sum to the existing bounded group
   aggregate and left join. It performs no `ARRAY JOIN`, `groupArray`,
   per-group query, Go-side buffering, or row expansion.
8. The reserved open-event `fields` payload remains unreadable as a measure
   and unusable as an output alias until an upstream transforming command
   establishes a closed schema. GORM ownership is unchanged: SQLite/control
   plane only; ClickHouse ingestion, compilation, execution, and integration
   remain native.
9. A pinned ClickHouse run exposed that `length(Dynamic)` is statically
   `Nullable(UInt64)`. The homogeneous-array branch now normalizes only that
   dispatch result with `ifNull(..., 0)`, preserving a definite `UInt64`
   measure without changing `Array(Dynamic)` non-null-member semantics.
   Integration assertions pin both values and database column types.
10. Adversarial review also found that the first calculated-array live cases
    were vacuous: a two-member array plus an empty array had the same total as
    two incorrectly scalar-counted values. Final tests isolate the nonempty
    value at two and the empty value at zero, group them independently by
    `event_id`, and prove the shared `stats count(lowered)` path with an
    eight-member array and a `UInt64` result.
11. Simplification review removed an unused global occurrence aggregate from
    grouped plans, removed redundant parser state, shared forged-plan cloning,
    and consolidated live-result collection. Final independent semantics,
    ClickHouse/boundedness, and test-coverage reviews confirmed frozen staged
    diff
    `aff91744a27ef3d6c937d4bde2e12fcf50ebe720536a53521751c664a1e9fa58`
    with no remaining P0–P2 finding.
12. The architecture plan remains unfinished. The highest first-release
    deployment milestone is still the actual GradeThis Compose cutover. It
    first needs the Open Splunk deployment foundation: a release OCI image and
    full-stack Compose contract, explicit listener/TLS topology, bootstrap
    tooling, a dedicated ClickHouse schema/volume boundary, and current
    dashboard wiring. That work belongs in a dedicated GradeThis branch after
    the foundation exists; no next unit is selected here.

Validation on implementation commit `711a6b1`:

```sh
go mod tidy -diff
go test ./... -count=1
go test -race -shuffle=on ./... -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
make proto
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
EVENTSTATS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache \
  "$EVENTSTATS_LINTER" run ./...

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' \
  -count=1 -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=15m -v

git show --check --oneline 711a6b1
```

The full repository unit and race/shuffle suites, tidy verification, vet/build,
cached v2.12.2 Go lint, TypeScript typecheck/lint, all 47 release/materializer
tests, all 137 frontend runtime tests, production UI build, and protobuf
format/lint/generation/breaking checks passed; Go lint reported `0 issues`.
The digest-pinned Store/compiler integration passed in 61.44 seconds, the
digest-pinned backend vertical passed in 24.57 seconds, and Docker inspection
found no remaining test-owned container or volume.

Explicit pause point:

1. Exact-field `eventstats count(field)` is committed, pushed, and
   digest-pinned live-proven.
2. Its occurrence semantics, zero-versus-absent grouped behavior, output
   types, row ceiling, and composition with `stats` are pinned.
3. GORM remains control-plane-only; ClickHouse remains native-only.
4. Commit and push this handoff, then pause until the user gives further
   instructions.

## Previous checkpoint: bounded row-preserving eventstats count

Date: 2026-07-30

Committed and pushed implementation checkpoint:

- `490451b` — exact argument-free `eventstats count`, optional `AS` and exact
  `BY`, row-preserving logical planning, bounded materialized ClickHouse
  aggregation, atomic runtime guards, and adversarial unit/live integration
  coverage.

This test-first unit adds the first deliberately narrow `eventstats`
compatibility slice:

1. The accepted grammar is exactly one argument-free `count`, an optional
   exact `AS` alias, and an optional `BY` tuple of one through sixteen exact
   fields. `count()`, field and eval arguments, other functions, multiple
   measures, options, quoted fields, wildcard fields, and aliases after `BY`
   fail at their source ranges with bounded command-specific diagnostics and
   suggestions.
2. A dedicated `EventAggregate` logical operator makes the row-preserving
   contract explicit. It preserves input cardinality, source-event identity,
   established order, every other visible field, the current result kind, and
   the existing index authorization scope. It participates fail-closed in
   dependency analysis, field-analysis eligibility, timeline eligibility,
   inspection projection, and defensive result validation.
3. Global `count` is attached to every upstream row. Grouped count is attached
   only to rows with a complete non-null `BY` tuple; rows with a missing or
   explicit-null key remain in the result with a logically absent/nullable
   aggregate. Dynamic scalar groups reuse stats' lexical normalization, so
   integer `500` and string `"500"` converge on the same key.
4. A runtime list, object, or flattened object parent in any scoped Dynamic
   grouping field poisons the whole command before output, including when
   another key is missing or a downstream predicate would otherwise hide the
   row. Fixed multivalue groups are rejected at compilation. These boundaries
   prevent containers from being silently stringified into misleading groups.
5. An alias replaces an existing field in place. Replacing `_time` correctly
   invalidates timeline eligibility, while replacing `index` remains
   calculated pipeline data and cannot alter the physical authorization
   selector. The reserved open-event `fields` payload cannot be read or
   replaced until an upstream table or transforming command establishes an
   exact closed schema.
6. Each eventstats stage accepts at most 10,000 input rows. Its scoped input is
   materialized once with a 10,001-row sentinel, and the guarded aggregate
   fails the whole query before any result row when the boundary is exceeded.
   The executor classifies the private marker as `ErrExecutionLimit` without
   leaking it to the user.
7. Global lowering reads the bounded materialized relation for its rows and
   total. Grouped lowering adds one unique-key aggregate and a left join back
   to those rows. It performs no per-group query, row expansion, `groupArray`,
   Go-side buffering, or unbounded window state; existing read, time, memory,
   query-depth, subquery, result, and group-cardinality limits remain active.
8. Stats and eventstats now share one parameterized exact-`BY` parser and one
   ClickHouse scalar-group descriptor for resolution, presence, lexical keys,
   descendant-aware validation, and bind ordering. Command-specific syntax
   and multivalue diagnostics remain exact, preventing future stats fixes from
   drifting away from eventstats.
9. Composition is live-proven in both directions: stats followed by
   eventstats, eventstats followed by stats, and grouped eventstats followed by
   a second global eventstats. Downstream `head` cannot change the upstream
   total, upstream `head` does, empty input produces no rows, aliases replace
   prior values, and `where isnull(...)` observes missing/null group outputs.
10. Parser, source-range, suggestion, result-shape, plan, analysis, timeline,
    inspection, compiler SQL/bind-order, forged-plan, fixed-multivalue,
    executor-marker, and frontend completion tests cover the entire bounded
    surface. Forged wrong-function, duplicate-group, malformed-field,
    invalid-output, empty-metadata, and typed-nil operators fail closed in
    every metadata consumer and at the compiler boundary.
11. The pinned ClickHouse fixture stores 10,001 real events in a dedicated
    `eventstats-boundary` index. Compiler-produced queries prove the exact
    10,000-row success boundary, visible and downstream-pruned 10,001-row
    atomic failure, consecutive materialized CTE stages, sparse group
    presence, numeric/string convergence, tenant scoping, and whole-query
    container poisoning. The dedicated index and collector keep this large
    adversarial fixture out of unrelated compiler scans.
12. Independent correctness, efficiency/boundedness, and reuse/test-quality
    reviews first found a misleading duplicate-field message, duplicated
    parser/lowering logic, incomplete forged-validator coverage, missing
    compiler-produced boundary coverage, and boundary-fixture scan overhead.
    All were fixed. The final frozen-diff reviews reported no remaining
    production, correctness, reuse, test-quality, allocation, bind-order,
    query-plan, or fixture-isolation finding.
13. Persistence ownership remains unchanged. GORM is used only for the SQLite
    control plane; ClickHouse event ingestion, search compilation/execution,
    statistics, field discovery, and physical deletion remain native.
14. The architecture plan remains unfinished. The actual GradeThis Compose
    cutover is still the highest first-release deployment milestone and
    belongs in its own GradeThis worktree/branch. No next implementation unit
    is selected here; this checkpoint intentionally pauses for user direction.

Validation on implementation commit `490451b`:

```sh
go mod tidy -diff
go test ./... -count=1
go test -race -shuffle=on ./... -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
EVENTSTATS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
"$EVENTSTATS_LINTER" run ./...

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline 490451b
```

The full repository unit and race suites, tidy verification, vet/build, exact
cached v2.12.2 linter, TypeScript typecheck/lint, all 47 release/build
transaction tests, all 137 frontend runtime tests, production UI build, and
protobuf generation/lint/breaking checks passed; Go lint reported `0 issues`.
The final digest-pinned Store/compiler integration passed in 64.10 seconds,
the digest-pinned backend vertical passed in 24.19 seconds, and Docker
inspection found no remaining test-owned container or volume.

Explicit pause point:

1. Bounded row-preserving `eventstats count` is committed, pushed, and
   digest-pinned live-proven.
2. Its runtime boundary is atomic and its global/grouped semantics compose
   with existing event and stats pipelines.
3. GORM remains control-plane-only; ClickHouse remains native-only.
4. Commit and push this handoff, then pause until the user gives further
   instructions.

## Previous checkpoint: bounded revision-stable index catalog

Date: 2026-07-30

Committed and pushed implementation checkpoint:

- `e85fc3c` — hard-bounded physical index identities, atomic GORM admission,
  revision-stable keyset pages, server-signed continuations, defensive
  transport validation, and adversarial unit/live integration coverage.

This test-first unit replaces the last serialized full-catalog
index-administration path:

1. The catalog now admits at most 1,024 physical index identities. Active,
   archived, deleting, and terminally tombstoned rows all consume the bound,
   so a ClickHouse-facing canonical name can never be recycled after product
   deletion.
2. Migration 0020 adds the authoritative singleton physical-count/catalog-
   revision marker, composite name/created/updated keyset indexes, persisted
   byte and timestamp bounds, and triggers that reject overflow, identity
   replacement, physical deletion, marker rollback, and accounting drift.
3. Creation performs duplicate-name, capacity, and random-ID admission plus
   insertion in one explicit GORM transaction. SQLite's configured immediate
   writer admission makes the final slot atomic; an N-1 two-creator race
   proves that exactly one succeeds. A duplicate canonical name still wins
   over capacity at the error boundary.
4. Startup and creation admission perform a bounded structural audit of the
   singleton against physical rows. The keyset page hot path reads only the
   guarded singleton, and the post-insert check reads only its new revision
   and count, avoiding an O(catalog) scan while holding a page or writer
   transaction.
5. `ListIndexPage` is a GORM/SQLite read-only transaction with page sizes
   capped at 64, `LIMIT page_size + 1`, optional exact filtered totals, and
   deterministic `(name|created_at|updated_at, index_id)` keysets in both
   directions. The legacy full index view remains bounded by the physical
   ceiling.
6. State filters and literal text matching execute in SQLite. ASCII letters
   are case-insensitive, non-ASCII bytes remain case-sensitive, and `%`, `_`,
   quotes, and other wildcard-looking input remain literal. The transport's
   defensive alternate-provider validation reuses a guaranteed-linear,
   one-time-preprocessed ASCII-fold KMP matcher.
7. Continuations are authenticated before storage work and bind the endpoint,
   normalized filters, page semantics, sort, direction, statistics mode,
   global catalog revision, and final composite key. Create, update, lifecycle,
   deletion-admission, completion, and tombstone mutations invalidate an
   outstanding cursor instead of risking skips or duplicates.
8. SQLite page selection now happens before acquiring the shared
   administrative response permit, so a busy database cannot occupy response
   capacity. The permit covers only bounded validation/materialization, is
   released during native page-statistics work, and is reacquired before
   defensive enrichment and protobuf transfer.
9. Existing page-local ClickHouse statistics behavior is unchanged: zero
   native queries for an empty page, one grouped event query for an all-empty
   nonempty page, and one additional shared `system.parts` query otherwise.
   Event-count and storage-byte global sorts remain rejected because a
   metadata revision cannot freeze changing native statistics.
10. Alternate control providers are checked for revision, page size, totals,
    cursor identity, ordering, uniqueness, filters, canonical definitions,
    timestamps, and persisted bounds before serialization or native work.
    Cursor-shape validation and page-response validation are shared with the
    GORM and other bounded-list boundaries so their limits cannot drift.
11. Adversarial tests cover every sort/direction with tied keys, literal and
    non-ASCII filtering, malformed and stale cursors, every catalog mutation,
    failed multirow rollback, tombstone capacity retention, marker and
    identity replacement attempts, direct schema overflow, structural drift,
    startup auditing, final-slot concurrency, and GORM/schema parity.
12. Independent reuse, quality/correctness, and efficiency reviews drove the
    shared cursor/page/matcher validators, canonical display-name check,
    centralized catalog/time bounds, response-permit ordering, state-only hot
    path, startup/admission audit split, and linear matcher. All three
    post-fix reviews reported no remaining concrete issue.
13. Persistence ownership remains strict: GORM is used only for the SQLite
    control plane. ClickHouse event storage, statistics, field discovery,
    search execution, and physical deletion remain native.
14. The actual GradeThis Compose cutover remains the highest first-release
    deployment milestone and belongs in its own GradeThis worktree/branch.
    The next selected in-repository SPL expansion candidate remains a
    separately bounded `eventstats count` contract.

Validation on implementation commit `e85fc3c`:

```sh
go test ./... -count=1
go test -race ./internal/control ./internal/server \
  ./cmd/open-splunk-server ./integration -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
INDEX_CATALOG_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
"$INDEX_CATALOG_LINTER" run ./...

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline e85fc3c
```

The full repository suite, affected race suite, vet/build, TypeScript
typecheck/lint, all 47 release/build tests, all 136 frontend runtime tests,
protobuf generation/lint/breaking checks, and exact cached v2.12.2 linter
passed; lint reported `0 issues`. The digest-pinned backend vertical passed in
24.51 seconds and left no test-owned container or volume.

Explicit pause point:

1. The bounded, revision-stable index catalog is committed, pushed, and
   digest-pinned live-proven.
2. Control-plane indexing and paging use GORM; ClickHouse remains native-only.
3. The larger architecture plan still has future release and SPL units.
4. Commit and push this handoff, then pause until the user gives further
   instructions.

## Previous checkpoint: truthful complete index administration capability

Date: 2026-07-30

Committed implementation checkpoint:

- `a219074` — complete-family `SERVER_FEATURE_INDEX_ADMIN` advertisement,
  fail-closed partial compositions, wire-stable protobuf documentation, and
  unit/runtime/live-production coverage.

This test-first unit closes the deliberate bootstrap suppression left while
the index route family was incomplete:

1. The first focused test failed because a fully configured handler still
   returned zero `SERVER_FEATURE_INDEX_ADMIN` values. The implementation now
   manages that feature through the same normalization path as the other
   service-derived capabilities.
2. Advertisement requires the entire configured family: base index
   administration, native single and page-batched statistics plus its
   visibility snapshotter, native index field discovery, and durable physical
   deletion admission plus its running coordinator wake boundary.
3. Partial embedded and test configurations remain valid. Administration
   alone, or any valid pair of the statistics, field, and physical-deletion
   groups, remains feature-suppressed rather than overstating
   `include_stats`, `stats/get`, `fields/list`, or `DELETE_DATA`.
4. A statically requested feature cannot bypass service composition.
   Incomplete configurations remove it, while a complete configuration
   collapses duplicate requested values to exactly one.
5. Typed-nil dependencies are normalized before capability calculation, and
   the existing statistics/snapshotter and deletion-admission/waker pairing
   invariants still fail invalid half-configurations during construction.
6. The feature is server capability discovery, not a live ClickHouse health
   signal or a future RBAC entitlement. Transient dependency failures remain
   per-operation errors, and every index route independently retains the
   administrator browser-authentication and Host/Origin boundary.
7. Production composition is covered at both seams. The runtime unit proves
   that base administration plus the indirectly injected index-field service
   is still partial, while the real server supplies GORM administration,
   native statistics/snapshotting and fields, and the durable deletion
   coordinator before handler construction.
8. The digest-pinned backend vertical requires the standalone production
   bootstrap to advertise the feature exactly once. That same fixture already
   executes index creation, single statistics, page-batched list statistics,
   field-catalog paging, and authenticated production routing before its
   GradeThis collector/search proof.
9. The protobuf enum number and wire format are unchanged. Its source comment
   now defines complete-family/configuration semantics, and both generated Go
   and TypeScript bindings were regenerated reproducibly.
10. Persistence ownership is unchanged. GORM remains restricted to the
    SQLite control plane; index statistics, field analysis, event storage, and
    physical mutations continue through native ClickHouse services. This unit
    adds no migration.
11. Independent reuse, quality/correctness, and efficiency reviews found no
    actionable reuse, configuration, typed-nil, auth, production-composition,
    protobuf, concurrency, boundedness, or ORM-boundary defect. The explicit
    paired-dependency checks remain because they state the complete-family
    contract and cost only two startup pointer comparisons.
12. The next in-repository control-plane debt is bounding the index catalog
    and replacing its serialized full-catalog filtering/sorting with bounded
    GORM admission and keyset pagination. The actual GradeThis Compose cutover
    remains the highest first-release deployment milestone and belongs in its
    own GradeThis worktree/branch. The next selected SPL expansion candidate is
    a separately bounded `eventstats count` contract.

Validation on implementation commit `a219074`:

```sh
go test ./... -count=1
go test -race ./internal/server ./cmd/open-splunk-server ./integration \
  -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
INDEX_ADMIN_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
"$INDEX_ADMIN_LINTER" run --timeout=10m \
  --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline a219074
```

The full repository suite, complete affected race suite, vet/build,
TypeScript typecheck/lint, all 47 release/build tests, all 136 frontend runtime
tests, protobuf generation/lint/breaking checks, and exact cached v2.12.2
linter passed; lint reported `0 issues`. The digest-pinned backend vertical
passed in 24.39 seconds and cleaned up every test-owned container and volume.

Explicit pause point:

1. Complete index administration is truthfully discoverable, committed, and
   live-proven.
2. Partial service compositions remain legal and fail closed at discovery.
3. GORM remains control-plane-only; ClickHouse remains native-only.
4. Commit and push this handoff, then pause until the user gives further
   instructions.

## Previous checkpoint: bounded owner-scoped export job listing

Date: 2026-07-30

Committed implementation checkpoint:

- `6803951` — bounded owner-and-tenant export-job listing, authenticated
  high-water cursors, deterministic scoped indexes, live lifecycle filtering,
  exact per-request totals, hardened protobuf projection, and unit/vertical
  coverage.

This test-first unit implements
`POST /api/v1/search/exports/list`:

1. The route is present only when the export service is enabled, is exact and
   POST-only, uses the browser API Host/Origin boundary before body decoding,
   and is represented in the TypeScript protobuf route map. Disabled
   deployments return `404`; hostile Host/Origin requests never reach the
   export service.
2. Manager requests are scoped by trusted tenant and owner. Admission and
   listing share the same nonempty, canonical, UTF-8, control-free identity
   rules, including the exact search-job filter. Cross-scope results are
   non-disclosing, and the server independently rejects any corrupt service
   projection carrying the wrong scope.
3. Results use immutable `created_at DESC, export_job_id DESC` ordering.
   Each scope owns a deterministic treap keyed by those admission fields, so
   mutation and keyset seek are expected `O(log N)` and ordinary bounded pages
   scan in chunks without sorting the manager-wide job map.
4. The authenticated, purpose-separated cursor binds manager epoch, caller
   scope, canonical state and exact search-job filters, fixed ordering, last
   key, and the first page's admission high-water mark. Later admissions are
   excluded even with reversed clocks or reused IDs; deletion of the anchor
   does not prevent continuation. Tampered, unsigned, cross-manager,
   cross-purpose, filter-mismatched, and signed future-high-water cursors fail
   closed.
5. Page size defaults to and is capped at 15, while tokens are capped at 4
   KiB. Page size and exact-total inclusion may change on continuation;
   filters may not. Exact totals count the currently retained matching jobs at
   or below that traversal's high-water for the individual request that
   computes them rather than claiming a frozen lifecycle snapshot.
6. Lifecycle state remains live between calls. A due terminal job is expired
   before state filtering, while unacknowledged terminal work is not expired.
   List-triggered expiry invalidates stale grants but preserves an already
   pinned download lease. Cancellation and shutdown are rechecked after an
   entry-lock wait before expiry or cloning, so a canceled read cannot mutate a
   blocked entry or transfer a response.
7. List concurrency is a fail-fast four-slot manager gate. Saturation returns
   a classified `429` instead of accumulating waiters that hold every global
   serialization permit. Only response candidates are deeply detached;
   sparse exact totals do not clone every retained column slice.
8. Metadata accounting includes every treap node and a conservative scope-map
   allowance. Removing the job that supplied a scope key rebinds storage to a
   retained entry, and cleanup rebuilds the scope map after any scope deletion
   so Go's high-water buckets cannot survive after their accounting is
   released. Generation overflow and cleanup remove every job/index/accounting
   reservation atomically.
9. `export.ValidPublicJob` is the shared manager/transport contract for hard
   IDs, limits, columns, options, lifecycle timestamps, canonical failures,
   and canonical artifact metadata. Byte limits are checked before UTF-8 or
   control scans, so a corrupt alternate service cannot cause unbounded work.
   Progress cannot predate creation, start, or finish. Transport clock skew is
   not treated as corruption; only protobuf timestamp range and internal
   ordering are enforced.
10. The handler acquires one shared serialization permit before calling the
    bounded manager, validates scope, uniqueness, ordering, filters, totals,
    tokens, and every public-job invariant, and retains that permit through
    protobuf serialization. The response has an independent 8 MiB ceiling,
    cancellation suppresses transfer, and dependency or cursor details are
    never disclosed.
11. List responses contain detached export definitions, progress, stable
    failure metadata, and safe artifact metadata only. They never mint,
    contain, or serialize a download capability. The backend vertical creates
    and completes exports, retains an existing grant, pages with an exact
    state/search filter, changes page size on continuation, proves a later
    export is excluded by the high-water mark, proves a fresh traversal sees
    it, and checks that response wire bytes do not contain the grant token.
12. GORM and ClickHouse boundaries are unchanged: this unit adds no migration
    and no ClickHouse ORM use. GORM remains limited to SQLite control-plane
    work, while export execution continues through the existing native
    ClickHouse-backed search snapshot path.

Validation on implementation commit `6803951`:

```sh
go test ./... -count=1
go test -race ./internal/export ./internal/server ./integration \
  ./cmd/open-splunk-server -count=1
go vet ./...
go build ./...
npm run typecheck
npm run lint
npm run test:frontend
make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
EXPORT_LIST_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
"$EXPORT_LIST_LINTER" run --timeout=10m \
  --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline 6803951
```

The full repository suite, affected race suite, vet/build, TypeScript
typecheck/lint, both frontend test groups, protobuf generation/lint/breaking
checks, and exact cached v2.12.2 linter all passed; lint reported `0 issues`.
The digest-pinned backend vertical passed and cleaned up its test-owned Docker
resources.

Independent reuse, quality/correctness, and efficiency reviews drove shared
public-job and pagination validation, canonical identifier rules, clock-skew
handling, post-lock cancellation checks, fail-fast list saturation, exact
scope-map accounting/compaction, byte-first validation, and monotonic progress
timestamps. Final post-fix reviews found no remaining actionable reuse,
correctness, concurrency, boundedness, cursor, route, response, or
manager/transport-contract defect. The package-local search/export treaps and
cursor wrappers remain intentionally explicit because their private entry
types, lock invariants, fingerprints, domains, and error mappings differ; the
common authenticated codec is already shared.

Explicit pause point:

1. Bounded export-job listing is implemented, committed, digest-pinned
   live-proven, and ready to push with this handoff.
2. Listing is owner-and-tenant scoped, high-water stable across admissions,
   lifecycle-live, resource bounded, and incapable of issuing or leaking
   download capabilities.
3. GORM remains control-plane-only; ClickHouse remains native.
4. Pause after pushing this checkpoint until the user gives further
   instructions.

## Previous checkpoint: bounded administrator index field catalog

Date: 2026-07-30

Committed and pushed implementation checkpoint:

- `6668cd6` — administrator-only index field discovery, GORM-backed selector
  resolution, immutable no-job analysis snapshots, native ClickHouse
  profiling, bounded cache/cursors, and unit/live/vertical coverage.

This test-first unit implements
`POST /api/v1/indexes/fields/list` without creating a search job or accepting
caller-written SPL:

1. Browser authentication and administrator authorization complete before
   request-body decoding, GORM access, snapshotting, or ClickHouse work.
   Unauthorized, non-administrator, and cross-tenant callers cannot use body
   parsing or dependencies as an oracle.
2. Every request resolves its ID or canonical-name selector through the
   existing GORM/SQLite index catalog. The stable ID, canonical name, and
   current version become trusted service inputs. Search-disabled,
   `ARCHIVED`, and outstanding `DELETING` records remain inspectable, while a
   terminal tombstone returns `404` before analysis.
3. `time_range`, `earliest`, and `latest` are required. Absolute or relative
   intent is resolved once for an initial request. The service then captures
   one committed visibility cutoff followed by one canonical UTC clock anchor
   used identically for `SearchStart` and `IndexTimeCutoff`.
4. The service constructs an empty-AST raw-event plan from the trusted tenant,
   one resolved canonical index, absolute half-open event-time interval, and
   immutable snapshot. It rejects any planner output other than exactly one
   scope-preserving `Scan`; browser input cannot inject SPL or widen tenant or
   index scope.
5. ClickHouse compilation and execution stay on the existing native field
   catalog path, not GORM. The parameterized scan enforces
   `tenant_id/index_name`, `event_time` in `[earliest, latest)`,
   `index_time <= snapshot_anchor`, `expires_at > snapshot_anchor`, and
   `visibility_seq <= visibility_cutoff`.
6. The native executor buffers and validates the complete bytewise-sorted
   catalog before publishing anything. Metadata version, normalized names,
   durable observed types, total/present/null/missing count invariants, the
   header, row ordering, result schema, and overflow sentinel all fail closed.
   Known canonical fields remain visible with zero counts for an empty index.
7. Catalogs admit at most 10,000 profiles and request one extra ordered group
   as a truncation sentinel. A query is capped at 15 seconds, 128 MiB memory,
   five million source rows, 1 GiB source bytes, 10,001 groups, two threads,
   32 MiB result data, and the existing bounded query/subquery sizes. Overflow
   modes throw, materialized CTE and short-circuit behavior are required, and
   async insertion plus ClickHouse query caching are explicitly disabled.
8. Index and completed-search catalogs share one `FieldService` lifecycle,
   fail-fast four-slot computation gate, coalescing, and bounded LRU while
   retaining explicit cache and cursor domains. The default catalog cache is
   128 entries and 64 MiB with a five-minute absolute TTL.
9. A miss captures and executes one immutable catalog. Continuations perform
   neither snapshot nor native work and require the exact live cache
   generation. Their domain-separated HMAC cursors bind service instance,
   caller, index ID/name/version, original time intent, snapshot, filter,
   generation, exact filtered total, offset, and scan position.
10. An equivalent ID/name selector and a different valid page size may
    continue the same catalog. A caller, filter, intent, version, snapshot,
    restart, eviction, or expiry change fails closed. Authenticated scan
    positions make all filtered continuation pages together linear in the
    catalog rather than repeatedly rescanning prefixes.
11. Field profiles expose exact presence, explicit-null, and missing counts
    plus the complete sorted observed-type set. `selected` and `interesting`
    reuse the completed-search rules. Index catalogs deliberately do not
    calculate distinct counts; the HTTP boundary rejects even otherwise-valid
    distinct data from a regressed or alternate service implementation.
12. Native work, snapshotting, coalesced waits, and cache access occur before
    the global large-response permit. The handler acquires that permit only
    while validating and serializing the bounded page, enforces an independent
    32 MiB protobuf ceiling, releases the permit on every malformed response,
    and sanitizes all dependency and compiler details.
13. Production wires the same field service to both completed-search and
    administrator index catalog routes, with the runtime snapshotter supplied
    explicitly. The frontend route map and protobuf contract comments now
    advertise the endpoint and its required-time/no-distinct semantics.
14. GORM remains control-plane-only. ClickHouse field reads use the native
    driver, existing migration `0003` already contains the required metadata,
    and the existing event-table `SELECT` grant is sufficient. No migration or
    grant was added.
15. The digest-pinned native fixture proves tenant, index, earliest, exclusive
    latest, index-time, expiry-equality, and visibility isolation with poison
    rows. The real backend vertical authenticates the production route, pages
    from ID to an equivalent name selector with a changed page size, and
    verifies exact profiles against direct ClickHouse counts.

Validation on implementation commit `6668cd6`:

```sh
go test ./... -count=1
go test -race ./internal/queryexec ./internal/searchanalysis \
  ./internal/server ./cmd/open-splunk-server ./integration -count=1
go vet ./...
go build ./...
npm run typecheck
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" npx --no-install buf breaking \
  --against '.git#branch=main'

# Executed with this already-cached binary reporting exactly v2.12.2.
INDEX_FIELDS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
GOTOOLCHAIN=local GOPROXY=off "$INDEX_FIELDS_LINTER" \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$/field_catalog_compiler_and_executor' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline 6668cd6
```

The final repository suite, affected race suite, vet/build, TypeScript,
protobuf format/lint/breaking checks, and exact cached v2.12.2 linter all
passed; lint reported `0 issues`. Both final digest-pinned live suites passed
and cleaned up their test-owned Docker resources.

Independent native, route/security, and state/cursor adversarial reviews drove
query-cache hardening, an index-specific no-distinct transport invariant, and
GORM lifecycle coverage for disabled, archived, deleting, and tombstoned
records. Post-fix reviews found no remaining actionable authentication,
identity, snapshot, cursor, cache, lifecycle, predicate, resource-bound,
response-contract, production-wiring, or GORM/native-boundary defect.

Explicit pause point:

1. Administrator index field discovery is implemented, committed,
   digest-pinned live-proven, and pushed with this handoff.
2. The endpoint uses one trusted GORM-resolved index and one immutable native
   ClickHouse catalog; no search job, caller SPL, migration, or new grant is
   involved.
3. GORM remains limited to SQLite control-plane work; ClickHouse remains
   native.
4. Pause here until the user gives further instructions.

## Previous checkpoint: bounded page-batched index list statistics

Date: 2026-07-30

Committed and pushed implementation checkpoint:

- `fc3e6e3` — page-local `include_stats` enrichment, one grouped native
  ClickHouse read plus at most one shared part sample, cursor-mode binding,
  two-phase response admission, and unit/live/vertical coverage.

This test-first unit enables `ListIndexesRequest.include_stats` without
pretending that page-local enrichment can safely implement global statistics
ordering:

1. GORM/SQLite remains the catalog authority. The existing control-plane list,
   state/text filtering, name/created/updated ordering, catalog snapshot,
   cursor validation, and page selection all complete before a native
   statistics request is constructed.
2. The existing serialization permit still admission-bounds concurrent
   pre-existing full-catalog GORM materialization and in-memory
   filtering/sort. Only the selected page is cloned, with an endpoint maximum
   of 64 records; the full filtered backing slice is dropped and the permit is
   released before ClickHouse or visibility-snapshot work begins.
3. An empty selected page performs no visibility, clock, or native work.
   A nonempty enriched page captures exactly one committed visibility cutoff,
   then exactly one UTC millisecond `measured_at`, and sends the trusted
   deployment tenant plus resolved immutable index IDs/canonical names.
4. The native reader validates at most 64 unique protocol IDs and canonical
   names before querying. An empty native batch is a non-nil empty result with
   zero queries; an all-empty page uses one grouped logical query; a page with
   any events uses that query plus exactly one `system.parts` aggregate.
5. The grouped query is parameterized and applies the same
   `tenant_id/index_name`, `expires_at > measured_at`,
   `index_time <= measured_at`, and
   `visibility_seq <= visibility_cutoff` predicates as the single-index
   endpoint. ClickHouse groups that are absent become explicit exact empty
   results, and output is restored to request/catalog page order.
6. Unknown, duplicate, zero-count, malformed, or out-of-range ClickHouse
   groups fail closed. Row scan, iteration, `Err`, and `Close` failures are
   checked, cancellation wins over driver errors, and every successful query
   handle is closed exactly once.
7. Every nonempty page result uses one common active-table rows/bytes sample.
   The logical count total is overflow-checked and must not exceed the
   physical row sample before each per-index proportional ceiling estimate is
   computed. Empty indexes retain zero storage and omitted bounds.
8. Single and batch statistics share the same one-slot fail-fast operation
   gate and ten-second overall deadline, so the administrator surface cannot
   occupy multiple shared runtime sessions or queue behind a saturated
   statistics read.
9. The grouped query retains the single-reader memory, rows/bytes-read,
   result-byte, thread, cache, async-insert, subquery, and overflow bounds. It
   additionally limits groups and result rows to 64. A batch-only 64 KiB
   `max_query_size` safely covers clickhouse-go's client-expanded 64
   maximum-length index parameters; the single-index limit remains 16 KiB.
10. After native work, the handler reacquires a serialization permit before
    validating and attaching output. Results must contain exactly one unique
    echo for every selected index and the exact trusted
    tenant/name/ID/cutoff/time scope. Existing empty/nonempty shape,
    timestamp, storage, and response-size validation remains authoritative.
11. The signed index cursor binds whether statistics were requested, so plain
    and enriched tokens cannot cross modes. Plain index cursors retain their
    legacy fingerprint bytes, and the generic token/app fingerprint is
    unchanged; only `include_stats=true` uses a domain-separated index-list
    derivation.
12. Statistics on one response page share one measurement instant and cutoff.
    A continuation page is measured independently because the protobuf
    contract exposes `measured_at` per item rather than one persistent
    list-wide statistics snapshot. Catalog ordering remains stable through
    the existing ID/version snapshot.
13. Missing/typed-nil/partially paired statistics dependencies fail closed.
    Dependency and cancellation failures remain sanitized as `503`/`408`;
    native work never holds the serialization permit, and saturation before
    catalog access prevents another full-catalog read from starting.
14. The GORM boundary did not move into ClickHouse: SQLite performs catalog
    work only, while event and `system.parts` reads remain native. No schema,
    migration, or runtime-grant change was required.
15. `INDEX_SORT_BY_EVENT_COUNT` and `INDEX_SORT_BY_STORAGE_BYTES` remain
    rejected. Correct global ordering must measure every filtered catalog
    candidate and freeze that ordering across ingestion, TTL/retention
    removal, and changing part metadata. The current catalog has no
    product-owned row cap, so this requires a separate catalog-admission or
    immutable-snapshot design.
16. Native integration covers two nonempty indexes and one explicit empty
    index in request order, including tenant, index, retention, index-time, and
    visibility isolation plus a shared storage sample. The real backend
    vertical now authenticates `POST /api/v1/indexes/list` with
    `include_stats=true` through production wiring and verifies the returned
    page item against direct ClickHouse reads at its own measurement instant.
17. The existing CI opt-in regex already selects the extended native reader
    and backend vertical tests, so no duplicate container gate was added.

Validation on implementation commit `fc3e6e3`:

```sh
go test ./... -count=1
go test -race ./internal/clickhouse ./internal/server \
  ./integration ./cmd/open-splunk-server -count=1
go vet ./...
go build ./...

# Executed with this already-cached binary reporting exactly v2.12.2.
INDEX_LIST_STATS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
GOTOOLCHAIN=local GOPROXY=off "$INDEX_LIST_STATS_LINTER" \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
  -run '^TestIndexStatisticsReaderAgainstClickHouse$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

git show --check --oneline fc3e6e3
```

The final implementation passed the full repository suite, the affected race
suite, repository-wide vet/build, and the exact cached v2.12.2 linter with
`0 issues`. The final digest-pinned native batch reader and backend vertical
passed in 6.03 and 24.06 seconds respectively.

Independent native, route-contract, efficiency, and end-to-end adversarial
reviews drove fixes for driver-expanded query size, serialization-gate
lifetime, unavailable-dependency admission, and unrelated cursor-fingerprint
compatibility. Post-fix reviews found no remaining diff-scoped route, identity,
predicate, overflow, cancellation, concurrency, query-count, resource-bound,
cursor, production-wiring, or GORM/native-boundary defect.

Explicit pause point:

1. Page-batched index-list statistics are implemented, committed, live-proven,
   and pushed with this handoff.
2. Native enrichment is strictly bounded to the selected page: one grouped
   event query plus at most one active-parts query, never N+1.
3. GORM remains limited to SQLite control-plane work; ClickHouse remains
   native.
4. The serialized full-catalog GORM scan remains pre-existing architectural
   debt; global statistics ordering is unsupported pending bounded catalog
   admission/database pagination and immutable ordering semantics.
5. Pause here until the user gives further instructions.

## Previous checkpoint: bounded administrator index statistics

Date: 2026-07-29

Committed implementation checkpoint:

- `b9b6c6b` — administrator-only single-index statistics, exact logical
  visibility/retention semantics, bounded native ClickHouse execution,
  least-privilege part metadata, and unit/live integration coverage.

This test-first unit implements the previously declared
`POST /api/v1/indexes/stats/get` contract without broadening list-index
behavior:

1. The route is registered only when index administration, native statistics,
   and visibility snapshotting are configured together. Typed-nil,
   partially-paired, or missing catalog dependencies fail closed during
   handler construction.
2. Browser authentication and administrator authorization run before body
   decoding or any catalog/native work. The request can select only by index ID
   or canonical name; the server resolves a current non-tombstoned catalog
   record first.
3. Scope is built only from the trusted configured tenant plus the resolved
   immutable index ID/name. Browser input cannot select a tenant or substitute
   a physical name.
4. The handler captures the largest committed visibility sequence before it
   samples one UTC, millisecond-aligned `measured_at` instant. The same
   snapshot and instant are echoed through the native result and checked again
   at the protobuf boundary.
5. One parameterized native aggregate returns exact logical `count`,
   `minOrNull(event_time)`, and `maxOrNull(event_time)` under
   `tenant_id/index_name`, `expires_at > measured_at`,
   `index_time <= measured_at`, and
   `visibility_seq <= visibility_cutoff`.
6. Empty results use exactly one query, return zero count/storage, and omit
   both bounds. Nonempty results require ordered, ClickHouse/protobuf-range
   bounds and use one additional `system.parts` aggregate.
7. `storage_bytes` is the overflow-safe ceiling of
   `active_table_bytes * logical_event_count / active_table_rows`. Positive
   counts with zero rows/bytes, a logical count above the physical row count,
   arithmetic overflow, or racing/inconsistent metadata fail closed.
   `estimates` is always true because only storage attribution is estimated;
   logical count and event-time bounds remain exact.
8. A single-slot fail-fast native gate lets statistics occupy at most one
   session in the shared runtime pool. Concurrent saturation returns an
   unavailable result without issuing a ClickHouse query, so administrator
   calls cannot monopolize ingestion/search sessions; cancellation releases
   the slot and a later call succeeds.
9. The complete two-query operation owns a ten-second context. This accounts
   for clickhouse-go's roughly five-second deadline addition and prevents a
   long caller deadline from widening the configured fifteen-second
   `max_execution_time`.
10. Both queries are read-only and carry explicit memory, rows/bytes-read,
    result rows/bytes, thread, query-size, subquery-depth, cache, async-insert,
    and overflow bounds. Identifiers are construction-validated and quoted;
    all scope values are parameters; each query gets a unique protocol-valid
    query ID.
11. The runtime principal gains only
    `SELECT(database, table, active, rows, bytes_on_disk) ON system.parts`.
    Exact `SHOW GRANTS` validation uses ClickHouse 26.3's canonical backticked
    `table` spelling and rejects missing, duplicate, broad, role-derived, or
    option-bearing authority. Reads of other `system.parts` columns and
    statistics access by the deletion principal are denied live.
12. Production creates the reader over the already-owned runtime native
    connection and the existing committed-visibility sequencer. It borrows
    that connection, owns no second close path, and remains behind the normal
    HTTP request drain during shutdown.
13. The GORM/SQLite control plane is used only for selector/catalog resolution
    and visibility metadata. Event aggregation and part metadata remain
    native ClickHouse operations; GORM is not introduced into the ClickHouse
    path.
14. Dependency/cancellation failures map to sanitized `503`/`408` responses.
    Malformed echoed tenant/index/snapshot/time scope, zero storage for a
    positive count, inconsistent empty/nonempty shapes, invalid timestamps,
    and aliased pointer results are rejected before serialization without
    leaking dependency details.
15. `ListIndexesRequest.include_stats`,
    `INDEX_SORT_BY_EVENT_COUNT`, and `INDEX_SORT_BY_STORAGE_BYTES` remain
    explicitly rejected. Batched statistics and statistics sorting require a
    separate bounded query design.
16. The real backend vertical authenticates the route after collector
    ingestion and verifies its count and time bounds against direct
    ClickHouse reads at the captured measurement/snapshot. The standalone
    native reader, exact service-principal lifecycle, and checked-in Compose
    credential-rotation stack are separate digest-pinned gates.
17. CI now runs the standalone reader and the checked-in Compose deployment
    test serially beside the backend vertical under the exact audited image
    digest.

Validation on implementation commit `b9b6c6b`:

```sh
go test ./... -count=1
go test -race ./internal/clickhouse ./internal/server \
  ./cmd/open-splunk-server ./internal/testsupport \
  ./migrations/clickhouse -count=1
go vet ./...
go build ./...

# Executed with this already-cached binary reporting exactly v2.12.2.
INDEX_STATS_LINTER=/Users/suhaib/Library/Caches/go-build/06/067cb7bcb62095cd55b9becb2d5964b88a2ff4deecb1b39f4724f6a4b4d68df1-d/golangci-lint
GOTOOLCHAIN=local GOPROXY=off "$INDEX_STATS_LINTER" \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
  -run '^TestIndexStatisticsReaderAgainstClickHouse$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/server \
  -run '^TestClickHouseServicePrincipalLifecycle$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./migrations/clickhouse \
  -run '^TestDeploymentComposePersistentCredentialRotation$' \
  -count=1 -timeout=10m -v

git show --check --oneline b9b6c6b
```

The final implementation passed the full repository suite, the affected race
suite, repository-wide vet/build, and the exact cached v2.12.2 linter with
`0 issues`. The final digest-pinned native reader, privilege lifecycle,
backend vertical, and Compose rotation passed in 6.07, 2.79, 22.20, and 17.55
seconds respectively.

Three independent adversarial review lenses drove fixes for clickhouse-go
deadline widening, runtime-pool starvation, nonempty zero-byte results,
canonical `system.parts` grant spelling, and missing checked-in Compose CI
coverage. Post-fix re-reviews found no remaining route, scope, predicate,
overflow, cancellation, concurrency, privilege, deployment, resource-bound,
or GORM-boundary defect.

Explicit pause point:

1. Bounded single-index administrator statistics are implemented,
   committed, live-proven, and published with this handoff.
2. Exact logical count/time semantics share the same committed visibility and
   retention boundaries as search; compressed storage remains explicitly
   estimated.
3. GORM remains limited to SQLite control-plane work; ClickHouse remains
   native.
4. Batched list statistics and statistics-based ordering remain unsupported.
5. Pause here until the user gives further instructions.

## Previous checkpoint: authenticated physical index deletion admission

Date: 2026-07-29

Committed and pushed implementation checkpoint:

- `d687acf` — authenticated `DELETE_DATA` admission, trusted-tenant binding,
  postcommit coordinator wake, shutdown ordering, and unit/live lifecycle
  coverage.

This test-first unit exposes the previously isolated physical-deletion
pipeline through the production administrator API:

1. `POST /api/v1/indexes/delete` admits `DELETE_DATA` only when the handler has
   a complete index-administration, deletion-admission, and wake dependency
   set. Typed-nil and partial configurations fail closed during construction;
   `KEEP_DATA` remains available with index administration alone.
2. Authentication and administrator authorization run before admission.
   Physical scope is constructed only from the handler's trusted deployment
   tenant; no request field can select or replace it.
3. The route validates the optimistic version and mode before selector lookup,
   requires an exact canonical-name confirmation, rejects `MaxInt64`, and
   accepts only archived generation `N` or the exact outstanding retry seen as
   deleting generation `N+1`.
4. `BeginIndexDataDeletion` remains the GORM/SQLite transaction boundary. The
   handler validates the returned protocol ID, tenant, logical identity,
   archived/deleting versions, and positive, protobuf-range,
   microsecond-aligned timestamp before returning the index ID and deletion
   operation ID.
5. A successful durable admission always reaches a synchronous, nonblocking
   wake call, including when response-context cancellation races after the
   commit. Pre-admission rejections and `BeginIndexDataDeletion` errors never
   wake. Periodic coordinator recovery remains the correctness path.
6. Sequential, 24-way concurrent, selector-equivalent, and SQLite-restart
   handler tests require every exact outstanding retry to return one stable
   operation ID and leave one durable operation.
7. Deterministic KEEP-versus-DELETE race tests exercise both commit orders and
   require exactly one HTTP winner, preventing the synchronous tombstone path
   and asynchronous physical path from both succeeding.
8. Success-output adversarial tests require the wake even when a dependency
   returns malformed operation metadata; the handler then fails closed rather
   than exposing an untrusted operation identity.
9. Production startup passes the single GORM-backed control DB as deletion
   admission and the already-owned coordinator runtime as a narrow wake
   capability. ClickHouse target resolution, mutation submission/status, and
   physical proof remain native and do not use GORM.
10. Runtime `Wake` and `Close` are linearized: wake calls admitted before close
    finish before shutdown begins, while calls after close starts are ignored.
    HTTP shutdown first stops new requests and drains all active handlers, so
    each successfully admitted deletion completes its postcommit wake before
    runtime/Store teardown.
11. The digest-pinned live process test starts the compiled server with real
    SQLite and ClickHouse, rejects malformed/unauthorized/missing/stale/active
    requests without effects, and sends 16 simultaneous exact admissions that
    converge on one operation and one correlated mutation.
12. `SYSTEM STOP MERGES` holds the mutation while the test hard-kills the
    server. Reopened SQLite proves the operation and attempt survived; a
    restarted server and normalized-name retry recover the same operation
    before merges resume.
13. Terminal proof waits on the authoritative correlated mutation state, then
    checks physical scope once: only the trusted tenant/canonical index rows
    disappear, while a foreign tenant's same-name row and the trusted tenant's
    neighboring-index row survive.
14. A final hard stop and repeated SQLite reopen prove the immutable completion
    audit, consumed operation/attempt, retained deleting generation, catalog
    tombstone, and permanent name reservation.
15. HTTP replay is intentionally bounded to the outstanding operation. Once
    terminal completion consumes it and hides the catalog entry, the same
    request returns `404 Not Found`; no indefinite terminal replay/status
    contract is claimed.
16. CI now includes this lifecycle in the backend integration job and supplies
    the exact audited ClickHouse image digest. Process startup/poll failure
    diagnostics redact the administrator token and every ClickHouse password.

Validation on implementation commit `d687acf`:

```sh
go test ./... -count=1
go test ./cmd/open-splunk-server ./internal/server \
  -run 'Test(ShutdownDrainsDeletionAdmissionWakeBeforeRuntimeClose|TrackedHandlerRejectsNewWorkAndWaitsForActiveWork|IndexDataDeletion|DeleteIndex)' \
  -count=10
go test -race ./cmd/open-splunk-server ./internal/server \
  ./internal/control ./internal/indexes -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration \
  -run '^TestBackendIndexDataDeletionLifecycle$' -count=1 -timeout=10m -v

git show --check --oneline d687acf
```

The exact implementation passed the full repository suite, repeated focused
tests, selected race detection, repository-wide vet/build, and pinned lint
with `0 issues`. The final digest-pinned real-process lifecycle passed in
18.87 seconds.

Three parallel adversarial review lenses over the staged implementation drove
fixes for shutdown/wake races, timing-dependent assertions, duplicate polling
and transport helpers, unbounded probe contexts, redundant full-table polling
and INSERT round trips, duplicate process cleanup, raw secret-bearing failure
logs, opaque test tuples, and misplaced cross-suite fixtures. Post-fix reviews
found no remaining route, tenant, idempotency, shutdown, resource-bound, or
ClickHouse-scope defect.

Explicit pause point:

1. Authenticated physical-deletion admission and its production wake/shutdown
   lifecycle are implemented, committed, pushed, and live-proven.
2. Exact retry identity is guaranteed while an operation is outstanding;
   terminal replay is deliberately not part of this route.
3. GORM remains limited to the SQLite control plane; ClickHouse remains native.
4. Pause here until the user gives further instructions.

## Previous checkpoint: production deletion runtime and ClickHouse privilege isolation

Date: 2026-07-29

Committed and pushed implementation checkpoint:

- `7994dcb` — production coordinator/Store ownership, separate migration,
  runtime, and deletion principals, restart-safe deployment provisioning, and
  pinned unit/live integration coverage.

This test-first unit completes the production lifecycle and restricted-DDL
boundary without enabling HTTP `DELETE_DATA` admission:

1. Server startup opens one short-lived migrator connection, pings it,
   validates the exact ClickHouse version and complete explicit grant surface,
   applies the embedded files, and closes it before either long-lived
   connection or the Store exists.
2. The ordinary runtime principal has exactly `SELECT, INSERT` on
   `open_splunk.events`. The deletion principal has column-scoped
   `SELECT(tenant_id, index_name)`, `ALTER DELETE`, and `SELECT` on
   `system.tables` and `system.mutations`. All three identities are direct
   users without roles, grant/admin options, partial revokes, or wildcard
   authority.
3. Principal validation pins server version `26.3.17.4`, reads the whole
   canonical `SHOW GRANTS` result in one round trip, rejects every missing,
   duplicate, malformed, role, option-bearing, or excessive row, and requires
   ClickHouse Code 497 from a non-public `system.server_settings` canary.
   Version validation runs before migration DDL as well as before long-lived
   principal use.
4. The application removes
   `OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD` from its environment immediately
   after capturing connection options, then clears and releases the privileged
   options as soon as the migration session closes. All three application
   usernames and passwords must be pairwise distinct.
5. `Store` now owns separate ordinary and deletion connections. Ingestion and
   search stay on the runtime connection. Physical target resolution,
   mutation status/reconciliation, `ALTER DELETE`, and terminal zero proof use
   only the serialized deletion connection. Construction-failure ownership,
   dual ping, close ordering, joined errors, cancellation, and concurrent
   operation/close behavior have focused tests.
6. The server constructs exactly one `IndexDataDeletionCoordinator` beside the
   single Store after migrations and privilege validation. The coordinator
   owns final Store shutdown, while all later collector/search/export/HTTP
   consumers unwind first.
7. Deletion-runtime close starts exactly one asynchronous owner pipeline:
   unbounded coordinator join followed by exactly-once Store close. Each caller
   context still bounds its wait, including a blocked driver close. If the
   graceful budget expires, production performs a later unbounded join before
   allowing visibility/SQLite dependencies to unwind. A second process signal
   retains the ordinary forced-termination escape hatch.
8. The configured tenant is checked for UTF-8 and every Unicode control
   character before surrounding whitespace is normalized, so boundary
   newlines and C1 controls cannot be silently canonicalized. Native deletion
   requests independently require the same unpadded, control-free tenant
   grammar before any ClickHouse query.
9. HTTP `DELETE_DATA` remains disabled. The rejection test proves the request
   fails before selector validation and leaves the archived index, optimistic
   version, and outstanding-operation state unchanged.
10. The checked-in Compose image is pinned to
    `clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
    Initialization validates that exact version before DDL, validates four
    independent 256-bit lowercase-hex credentials, and reruns its idempotent
    migrator-first sequence on every start so partial provisioning and
    credential rotation recover over an existing volume.
11. The config-backed bootstrap administrator now reads its password from the
    container environment and permits only `127.0.0.1` and `::1`.
    `CLICKHOUSE_SKIP_USER_SETUP=1` prevents the official entrypoint from
    recreating a `::/0` administrator. Recovery is available through
    `docker compose exec`, while published ports remain loopback-bound and
    reject bootstrap authentication.
12. A new opt-in integration test uses the actual
    `deploy/docker-compose.yaml`, shell initializer, XML files, migration
    directory, healthcheck, and persistent named volume. It proves fresh
    provisioning, external-bootstrap denial/internal recovery, exact grants,
    migrated schema, data persistence, all-secret rotation through
    `--force-recreate`, old-credential rejection, and successful revalidation.
13. The reusable principal fixture retains unexported bootstrap credentials,
    writes its nonsecret bind-mounted XML with Linux-container-readable
    permissions, bounds canceled Docker subprocess pipe waits, and supports a
    narrowly scoped adversarial GRANT/REVOKE hook. Backend vertical/load tests
    share one principal environment/flag harness.
14. ClickHouse 26.3 necessarily maps `ALTER DELETE` to destructive partition
    operations including `DROP PARTITION`; column-scoping `ALTER DELETE`
    prevents the required fixed mutation from running. That audited residual
    is contained by the private fixed-SQL deletion connection. Event `SELECT`
    is separately column-scoped, and the live contract confirms
    `ALTER MOVE PARTITION` is absent.
15. GORM remains confined to the SQLite control plane. ClickHouse migrations,
    grant inspection, target/status queries, mutation submission, physical
    proof, and deployment validation remain native.

Validation on implementation commit `7994dcb`:

```sh
go test ./... -count=1
go test ./cmd/open-splunk-server ./internal/clickhouse ./internal/server \
  ./internal/testsupport ./migrations/clickhouse -count=10
go test -race ./cmd/open-splunk-server ./internal/clickhouse \
  ./internal/server ./internal/testsupport -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
sh -n deploy/clickhouse-init.sh deploy/generate-env.sh

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/server \
  -run '^TestClickHouseServicePrincipalLifecycle$' -count=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./migrations/clickhouse \
  -run '^TestDeploymentComposePersistentCredentialRotation$' -count=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/indexes \
  -run '^TestIndexDataDeletionCoordinatorAgainstClickHouse$' -count=1 -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./integration -run '^TestBackendVertical$' -count=1 -v

git show --check --oneline 7994dcb
```

The exact implementation passed the full repository suite, ten repeated
focused runs, race detection, repository-wide vet/build, and pinned lint with
`0 issues`. The pinned principal lifecycle passed in 3.03 seconds; the actual
Compose fresh-start/credential-rotation proof passed in 17.66 seconds; the
composed deletion crash/restart proof passed in 6.17 seconds; and the complete
backend/browser vertical passed in 21.55 seconds.

Three parallel adversarial review lenses over the staged implementation found
and drove fixes for unsafe timeout dependency teardown, an unbounded Store
close, a missing migrator allowlist, tag-only/one-shot deployment
initialization, a network-wide bootstrap administrator, a fixture permission
portability bug, boundary control-character canonicalization, weaker native
tenant validation, stale deployment documentation, duplicated backend
principal wiring, and five pinned-lint failures. A separate pinned ClickHouse
privilege audit confirmed the unavoidable `ALTER DELETE` partition blast
radius. The post-fix automated and live validation found no further failure.

Remaining `DELETE_DATA` work:

1. enable the authenticated route by deriving `IndexDataDeletionScope` only
   from the trusted handler tenant, never request data;
2. preserve early confirmation/version/mode checks, call
   `BeginIndexDataDeletion`, and issue a best-effort postcommit coordinator
   wake while retaining periodic recovery as the correctness path;
3. add handler-level concurrency, restart, and live API proofs that admission
   cannot cross tenants or mutate state on any rejected request; and
4. keep ClickHouse native and GORM limited to the control plane.

Explicit pause point:

1. Production coordinator/Store ownership, principal isolation, deployment
   recovery, and their unit/live proofs are implemented, committed, and pushed.
2. The HTTP `DELETE_DATA` route remains deliberately disabled.
3. The next cohesive deletion unit is authenticated route admission and wake
   wiring only.
4. Pause here until the user gives further instructions.

## Previous checkpoint: durable admission-time deletion tenant binding

Date: 2026-07-29

Committed and pushed implementation checkpoint:

- `31d20ac` — immutable admission-tenant snapshots, cross-tenant GORM/SQLite
  integrity, pre-native coordinator drift rejection, and a live wrong-tenant
  restart proof.

This test-first unit closes the coordinator's pre-attempt tenant-drift gap
without wiring the server runtime or enabling the `DELETE_DATA` route:

1. `BeginIndexDataDeletion` now accepts an explicit
   `IndexDataDeletionScope` derived from trusted process identity. The
   immutable GORM operation snapshots its validated tenant together with the
   exact index ID/name, archived generation, and admission time. Same-tenant
   sequential, concurrent, and restart retries return the original operation;
   a different valid tenant returns `ErrDependencyConflict` and cannot rebind
   the work.
2. The shared control-plane tenant grammar lives in
   `internal/control/tenant.go`: 1..255 UTF-8 bytes, no surrounding Unicode
   whitespace, NUL, or Unicode control characters. Invalid admission tenants
   fail before database reads, entropy, or state transition. Decoder checks
   make malformed persisted tenant values fail closed.
3. Because the project is greenfield and unreleased, the existing 0017 schema
   was strengthened in place rather than adding a fallback/backfill migration.
   `index_deletion_operations.tenant_id` is non-null and immutable, with
   byte-length, NUL, ASCII-space, and C0/C1 control guards. An already-created
   pre-release database with the old 0017 checksum must be rebuilt; no tenant
   is guessed from runtime configuration.
4. Migration 0018 and the GORM attempt path both require the mutation target
   tenant to equal the parent operation tenant. Fresh mismatches return
   `ErrDependencyConflict` without creating an attempt; a persisted
   operation/attempt mismatch is invalid durable state. Direct SQL cannot
   insert a foreign-tenant child.
5. Migration 0019 now requires the completion, attempt, and outstanding
   operation tenants to agree before terminal insertion, operation deletion,
   or trigger-owned cleanup. Completion reconstruction carries the immutable
   tenant through the synthetic operation/attempt validation, and the terminal
   audit remains the durable tenant copy after outstanding rows are consumed.
6. The coordinator validates the oldest operation tenant against its
   constructor-cloned configured tenant before
   `GetIndexDeletionMutationAttempt`, `IndexDataDeletionStatus`,
   `WithWritesFrozen`, drain, target resolution, or mutation advancement. A
   mismatched oldest operation remains cached and rate-limited, so younger
   operations cannot bypass the failure. Fresh targets derive tenant from the
   durable operation rather than inferring it from current configuration.
7. Existing attempts must match operation identity and admission tenant before
   native polling. Concurrent-ensure terminal-audit and ambiguous-completion
   recovery also require the audit tenant to match the admitted operation in
   addition to the exact logical generation and physical database/table/UUID.
8. Control-plane tests cover invalid UTF-8/whitespace/control/NUL/oversized
   tenants, exact retrieval and restart, cross-tenant retries, simultaneous
   tenant A/B admission with one immutable winner, GORM column parity,
   direct-SQL constraints and immutability, attempt parent mismatch, completion
   forgery, cancellation, and unchanged archived/deleting state after every
   rejected path.
9. Coordinator units prove a mismatched operation is discovered once, retained
   across retries, and rejected before even an attempt read; all ClickHouse
   status/freeze calls remain zero. Existing-attempt tenant rejection and the
   previously proven frozen advancement/ambiguity paths remain green.
10. The digest-pinned composed SQLite/ClickHouse test now includes a
    deliberately misconfigured process lifetime after admission and restart.
    While one ambiguous outbox row remains durably held, that coordinator
    reports tenant drift, creates no attempt, makes zero status/freeze calls,
    leaves the outbox and target event unchanged, and closes cleanly. A
    correctly configured restart then replays the row, executes the existing
    precommit-failure and commit-then-EOF schedule, deletes only the admitted
    tenant/index rows, and recovers a terminal audit whose tenant still equals
    the original operation after the final SQLite reopen.
11. GORM remains limited to the SQLite control plane. ClickHouse target
    resolution, outbox replay, mutation submission/status, physical-zero
    proof, and audit assertions remain native and unchanged in ownership.

Validation on implementation commit `31d20ac`:

```sh
go test ./... -count=1
go test ./internal/control ./internal/indexes -count=10
go test -race ./internal/control ./internal/indexes -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/indexes \
  -run '^TestIndexDataDeletionCoordinatorAgainstClickHouse$' -count=1 -v
git show --check --oneline 31d20ac
```

The exact implementation tree passed the full repository suite, ten repeated
control/coordinator runs, focused race detection, repository-wide vet/build,
and pinned repository-wide lint with `0 issues`. The approved ClickHouse
26.3.17.4 digest-pinned composed restart test passed on the final simplified
tree in 6.56 seconds.

Three identical-diff simplify passes found and fixed an avoidable tenant-string
clone, moved the newly shared tenant grammar out of the app implementation,
and collapsed duplicated raw-SQL tenant guard setup. Independent
control/state, integration/crash, test/lint, reuse, quality, and efficiency
adversaries found no remaining concrete defect. The exact staged feature diff,
including the new shared validator file, was clean at SHA-256
`545d7b4bcde3fce3be357c30ac1b495005132f91fb267830d86084bd7b957f2b`.

Remaining `DELETE_DATA` work:

1. wire exactly one coordinator beside the production ClickHouse Store after
   migrations/table-changing DDL and before request admission, then close the
   coordinator before the Store and control plane;
2. enforce the documented restricted runtime DDL principal and exclusive
   physical table-generation lifecycle;
3. enable authenticated `DELETE_DATA` admission only after those runtime
   ownership guarantees hold, pass the handler's trusted tenant scope rather
   than request data, and issue the best-effort postcommit wake; and
4. keep the periodic recovery scan as the correctness fallback for lost wakes.

Explicit pause point:

1. Admission-time tenant binding, cross-layer integrity, wrong-config restart
   rejection, and the composed pinned live proof are implemented, validated,
   committed, and pushed.
2. Runtime ownership and the authenticated `DELETE_DATA` route remain
   deliberately unwired.
3. The next correctness unit is production coordinator/Store lifecycle and
   restricted DDL ownership; do not enable the route before it is complete.
4. Pause here until the user gives further instructions.

## Previous checkpoint: serialized physical-deletion coordinator

Date: 2026-07-29

Committed and pushed implementation checkpoint:

- `77dd8f7` — oldest-first physical-deletion coordination, frozen outbox
  replay, restart-safe mutation recovery, terminal ambiguity resolution, and
  adversarial SQLite/ClickHouse coverage.

This test-first unit composes the existing GORM control-plane and native
ClickHouse deletion primitives without wiring the server runtime or enabling
the `DELETE_DATA` route:

1. `internal/indexes.IndexDataDeletionCoordinator` owns one immediate-start
   worker. It discovers and caches the oldest outstanding operation, never
   skips poisoned or pending head-of-line work, periodically rescans SQLite so
   correctness does not depend on a wake, and moves directly to the next
   operation only after terminal completion.
2. Existing mutation attempts are checked against the exact operation identity
   and configured deployment tenant before any native call.
   `IndexDataDeletionStatus` polls pending mutation state outside the global
   write freeze. Missing mutation history yields `Ready`, never completion, and
   therefore requires a new frozen advancement.
3. A new attempt follows one callback-scoped order:
   `WithWritesFrozen` -> `DrainPending` -> resolve the table generation ->
   atomically ensure the immutable GORM attempt -> construct the native request
   from that returned attempt -> `AdvanceIndexDataDeletion`. No `ALTER` is
   possible before the durable attempt exists.
4. Every existing-attempt advancement also reacquires the freeze and drains the
   outbox first. Pending advancement releases the freeze promptly. Only
   `PhysicallyEmpty` paired with a nil error can call
   `CompleteIndexDataDeletion`, and completion receives the exact frozen
   callback context and runs before that callback returns.
5. A precommit terminal error leaves the operation and attempt outstanding. An
   outcome-ambiguous commit is accepted only when a fresh, worker-owned,
   close-cancellable audit read returns the exact immutable completion. A
   missing or mismatched audit backs off and requires another freeze, drain,
   and zero proof; old physical-absence evidence is never cached.
6. A concurrent `EnsureIndexDeletionMutationAttempt` not-found result is not
   silently treated as success. The coordinator re-reads terminal audit state
   and clears the cached operation only when the logical operation, configured
   tenant, and exact database/table/UUID just resolved under the freeze all
   match.
7. Polling and error retries are bounded independently, error backoff doubles
   with an overflow-safe cap, and only idle recovery waits are interruptible.
   `Wake` is a capacity-one nonblocking hint: pending/error wakes stay
   coalesced rather than draining into a CPU spin. The worker resets backoff
   after successful progress.
8. `Close` is idempotent and retryable after a caller deadline. It rejects
   future wakes, cancels blocked control, status, freeze, drain, advance, and
   completion work through the owned context, and joins the sole worker.
   `OnError` is asynchronous, panic-contained, and limited to one live
   callback; a deliberately blocked callback may outlive `Close` and is
   documented accordingly.
9. The digest-pinned composed test runs three process lifetimes over one
   persistent `control.sqlite`. It first loses a successful `MarkSending`
   result and retains one ambiguous durable outbox reservation, restarts with
   an operation but no attempt, proves the coordinator's frozen drain replays
   that row before the first `ALTER`, reaches physical zero, injects a
   precommit completion failure, restarts again, and proves a fresh
   drain/zero-check before a commit-then-EOF converges through immutable audit.
10. The composed proof spans three target rows across three monthly partitions
    including the replayed late row, preserves a same-name foreign-tenant row
    and a same-tenant neighboring-index row, observes exactly one correlated
    completed ClickHouse mutation through the native reconciliation API,
    consumes the operation/attempt, creates the tombstone, retains the
    `DELETING` N+1 catalog row, reserves the name, and recovers the exact
    terminal audit after the final SQLite restart.
11. The control plane now exports its validated
    `IndexDataDeletionCompletionMatchesAttempt` comparator so coordinator and
    terminal GORM logic share one protocol-identity definition. ClickHouse
    persistence, mutation submission, and reconciliation remain native and do
    not use GORM.

Validation on implementation commit `77dd8f7`:

```sh
go test ./... -count=1
go test ./internal/indexes ./internal/control -count=10
go test -race ./internal/indexes ./internal/control -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/indexes \
  -run TestIndexDataDeletionCoordinatorAgainstClickHouse -count=1 -v
git show --check --oneline 77dd8f7
```

The exact implementation tree passed the full suite, repeated coordinator and
control-plane tests, race detection, repository-wide vet/build, and pinned
repository-wide lint with `0 issues`. The approved ClickHouse 26.3.17.4
digest-pinned three-process crash schedule passed in 6.16 seconds on the final
reviewed tree. Focused coordinator package coverage is 87.7% of statements.

Independent state-machine, crash, lifecycle, and integration adversaries drove
the durable-outbox/restart schedule, fresh terminal-audit context, concurrent
completion target binding, fixed pending/error wake behavior, and invalid-state
coverage. The simplify pass replaced coupled callback-result booleans with an
explicit outcome enum, centralized completion identity matching, reused native
mutation reconciliation in the integration assertion, documented timeout
semantics, and removed a wake-storm CPU spin. Final state/integration and
lifecycle/concurrency reviews are clean at staged diff SHA-256
`b558c22ce087c1988526e8570d0cb5dd694c128122f69fd23c1916f9c1cb7e98`.

Remaining `DELETE_DATA` work:

1. close the pre-attempt tenant-drift gap before activation. The outstanding
   operation currently has no tenant field, so a changed configured tenant
   after admission but before attempt creation cannot be inferred safely.
   Either snapshot tenant atomically in `BeginIndexDataDeletion` or enforce an
   immutable deployment-tenant binding at startup;
2. wire exactly one coordinator beside the single production ClickHouse Store,
   after migrations/table-changing DDL and before request admission, then close
   the coordinator before the Store/control plane;
3. enforce the documented restricted runtime DDL principal and exclusive
   physical table-generation lifecycle; and
4. after those invariants are in place, enable authenticated `DELETE_DATA`
   admission and issue the best-effort postcommit wake. The periodic recovery
   scan remains the correctness fallback.

Explicit pause point:

1. The coordinator, crash-boundary units, and composed digest-pinned
   SQLite/ClickHouse proof are implemented, validated, committed, and pushed.
2. Runtime ownership and the authenticated route remain deliberately unwired.
3. The pre-attempt tenant binding is the next correctness prerequisite; do not
   enable `DELETE_DATA` before it is durable.
4. Pause here until the user gives further instructions.

## Previous checkpoint: atomic GORM physical-deletion terminality

Date: 2026-07-29

Committed and pushed implementation checkpoint:

- `b497b73` — immutable physical-deletion completion audit, atomic terminal
  tombstoning/cleanup, exact retries, and hardened SQLite integrity guards.

This test-first unit completes the terminal control-plane prerequisite for
physical `DELETE_DATA` without starting the coordinator or enabling the route:

1. SQLite migration 0019 and the explicit GORM
   `indexDataDeletionCompletionRecord` add one immutable terminal audit row per
   physically deleted index. It permanently copies the operation ID,
   correlation ID, index ID/name, archived and deleting versions, tenant,
   ClickHouse database/table/UUID, protocol, operation/attempt timestamps, and
   completion time.
2. `CompleteIndexDataDeletion` accepts the full expected immutable mutation
   attempt. A single GORM completion insert invokes trigger-owned terminality:
   create the catalog tombstone, delete the exact outstanding operation, and
   cascade deletion of the mutation attempt. Failure at completion insertion,
   tombstone creation, operation cleanup, or attempt cleanup rolls the entire
   statement/transaction back. A commit-outcome ambiguity is resolved by
   reading or exactly retrying the immutable completion.
3. The retained raw index remains `DELETING` at its already-admitted `N+1`
   version. There is deliberately no `N+2` update: archived version
   `MaxInt64-1` can enter and finish at SQLite `MaxInt64`. The tombstone hides
   the row from live Get/List calls while preserving foreign keys and
   permanently reserving its canonical ClickHouse-facing name.
4. Exact sequential, concurrent, and restart retries return the same audit.
   The common completed retry is a read-only fast path and does not reserve
   SQLite's writer. A transaction recheck closes the read-miss race before
   insertion.
5. Completion insertion validates the exact immutable operation, attempt, and
   deleting-index relationship. Downstream triggers rely on that already
   validated immutable audit while separately guarding tombstone replacement,
   operation/attempt deletion, completion update/delete, and completed
   operation/correlation identity reuse. `INSERT OR REPLACE`, UPSERT-like
   identity replacement, forged copied fields, and direct physical
   tombstoning fail closed.
6. Completion preserves every opaque index ID accepted by the existing index
   schema, including IDs longer than 255 bytes and embedded NULs. The terminal
   schema does not introduce a narrower completion-only grammar that could
   strand an admitted index.
7. A completion racing a previously read operation or mutation-attempt row is
   monotonic, not corruption: stale validators converge to `ErrNotFound`, and
   oldest-first discovery retries past a just-completed stale row. Context,
   deadline, and operational SQLite errors retain their error identity; only
   sentinel-identified invalid relationships are classified as corruption.
8. Random operation/correlation collision probes now inspect both outstanding
   rows and immutable completions with one indexed `EXISTS` query. Completed
   protocol identities can never be recycled, while genuine unrelated insert
   failures keep their original errors.
9. GORM model/SQLite column order and key parity is tested explicitly, and the
   embedded migration count advances to 19. SQL migrations remain schema
   authority; ClickHouse persistence and mutation reconciliation remain native
   and do not use GORM.

Validation on implementation commit `b497b73`:

```sh
go test ./... -count=1
go test ./internal/control -count=10
go test -race ./internal/control -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
  -run TestStoreAgainstClickHouse/durable_physical_deletion_mutation_is_scoped_and_restartable \
  -count=1 -v
git show --check --oneline b497b73
```

The exact implementation tree passed the full suite, ten repeated
control-plane runs, the full control-plane race detector, repository-wide vet
and build, and pinned repository-wide lint with `0 issues`. The approved
ClickHouse 26.3.17.4 digest-pinned mutation integration also passed, preserving
the previously proven table-UUID binding, restart reconciliation,
multi-partition deletion, and foreign-tenant/index isolation. This unit changes
only GORM/SQLite terminality; the next coordinator checkpoint must add the
composed pinned test that invokes this terminal transaction inside the
callback-scoped frozen zero proof.

Independent SQLite, GORM/concurrency, and coverage adversaries drove fixes for
a completion-only opaque-index-ID restriction, untested cascade-attempt
failure, stale-read corruption misclassification, swallowed operational
errors, and writer-locking completed retries. The simplify pass additionally
collapsed redundant relationship reads, completion decoding, collision
lookups, and downstream trigger predicates. Final targeted re-reviews are
clean across trigger order, replacement bypass, cascade/FK rollback,
MaxInt64, KEEP_DATA compatibility, concurrency, restart, context, and schema
parity.

Remaining `DELETE_DATA` work:

1. add the serialized production coordinator in `internal/indexes`: discover
   the oldest operation, resolve/ensure its immutable attempt, poll native
   progress outside the freeze, and reacquire the frozen drain before
   advancement;
2. call `CompleteIndexDataDeletion` inside the exact drained
   `WithWritesFrozen` callback that returns physical emptiness, and require a
   fresh freeze/drain/zero proof after any nonterminal or ambiguous commit;
3. bind recovery to the configured deployment tenant and crash-test every
   attempt, mutation, zero-proof, terminal-commit, shutdown, and restart
   boundary with a digest-pinned composed ClickHouse test;
4. enforce the documented migration/runtime DDL-principal lifecycle; and
5. enable the authenticated `DELETE_DATA` handler only after those coordinator
   and runtime guarantees are proven.

Explicit pause point:

1. Terminal GORM/SQLite completion is implemented, validated, committed, and
   pushed.
2. No coordinator or production lifecycle wiring was started in this unit.
3. `DELETE_DATA` still returns its explicit disabled response.
4. Pause here until the user gives further instructions.

## Previous checkpoint: durable native ClickHouse deletion mutations

Date: 2026-07-29

Committed implementation checkpoint:

- `d98a741` — immutable GORM mutation attempts, native ClickHouse
  reconciliation, asynchronous scoped deletion, and frozen physical proof.

This test-first unit completes the outcome-ambiguous ClickHouse mutation
primitive without enabling the `DELETE_DATA` route or prematurely completing
the outstanding control-plane operation:

1. SQLite migration 0018 and the explicit GORM
   `indexDeletionMutationAttemptRecord` add exactly one immutable child row per
   outstanding deletion operation. It persists a stable correlation ID,
   configured deployment tenant, ClickHouse database/table and canonical
   nonzero table UUID, protocol version, and creation time before any native
   side effect. ClickHouse state is not persisted through GORM.
2. `EnsureIndexDeletionMutationAttempt` is exactly idempotent sequentially,
   concurrently, and across database restart. It validates the deleting parent
   snapshot, reuses the shared bounded tenant and index-name grammars, and
   returns `ErrDependencyConflict` on any durable-target drift.
3. SQL constraints and triggers reject malformed byte lengths, embedded NULs,
   unsupported protocol versions, nil/noncanonical UUIDs, invalid parents,
   direct update/delete, and `INSERT OR REPLACE` identity collisions. The
   child delete guard permits only a future parent-owned cascade after the
   parent has been removed.
4. The frozen native Store resolves the configured `MergeTree` generation
   through `system.tables` only after the durable outbox is drained.
   `AdvanceIndexDataDeletion` validates the exact durable request, reconciles
   correlated `system.mutations` rows, and submits at most one asynchronous
   heavyweight `ALTER DELETE` with `mutations_sync=0`.
5. The mutation marker and query ID share a domain-separated SHA-256 digest of
   length-prefixed operation, correlation, tenant, index, database, table,
   table UUID, and protocol fields. Correlated command shape is checked, and an
   observed newer mutation block resolves an outcome-ambiguous `Exec` result.
6. Pending mutations return immediately so the global write freeze is
   released while ClickHouse rewrites parts. `IndexDataDeletionStatus` polls
   read-only progress outside the freeze. When no mutation is pending, the
   coordinator must reacquire the freeze and drain before advancing.
7. Missing or pruned mutation history never proves completion. The terminal
   physical candidate is one bounded, key-aligned query that jointly reads the
   current table UUID/engine and tests for a row under the exact
   `(tenant_id, index_name)` key. Zero is accepted only while Store writers and
   outbox replay remain frozen.
8. The Store contract now states the necessary DDL ownership boundary:
   migrations run before Store construction, and the configured table name
   remains bound to that generation until shutdown. UUID checks fail closed on
   observable drift; privileged out-of-band rename/drop/exchange/replace DDL
   is forbidden because ClickHouse `ALTER TABLE` is name-targeted.
9. The current global index catalog belongs to one configured deployment
   tenant. Physical deletion is tenant/index scoped and intentionally preserves
   foreign-tenant rows. A future multi-tenant catalog must first tenant-scope
   indexes, tombstones, and deletion operations.

Validation on implementation commit `d98a741`:

```sh
go test ./... -count=1
go test ./internal/control ./internal/clickhouse ./internal/indexname -count=10
go test -race ./internal/control ./internal/clickhouse ./internal/indexname -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
  -run TestStoreAgainstClickHouse/durable_physical_deletion_mutation_is_scoped_and_restartable \
  -count=1 -v
git show --check --oneline d98a741
```

The exact tree passed the full suite, ten repeated control/native/name-grammar
runs, focused race detector, vet, build, pinned repository-wide lint with
`0 issues`, and the digest-pinned ClickHouse 26.3.17.4 integration. The live
test proves table-UUID binding, restart persistence, durable marker recovery,
multi-partition deletion, foreign-tenant/index preservation, and that the
operation remains outstanding until a later terminal transaction.

Independent SQLite, ClickHouse, and crash-schedule adversaries drove fixes for
an embedded-NUL UUID CHECK bypass, request-scope marker aliasing, separate
UUID/zero-proof round trips, and an undocumented table-swap assumption. The
final re-reviews are clean under the explicit single configured tenant, single
Store writer/mutation owner, and exclusive table-DDL contract. The official
and pinned `system.mutations.parts_to_do` type is `Int64`, matching the native
scan.

Remaining `DELETE_DATA` work:

1. add the production coordinator that serially discovers the oldest
   operation, resolves/ensures its immutable attempt, polls native progress,
   and reacquires the frozen drain only when advancement is ready;
2. add one terminal GORM/SQLite transaction that consumes the outstanding
   operation and creates the immutable tombstone/audit state only after a
   callback-scoped frozen zero proof;
3. crash-test every boundary: attempt-before-ALTER, ambiguous submission,
   pending restart, completed-history loss, zero-proof-before-terminal commit,
   terminal commit ambiguity, shutdown, and retry;
4. enforce the documented DDL/principal lifecycle in production startup and
   deployment configuration; and
5. enable the authenticated `DELETE_DATA` handler only after the coordinator
   and terminal transaction are proven.

Explicit pause point:

1. Durable mutation identity and native ClickHouse reconciliation are complete,
   committed, and pushed.
2. The operation intentionally remains outstanding after physical zero proof;
   no terminal tombstone is written by this unit.
3. The route still returns its explicit disabled response.
4. Do not begin the runtime coordinator until the user gives further
   instructions.

## Previous checkpoint: durable GORM physical-deletion admission

Date: 2026-07-29

Committed implementation checkpoint:

- `dca543a` — atomic GORM deletion-operation admission, restart discovery,
  coordinator-owned `DELETING`, and hardened SQLite integrity guards.

This test-first unit completes the durable control-plane prerequisite for
physical `DELETE_DATA` without exposing the route or submitting a ClickHouse
mutation:

1. Migration 0017 and the explicit GORM `indexDeletionOperationRecord` model
   add one immutable outstanding-work row per index. It snapshots the stable
   operation ID, index ID, canonical name, archived version, and creation
   timestamp. No ClickHouse persistence uses GORM.
2. `BeginIndexDataDeletion` requires the exact archived version and canonical
   confirmation. The operation insert trigger owns the archived
   generation `N` to coordinator-owned `DELETING` generation `N+1`
   transition, so an operation and its state change cannot commit
   independently. An injected transition failure rolls both back.
3. Exact sequential, concurrent, and post-restart retries return the original
   operation without another version bump. A read-only fast path avoids
   entropy and write-lock acquisition for the common exact retry, while the
   immediate write transaction rechecks before creation.
4. `GetIndexDeletionOperation` provides stable lookup and
   `NextIndexDeletionOperation` returns exactly the oldest outstanding row by
   the indexed `(created_at_unix_micro, deletion_operation_id)` order.
   Discovery is therefore deterministic and bounded to one row. The
   supported host singleton permits the later coordinator to remain
   serialized; speculative durable leases and ClickHouse-specific phases are
   intentionally absent.
5. Generic `UpdateIndex` and `SetIndexState` now reject an already-deleting
   index. SQLite guards also forbid creating or entering `DELETING` without
   an operation, mutating/deleting a coordinator-owned index, and
   mutating/deleting the immutable operation.
6. The operation ID uses the shared bounded ASCII protocol-ID grammar in both
   Go and SQLite, including byte-length and embedded-NUL checks. Version and
   timestamp ceilings are aligned. A pre-conflict identity trigger defeats
   SQLite `INSERT OR REPLACE`/UPSERT conflict policies that could otherwise
   delete one operation without firing its DELETE trigger and orphan its
   deleting index.
7. Adversarial coverage proves atomic rollback, penultimate-to-final SQLite
   version admission, concurrent physical-vs-`KEEP_DATA` single-winner
   behavior, identity replacement resistance, direct-SQL corruption guards,
   second-query cancellation identity, restart persistence, and deterministic
   discovery.

Validation on implementation commit `dca543a`:

```sh
go test ./... -count=1
go test ./internal/control -count=10
go test -race ./internal/control -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
git show --check --oneline dca543a
```

The exact implementation tree passed the full suite, ten repeated
control-plane runs, the control race detector, vet, build, and the
repository-wide pinned lint ratchet with `0 issues`.

Two design agents first converged on an immutable outstanding intent rather
than speculative leases or mutation phases. The frozen-diff reviews then
found and drove fixes for retry entropy/write-lock waste, duplicated GORM
lookup logic, character-vs-byte identifier drift, lost relationship-query
errors, and unnecessary terminal-history scanning. The transaction adversary
reproduced the SQLite `INSERT OR REPLACE` trigger bypass against two indexes;
the pre-conflict identity guard and regression close it. Final correctness,
atomicity, and simplify passes report no remaining finding.

Remaining `DELETE_DATA` work:

1. define and persist the concrete ClickHouse mutation-attempt/correlation
   protocol, including outcome-ambiguous submission and
   `system.mutations` reconciliation;
2. compose oldest-operation recovery with `Store.WithWritesFrozen`, the
   proven bounded outbox drain, mutation completion, and physical zero-row
   verification;
3. atomically replace the outstanding operation with a terminal audit marker
   only after zero rows are proven, then crash-test every coordinator
   boundary and shutdown ordering; and
4. enable the authenticated `DELETE_DATA` route only after the complete
   coordinator is safe. The route still returns an explicit 400 today.

Explicit pause point:

1. Durable GORM admission and restart discovery are complete and committed.
2. ClickHouse mutation execution remains native and is unchanged in this
   unit.
3. Do not begin the mutation/reconciliation coordinator until the user gives
   further instructions.

## Previous checkpoint: scoped ClickHouse write freeze and proven outbox drain

Date: 2026-07-29

Committed implementation checkpoint:

- `8676b4d` — writer-preferring Store freeze, bounded durable-outbox drain,
  explicit SQLite pending-usage proof, lifecycle fencing, and real ClickHouse
  ambiguity/deduplication coverage.

This test-first unit completes the first two physical `DELETE_DATA`
prerequisites without exposing the route prematurely:

1. `Store.WithWritesFrozen` owns a fair FIFO exclusive scope. A queued freeze
   prevents later `Store`, `ResumeBatch`, and manual/background reconciliation
   calls from bypassing it, while already-admitted writers may finish. Waiting
   writers and freezes remain context-cancelable.
2. The callback-scoped `FrozenWrites` capability is the only replay path while
   exclusivity is held. It serializes concurrent drains context-sensitively,
   expires before the gate is released, rejects escaped use, and rejects
   ordinary/nested writer reentry when callers propagate the supplied
   callback context.
3. The drain replays no more than the visibility admission ceiling of 64
   reservations / 256 MiB. A defensive 65th acquisition is released and fails
   closed. Terminal-ledger pruning is deliberately deferred until after the
   global freeze is released.
4. `visibility.PendingUsage` counts all durable `reserved` rows and outbox
   bytes, including reservations hidden from `AcquirePending` by a live
   in-process attempt lease. It validates aggregate and per-row bounds and
   makes zero reservations plus zero bytes the explicit success proof.
   Therefore `AcquirePending(found=false)` alone is never treated as drained.
5. Store lifecycle admission now covers ordinary writes, replay, lookup,
   visibility cutoff, and ping. `Close` synchronously rejects queued gate
   entrants, cancels admitted operation contexts, waits for visibility
   finalization/capability invalidation, and only then closes the native
   ClickHouse connection.
6. The production runtime's single Store owner is part of the safety
   contract. Every writer for the physical events table must use that Store
   during a frozen callback; the primitive is global across that owner, not an
   index-specific lock. This is required because one opaque durable outbox may
   contain several indexes.
7. The pinned ClickHouse integration forces a two-index batch to commit
   physically and then report an ambiguous EOF. The frozen drain replays the
   exact persisted block/token, advances the visibility cutoff, proves zero
   pending usage, and leaves exactly one physical row per event through
   ClickHouse deduplication.

Validation on implementation commit `8676b4d`:

```sh
go test ./...
go test -race ./internal/clickhouse ./internal/visibility \
  ./internal/control ./internal/server
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' \
    -count=1 -timeout=10m
git diff --check
```

The full suite, cross-package race gate, vet, build, and pinned
golangci-lint v2.12.2 passed; lint reported `0 issues`. The digest-pinned
ClickHouse integration passed in 60.051 seconds.

Three design/adversarial agents first audited every writer, replay,
visibility, and shutdown edge. Their key finding was that a live attempt lease
can make `AcquirePending` return no row while durable work still exists.
Three frozen-diff simplify reviewers then found and drove fixes for zero-sized
waiter identity, broadcast wake storms, cancellation-blind drain
serialization, O(n) waiter removal, pruning under exclusivity, duplicated
lifecycle/query logic, and callback reentrancy. Final adversarial review found
the nested different-Store context-shadowing case; per-Store context keys and
a regression test close it. No unresolved blocker remains in this unit.

Remaining `DELETE_DATA` work at that checkpoint:

1. atomically set `DELETING` and create a restartable GORM deletion-operation
   record;
2. submit and reconcile heavyweight ClickHouse mutations, including
   outcome-ambiguous responses and restart;
3. verify zero physical rows before terminal tombstoning; and
4. compose and crash-test the coordinator while keeping GORM confined to the
   SQLite control plane.

Explicit pause point:

1. The scoped Store fence and proven bounded outbox drain are complete and
   pushed as one cohesive prerequisite.
2. `DELETE_DATA` remains intentionally unavailable.
3. Do not begin the durable deletion-operation/mutation unit until the user
   gives further instructions.

## Previous checkpoint: terminal KEEP_DATA index deletion

Date: 2026-07-29

Committed and pushed checkpoint:

- `66f36d1` — archived-index deletion through a durable GORM tombstone,
  authenticated `/indexes/delete`, and retained-row ClickHouse proof.

This test-first unit closes the unsafe permanent-intermediate behavior for
logical index deletion without pretending that physical deletion is ready:

1. `KEEP_DATA` requires an archived index, its current optimistic version, and
   an exact canonical-name confirmation. It completes synchronously and
   returns the stable index ID without a deletion-operation ID.
2. Migration 0016 adds the explicit GORM-modeled
   `index_deletion_tombstones` table. The archived `indexes` row remains in
   place so ingestion-token/app foreign keys and the immutable name
   reservation survive, while Get-by-ID, Get-by-name, List, Update, state
   mutation, and repeat deletion all treat the tombstoned object as absent.
3. SQLite triggers require every tombstone to match the exact archived
   index/version/name, then make both the retained row and tombstone
   irreversible through direct SQL. The migration recovers any legacy
   `deleting` row to `archived`, bumps its version and timestamp when the
   SQLite integer ceiling permits, and remains valid at that ceiling.
4. Creating the same name after deletion returns `ErrAlreadyExists`. This is
   mandatory because ClickHouse rows and compiled search scope currently use
   `tenant_id + index_name`; freeing the name would make retained events
   visible through a new logical index.
5. `/api/v1/indexes/delete` is an exact administrator-only protobuf route with
   selector resolution, optimistic/state checks, sanitized 400/404/409/408/503
   mapping, and a second transactional confirmation check. The shared mapper
   now correctly reports dependency conflicts as 409.
6. Generic state administration rejects `DELETING` in both the HTTP adapter
   and control store. Only a future physical-deletion coordinator may own
   that intermediate state and it must create a durable operation atomically.
7. `DELETE_DATA` returns a clear 400 without changing control or storage
   state. The adversarial audit proved that a state CAS plus ClickHouse
   mutation is unsafe: durable outbox replay intentionally bypasses mutable
   authorization and could insert a late row after the mutation.
8. Deterministic coverage pins state/version/confirmation validation, hidden
   reads, permanent name reservation, preserved token references, concurrent
   single-winner deletion, direct-SQL guards, migration upgrade/ceiling
   behavior, authenticated routing, response invariants, mode rejection, and
   physical ClickHouse row retention.

Validation on the exact implementation tree committed as `66f36d1`:

```sh
go test ./... -count=1
go test -race ./internal/control ./internal/server ./internal/clickhouse \
  -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' \
    -count=1 -timeout=8m -v
git diff --check
```

The repository-wide suite, focused race suite, vet, build, and CI-pinned
golangci-lint v2.12.2 all passed; lint reported `0 issues`. The digest-pinned
ClickHouse Store integration passed in 60.25 seconds, including the new
terminal `KEEP_DATA` subtest. Its first placement correctly exposed an
existing exact visibility-cutoff dependency; moving the subtest to the end of
the reusable container run preserves isolation and the complete existing
corpus.

Three frozen simplify reviewers examined the same staged diff for reuse,
quality/correctness, and efficiency. Their concrete findings removed a
redundant permanent SQLite name index and corrected the live-catalog comment;
the quality reviewer found no behavioral or transaction-safety defect. The
final local adversarial pass also moved malformed confirmation/ID rejection
before the immediate write lock and made legacy-state recovery overflow-safe.
GORM remains confined to SQLite control-plane records and transactions.

Required future `DELETE_DATA` work is explicit rather than hidden behind an
unsafe partial route:

1. a context-aware Store write fence covering Store, ResumeBatch, and
   background reconciliation;
2. bounded durable-outbox drain while the fence is held;
3. an atomically created, restartable GORM deletion-operation record;
4. serialized heavyweight ClickHouse mutation submission and
   `system.mutations` reconciliation for ambiguous outcomes; and
5. post-mutation zero-row verification before terminal tombstoning.

Explicit pause point:

1. Synchronous terminal `KEEP_DATA` deletion is complete for this unit.
2. `DELETE_DATA` remains intentionally unavailable until all safety
   prerequisites above are implemented and tested together.
3. Do not begin another implementation slice until the user gives further
   instructions.
4. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.

## Previous checkpoint: production-owned bounded search-history retention

Date: 2026-07-29

Committed and pushed checkpoint:

- `ca48ca2` — configurable, database-wide physical search-history retention
  with bounded GORM transactions and production lifecycle ownership.

This test-first slice closes the idle search-history physical-retention
backlog:

1. Operators can configure terminal-history age and per-owner capacity with
   `-search-history-maximum-age` and
   `-search-history-maximum-entries-per-owner`. Zero-valued programmatic
   options resolve centrally to 30 days and 10,000 entries; hard bounds are
   ten years and one million entries. The terminal table and durable pending
   journal are capped independently at the configured entry count, so one
   owner may physically have up to twice that count while every admitted
   attempt is still awaiting recovery.
2. Startup first recovers the current authenticated owner's pending attempts,
   then performs at most four 256-row database-wide terminal-retention
   batches. Readiness is therefore not coupled to an inherited backlog.
   Remaining work transfers through an opaque cursor to one non-overlapping
   background worker.
3. The worker performs at most 64 fixed-size batches under one ten-second
   operation deadline. If work remains, it schedules another bounded run
   after a 100-millisecond duty-cycle gap instead of waiting for the hourly
   maintenance tick. Shutdown cancels and drains the worker idempotently.
   Error callbacks are asynchronous, single-flight, panic-contained, and
   cannot stall retention or shutdown.
4. Age retention uses the new
   `(created_at_unix_micro, search_job_id)` global index and one bounded
   transaction per batch. It deletes terminal rows across every persisted
   tenant/owner scope only when `created_at < now - maximum_age`; the exact
   cutoff remains visible. Pending attempts are never age-pruned because they
   must first become terminal crash-audit records.
5. `search_history_owner_counts` is a GORM-modeled, trigger-maintained SQLite
   control-plane table. Migration 0015 backfills existing owners, and
   insert/delete triggers update counts in the same transaction as terminal
   history mutations, including filtered clears. Count retention therefore
   performs a primary-key counter read and deletes the oldest excess rows
   directly; it never repeatedly counts a million-row owner or walks
   `OFFSET maximumEntriesPerOwner` while holding SQLite's write lock.
6. An ordered in-memory cursor resumes one over-capacity owner and then scans
   later owner keys without revisiting completed scopes. Every cursor batch
   reserves bounded work for global age deletion, so a large count-policy
   reduction cannot starve rows that expire while the count backlog drains.
   Counter reads are repeated transactionally between batches, preventing
   concurrent completion or clear operations from causing over-pruning.
7. Ordinary terminal completion, interrupted-attempt recovery, explicit
   owner pruning, startup pruning, and periodic pruning all use bounded
   delete statements. Explicit owner pruning commits each 256-row batch
   independently; a later injected failure preserves earlier committed
   progress and a retry finishes the remainder.
8. Deterministic coverage pins default and invalid configuration, option
   propagation, independent terminal/pending capacity, exact age boundaries,
   prior tenant/owner cleanup, pending preservation, count-policy shrink,
   ordered cursor continuation, age/count interleaving, counter backfill and
   trigger consistency, query-plan index selection, per-transaction work
   bounds, partial-commit retry, bounded startup readiness, prompt backlog
   continuation, non-overlap, retry, timeout cancellation, repeated close,
   blocked callbacks, and callback panic recovery.

Physical deletion makes SQLite pages reusable; this checkpoint does not claim
that the database file shrinks immediately and deliberately does not run
`VACUUM` in the server lifecycle.

Validation on the exact implementation tree committed as `ca48ca2`:

```sh
go test ./internal/control ./internal/searchhistory \
  ./cmd/open-splunk-server -count=20
go test -race ./internal/control ./internal/searchhistory \
  ./cmd/open-splunk-server -count=10
go test ./... -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec \
    -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
    -count=1 -timeout=6m -v
git diff --check
```

The repeated focused suite, repeated race-enabled suite, repository-wide Go
suite, vet, build, and CI-pinned golangci-lint v2.12.2 all passed; lint
reported `0 issues`. The digest-pinned ClickHouse GradeThis v0.1 corpus passed
all ten investigations in 6.076 seconds.

Frozen adversarial reviews first found and drove database-wide cleanup,
fixed-size transactions, callback containment, flag-contract clarification,
global age indexing, count continuation, bounded startup readiness, and reuse
of shared policy/batch drivers. A later three-lens review exposed age
starvation, repeated whole-owner counts, steady-state `OFFSET` cost, and
hourly backlog delay; the transactional owner counter, interleaving, and
prompt duty-cycled continuation close those findings. Three final reviewers
reported no remaining correctness, SQLite/GORM efficiency, runtime-lifecycle,
or test-quality finding in the exact `ca48ca2` commit, whose code diff is
SHA-256 `b513dc2694d735621c9849f41ad9b57c0519a86649a2ed246f8b31bba5ddf584`.
The final compatibility adjustment removed only an optional scope-update
trigger that interfered with the existing persisted-corruption classifier;
insert/delete counter ownership is unchanged.

The simplify pass directly produced the shared batch runner, centralized
retention-policy resolution, indexed global age deletion, ordered count
cursor, transactional owner counter, bounded startup pass, prompt backlog
continuation, and corrected ownership comments. GORM remains confined to the
SQLite control plane. ClickHouse storage and SPL execution retain native
bounded SQL and have no GORM dependency. The user's dependency upgrades remain
separately committed as `347a015`.

Explicit pause point:

1. Production-owned physical search-history retention is complete for this
   unit.
2. Do not begin another implementation slice until the user gives further
   instructions.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, digest-pinned ClickHouse acceptance,
   frozen adversarial review, and commit/push after each cohesive green unit.

## Previous checkpoint: explicit SQLite visibility ownership

Date: 2026-07-29

Committed and pushed checkpoint:

- `ab0514e` — physical-file visibility ownership, bounded autonomous
  shutdown, durable attempt-lease recovery, and explicit fixture lifecycle.

This test-first slice closes the process-global visibility lease-registry
lifecycle finding:

1. Exactly one live `SQLiteSequencer` may own a physical control database file
   in a process. `control.DB` retains immutable `os.FileInfo` identity and
   compares handles with `os.SameFile`, so opening the same path through two
   separate `*sql.DB` pools cannot bypass ownership. The production
   process-wide server lock continues to fence separate processes.
2. Ownership is claimed before stale-lease recovery. A duplicate constructor
   returns `ErrOwnerExists` without mutating durable attempt IDs; constructor
   failures unregister their claim. Successful or terminal shutdown removes
   the registry entry and releases borrowed database and lease references.
3. Every public sequencer operation joins one mutex/`sync.WaitGroup`
   admission barrier. Shutdown closes admission before waiting, so an
   operation admitted earlier may finish but no later operation can race
   final lease cleanup.
4. One autonomous finalizer owns drain, a separately bounded ten-second
   durable cleanup attempt, ownership unregister, and terminal publication.
   Each `Shutdown(ctx)` caller independently observes its own cancellation or
   the shared terminal result; a short caller deadline neither relinquishes
   ownership nor requires a second shutdown call to start finalization.
5. Attempt acquisition marks durable-risk state before its SQLite transaction.
   This deliberately treats a failed `Commit` as outcome-ambiguous: even if
   the live process lease is dropped, shutdown still clears any attempt ID
   that may have persisted. Owners with no acquisition attempt avoid an
   unnecessary shutdown transaction.
6. Runtime ownership remains explicit and borrowed. LIFO shutdown closes
   search/ingest consumers and the ClickHouse Store before the sequencer, then
   closes the control database. If the sequencer exhausts the process shutdown
   budget, `Shutdown` returns while its finalizer retains ownership until
   admitted work and bounded cleanup complete; the subsequent database close
   follows `database/sql` semantics and may overlap that completion.
7. Every unit and ClickHouse integration fixture that constructs a visibility
   sequencer now closes it before its control database. Resource-owning
   Store/reconciler fixtures close those active consumers first. The restart
   fixture performs the full ordering explicitly before reopening.
8. Deterministic coverage includes same-handle and separate-handle duplicate
   constructors, live-attempt non-stealing, constructor failure, zero/nil and
   repeated close, all post-close operations, an admitted real SQLite write
   blocked past a caller deadline, independently blocked durable cleanup,
   autonomous post-timeout unregister, outcome-ambiguous pre-bind cleanup,
   clean reopen, and crash-style restart recovery.

Validation on the exact implementation tree committed as `ab0514e`:

```sh
go test ./... -count=1 -timeout=20m
go test ./internal/control ./internal/visibility -count=20 -timeout=8m
go test -race \
  ./internal/control ./internal/visibility ./internal/clickhouse \
  ./cmd/open-splunk-server \
  -count=3 -timeout=10m
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-lint-visibility-owner-final-2 \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=8m
git diff --check
```

The repository-wide Go suite, repeated control/visibility suite, repeated
race-enabled lifecycle/storage/runtime suite, vet, build, and CI-pinned
golangci-lint v2.12.2 all passed; lint reported `0 issues`. The
digest-pinned ClickHouse Store integration passed in 64.369 seconds.

Three independent final reviewers verified the unchanged 796-insertion and
82-deletion implementation diff at SHA-256
`9f9b1cbe2f6034dbd551e2d9aecd106e8b7d85202a3750a59481695d0c5b60ee`.
They reviewed physical-file identity, registry and operation linearizability,
outcome-ambiguous commits, autonomous finalization, database-close races,
durable recovery, SQL/resource cost, fixture cleanup, reuse, and test quality;
no P0/P1/P2/P3 finding remained. Earlier frozen reviews found and drove the
same-file-handle fence, conservative pre-commit durable-risk marking,
autonomous post-timeout finalization, and complete test-fixture ownership.

GORM remains confined to persistent SQLite control-plane stores. Physical-file
identity is in-memory control-handle metadata, while visibility fencing
continues to use its narrow transactional SQLite SQL. ClickHouse storage and
SPL execution remain native bounded SQL and have no GORM dependency. The
user's dependency upgrades remain separately committed as `347a015`.

Explicit pause point:

1. SQLite visibility ownership and bounded shutdown are complete for this
   unit.
2. Do not begin another implementation slice until the user gives further
   instructions.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, digest-pinned ClickHouse acceptance when
   storage behavior changes, frozen adversarial review, and commit/push after
   each cohesive green unit.

## Previous checkpoint: bounded WebSocket snapshot shutdown

Date: 2026-07-29

Committed and pushed checkpoint:

- `67689e8` — cancellation-aware search/export snapshots, one bounded
  projection deadline, lock-free artifact collision inspection, and
  end-to-end WebSocket shutdown coverage.

This slice closes the WebSocket dependency/shutdown backlog finding:

1. The WebSocket service now depends on explicit context-aware search metadata
   and preview methods. Search-manager implementations join the manager
   operation barrier, honor caller and manager cancellation while waiting for
   bounded read capacity, and release every permit on error or shutdown.
2. `ProjectionTimeout` is hard-validated between 50 milliseconds and 10
   minutes, with a conservative 10-second default. One derived context covers
   projection-gate admission, preview lookup, metadata fallback, export
   lookup, and local materialization; a shorter caller deadline still wins.
3. Cancellation and deadline errors retain their identity after preview row
   materialization and initial target loading. They are no longer converted
   into target disappearance, so an expired projection cannot retire the
   wrong target.
4. Export progress polling uses a dedicated `Snapshot` operation. Due
   expiration is still published atomically, including artifact invalidation,
   while filesystem unlink is deferred to the cleanup lifecycle. A stalled
   artifact removal therefore cannot hold the WebSocket ownership barrier.
5. Export admission no longer performs `Lstat` while holding the manager
   mutex. It performs a bounded state precheck, inspects the private artifact
   session outside the lock, then atomically revalidates capacity and ID state
   while reserving. No-replace publication remains the final collision
   defense.
6. A stalled collision inspection can now outlive `export.Manager.Close`
   without holding manager state or artifact teardown. When inspection
   returns, it observes the closed manager and returns `ErrClosed`; completed
   shutdown never waits on that filesystem call.
7. WebSocket close cancels blocked snapshot providers, waits for their actual
   exit, shares one completion barrier across repeated callers, hard-closes
   hijacked sockets, and clears target/load/replay/queue accounting before
   runtime teardown proceeds.
8. Deterministic coverage includes read-capacity cancellation, admitted
   entry-lock ordering, projection-gate timeout, shared preview/fallback
   deadlines, search/export provider cancellation, mid-materialization
   cancellation identity, deferred artifact deletion, stalled collision
   inspection, repeated service close, and an actual upgraded socket blocked
   inside its provider.
9. The simplify pass consolidated bounded synchronous test readers behind one
   adapter while retaining direct context-aware implementations for every
   blocking fake. Compile-time assertions prevent those fakes from
   accidentally bypassing cancellation. A diagnostic full-suite run also
   exposed and removed a test-only lock observation that could invert the
   deliberate entry-lock barrier under queued-writer pressure.

Validation on the exact staged tree committed as `67689e8`:

```sh
go test ./... -count=1 -timeout=20m
go test -race \
  ./internal/searchjobs ./internal/export ./internal/searchws \
  ./internal/server ./cmd/open-splunk-server \
  -count=1 -timeout=20m
go test ./internal/searchjobs -count=20 -timeout=3m
go test ./internal/searchjobs \
  -run '^TestContextSnapshotReadHonorsCancellationAfterReadAdmission$' \
  -count=100 -timeout=2m
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-lint-ws-final-2 \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
git diff --check
```

The repository-wide Go suite, touched-package race suite, repeated
search-manager package and lock-order regression runs, vet, build, and
CI-pinned golangci-lint v2.12.2 all passed; lint reported `0 issues`.

No digest-pinned ClickHouse container run was warranted for this checkpoint:
the slice changes Go lifecycle, snapshot, and transport behavior only, with no
ClickHouse SQL, schema, storage, or SPL semantic change.

Three independent final reviewers verified the unchanged 1,606-insertion and
198-deletion staged diff at SHA-256
`79e372614f181273d42478fb21eba0897cb4fb57eb4ae27c72011e990f712ca4`.
They reviewed lock/I/O ordering, cancellation and deadline identity, scoped
non-disclosure, export expiration/download semantics, runtime ownership,
resource accounting, reuse, and test-boundary quality; no P0/P1/P2 finding or
remaining concrete simplify issue remains. Earlier frozen reviews found and
prompted the export lock-held `Lstat` repair, materialization-error identity
repair, synchronous-fake consolidation, and cancellation-fake hardening.

GORM remains confined to the SQLite control plane. ClickHouse storage and SPL
execution continue to use native bounded SQL; this slice introduces no GORM
dependency there. The user's dependency upgrades remain committed separately
as `347a015`, and this slice leaves `go.mod` and `go.sum` unchanged.

Explicit pause point:

1. The bounded WebSocket snapshot dependency and shutdown unit is complete.
2. Do not begin another implementation slice until the user gives further
   instructions.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, digest-pinned ClickHouse acceptance when
   SQL behavior changes, frozen adversarial review, and commit/push after each
   cohesive green unit.

## Previous checkpoint: observable export cleanup and capacity reclamation

Date: 2026-07-29

Committed and pushed checkpoint:

- `9d00f98` — safe due-capacity reclamation, coalesced cleanup, bounded
  deletion-failure observability, and adversarial lifecycle coverage.

This slice closes the retained export resource/lifecycle finding:

1. Export admission that reaches `ErrCapacity` now attempts due cleanup once
   and retries the reservation once. Queue saturation remains distinct and
   does not trigger an O(n) cleanup scan.
2. A single cleanup gate coalesces concurrent admission and periodic work.
   Waiters honor request cancellation and manager shutdown, while `Close`
   waits for admitted filesystem work before tearing down private artifact
   storage.
3. Cleanup recovers all three bounded resources: retained artifact bytes,
   expired job slots, and job/grant metadata. Active downloads continue to
   pin their artifact until the final lease closes.
4. Admission-cleanup timing separates advisory scheduling from the strict
   retry floor after an unlink failure. The floor is anchored to filesystem
   completion, so a slow failing unlink cannot erase its own backoff.
5. A lifecycle generation validates the cleanup snapshot. New earlier grant
   expirations, terminal worker/lease release, external expiration, final
   expired-download unpin, and external unlink completion invalidate stale
   advisory timing. Final path clearing and byte-accounting release publish
   atomically under the job lock.
6. Known artifact, tombstone, and grant deadlines can trigger reclamation
   earlier than the generic admission throttle. Partial or canceled cleanups
   do not publish a complete snapshot or suppress the next eligible attempt.
7. Failed unlink state retains the private path and charged bytes for retry;
   callers still receive only stable `ErrCapacity`. `LastCleanupError`
   preserves trusted operational detail, while `OnCleanupError` provides a
   bounded asynchronous background notification with panic containment and
   at most one callback in flight.
8. The server logs one fixed, path-free cleanup message. Raw filesystem
   errors never reach the API or logger, and cleanup error aggregation retains
   at most 16 samples plus one omitted-count summary.
9. Unit and race coverage includes byte/job/metadata reclamation, earlier
   deadlines, expired grants, active and closing download pins, unlink
   failures, completion-anchored retry, stale-snapshot invalidation,
   cancellation, cleanup coalescing, shutdown waits, callback blocking and
   panic recovery, mixed cancellation/failure classification, bounded error
   aggregation, and actual logger path secrecy.

Validation on the exact staged tree committed as `9d00f98`:

```sh
go test ./... -count=1 -timeout=20m
go test -race ./internal/export ./cmd/open-splunk-server -count=1
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache-export-cleanup-exact-20260729 \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
make proto
npm run lint
npm run typecheck
npm run test:frontend
go mod tidy
git diff --check
```

The final repository-wide Go suite, targeted export/server race suite, vet,
build, and CI-pinned golangci-lint v2.12.2 all passed; lint reported
`0 issues`. Protobuf generation, frontend lint/type checking, all 183
frontend/release tests, and module normalization also passed during this
slice and remained unaffected by the final Go-only lifecycle repair.
`go mod tidy` produced no module-file diff.

No digest-pinned ClickHouse container run was warranted for this checkpoint:
the slice changes only the Go export manager and server-side cleanup
reporting, with no ClickHouse SQL, schema, storage, or SPL behavior change.

Three independent final reviewers verified the unchanged 1,192-insertion and
71-deletion staged diff at SHA-256
`da27b9b5b5a307fe7841370ff30cca112616a8948496b182d48dc7b57fe2ff7c`.
They reviewed accounting/deadlines, concurrency/resource lifecycle, and
observability/path secrecy; no P0/P1/P2 finding remains. Earlier frozen
reviews found and prompted completion-anchored unlink backoff, preservation of
that floor across stale deadlines, lifecycle cache invalidation, and atomic
unlink/accounting publication. The `simplify` review also drove recursive
mixed-error classification, capped flat aggregation, actual logger testing,
panic-safe callback reuse, and minimal lifecycle invalidations.

GORM remains confined to the SQLite control plane. ClickHouse storage and SPL
execution continue to use native bounded SQL; this slice introduces no GORM
dependency there. The user's dependency upgrades remain committed separately
as `347a015`, and this slice leaves `go.mod` and `go.sum` unchanged.

Explicit pause point:

1. Export cleanup observability and due-capacity reclamation are complete for
   this unit.
2. Do not begin another implementation slice until the user gives further
   instructions.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, digest-pinned ClickHouse acceptance when
   SQL behavior changes, frozen adversarial review, and commit/push after each
   cohesive green unit.

## Previous checkpoint: exact mixed numeric ordering

Date: 2026-07-29

Committed and pushed checkpoint:

- `a03aa33` — exact mixed numeric comparison, automatic sort, and extrema
  ordering with bounded ClickHouse execution and adversarial boundary coverage.

This slice closes the known `Float64` collapse between values such as integer
`9007199254740993` and Decimal `9007199254740992.75`:

1. A shared lexical decimal key now orders eligible values by sign class,
   decimal order, and normalized coefficient. Negative magnitude and
   coefficient-prefix ordering are reversed correctly, equivalent spellings
   share a key, signed zero normalizes to zero, and exponents are never
   expanded.
2. Generic Dynamic comparisons use that exact key for integers, validated
   semantic Decimals, and bounded complete-decimal Strings. Fixed numeric
   columns and physical Float/literal comparisons retain their native
   ClickHouse semantics. A generic Dynamic Float contributes its canonical
   rendered `Float64` decimal spelling, which is an explicit v0.1 contract
   rather than a claim of exact binary-rational comparison.
3. Automatic Dynamic sort uses separate classifier and lexical channels.
   Numeric classification is bounded, while an ineligible String retains its
   complete logical text; distinct values above the numeric-parser ceiling no
   longer collapse to the same empty fallback key.
4. `stats min` and `stats max` use the same exact order for scalar and
   multivalue runtime fields. Losslessly round-trippable winners publish as
   `Float64`; every other numeric winner publishes as validated
   `decimal/v1`. The scalar aggregate retains only the compact publication
   tuple and ordering key required by `argMinOrNullIf` / `argMaxOrNullIf`.
5. Ordinary String classification is capped at 4,096 bytes and nonzero
   exponent magnitude 10,000. A validated Decimal ordering payload may use the
   one reserved normalization byte needed when `.digits` becomes
   `0.digits`. Separate `(lexical text, exact input)` Dynamic extrema channels
   preserve that 4,097-byte value through re-aggregation without treating a
   4,097-byte raw String as numeric.
6. Repeated exact predicates materialize one key and eligibility alias per
   field, including fields absent from the initial visible schema. The aliases
   compose with calculated-field `MATERIALIZED` / `ARRAY JOIN` fences, reject
   forged same-name/different-path references from optimization, and are
   removed after the filter.
7. Unit coverage includes exact-key normalization, pairwise `big.Rat`
   ordering, exponent and byte ceilings, SQL-size ceilings, binding mismatch
   defense, 32 repeated comparisons, calculated-field fences, and forged field
   references.
8. The digest-pinned ClickHouse suite covers positive and negative
   beyond-`2^53` neighbors, base and `where` comparisons, repeated
   field-to-field comparisons, exact sort, scalar/list extrema, downstream
   comparison/sort/bin metadata, zero and exponent edges, negative
   coefficient prefixes, 4,096/4,097-byte classification, adversarial
   over-limit lexical insertion order, and Decimal extrema followed by a
   second extrema.

Validation on pushed commit `a03aa33`:

```sh
make proto
go test ./... -count=1
go test -race ./internal/clickhouse -count=1
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache-exact-numeric-final-20260729 \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
npm run lint
npm run typecheck
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
go mod tidy
git diff --check
```

All repository Go tests, the ClickHouse race suite, vet, build, reproducible
protobuf generation, frontend lint/type checking, and all 183 frontend/release
tests passed. The repository-wide CI-pinned golangci-lint v2.12.2 run reported
`0 issues`; it identified and removed one genuinely dead comparison helper.
`go mod tidy` produced no module-file diff. The digest-pinned ClickHouse suite
passed in 63.14 seconds.

Three independent final reviewers verified the unchanged 2,243-insertion and
268-deletion staged diff at SHA-256
`8580e5284c2b07d343e3d19b312e9303a15dd6d80e2be0f88dc66a1a48bff519`.
They reviewed mathematical ordering, type intent, Float behavior, attacker
work, query growth, alias scope, ClickHouse lambda/type behavior, publication
metadata, downstream composition, integration coverage, and documentation; no
P0/P1/P2 finding remains. An earlier frozen review found and prompted both the
over-limit lexical-sort repair and the 4,097-byte extrema re-aggregation
repair. The `simplify` review reduced duplicated exact-key SQL, compacted
scalar aggregate state, and made the reference-order property test
non-vacuous before final verification.

GORM remains confined to the SQLite control plane. ClickHouse storage and SPL
execution continue to use native bounded SQL; this slice introduces no GORM
dependency there. The user's dependency upgrades remain committed separately
as `347a015`, and this slice leaves `go.mod` and `go.sum` unchanged.

Explicit pause point:

1. Exact mixed numeric comparison, sort, and extrema behavior is complete for
   this unit.
2. Do not begin another implementation slice until the user gives further
   instructions.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, pinned ClickHouse acceptance, frozen
   adversarial review, and commit/push after each cohesive green unit.

## Previous checkpoint: bounded GORM ingestion-token lifecycle

Date: 2026-07-29

Committed and pushed checkpoint:

- `72b1b11` — bounded token and scope catalogs, transactional revoked-token
  retention and capacity reclamation, bounded metadata hydration, sanitized
  administration behavior, and real collector-runtime acceptance.

This slice closes the unbounded ingestion-token control-plane lifecycle:

1. The GORM-backed SQLite catalog now has a 1,024-parent production default
   and hard structural ceiling plus a 16,384-row global token-to-index
   membership ceiling. Configured parent limits may be lower, but no request
   can raise either structural bound.
2. Ordinary revocation retains the just-revoked token and then the newest
   prior tombstones up to the configured retention bound, with deterministic
   ordering by `(revoked_at_unix_micro DESC, ingestion_token_id DESC)`.
   Migration `0014` adds the matching partial covering index over revoked rows,
   and the explicit GORM model carries the same index definition.
3. Creation admits a new token only after both parent and scope equations fit.
   It may reclaim the deterministic oldest suffix of revoked tombstones,
   including the last tombstone when necessary, but never deletes an active,
   disabled, or merely expired token. Each victim's exact scope count
   participates in admission.
4. Capacity inspection, victim deletion, parent insertion, and scope insertion
   share one immediate SQLite/GORM transaction. Trigger-forced insert and prune
   failures prove that reclaimed tombstones, their scopes, and the current
   revocation all roll back together.
5. Updates use the transactional equation
   `physical scopes - current scopes + replacement scopes <= 16,384`.
   Reductions remain possible at the ceiling, growth returns the shared
   capacity sentinel, and rejected updates leave the version, metadata, and
   memberships unchanged.
6. Get and list no longer hydrate an unbounded `group_concat`. They first use
   constant-size byte-width projections, then load bounded parent and scope
   rows under one read snapshot. Structural probes stop at `limit + 1`.
   Hydration rejects oversized persisted text, missing targets, unknown
   parents, duplicate scopes, zero-scope tokens, and more than 256 scopes for
   one token.
7. Exact-boundary coverage lists 1,024 tokens with exactly 16,384 memberships,
   including a 1,024-parent GORM child query. Hostile fixtures cover parent and
   scope overflow, orphan rows, oversized IDs and descriptions, scope-capacity
   recovery, update rejection, insert rollback, and revocation without
   mutation on corrupt catalogs.
8. Administrator creation maps unrecoverable capacity to sanitized HTTP `429`.
   Pruned tombstones return `404`, disappear from exact list totals and
   filters, and invalidate cursors whose snapshot included them. Database
   details, token prefixes, and plaintext credentials remain absent from error
   responses.
9. A real listener-backed gRPC runtime test proves a pruned credential remains
   indistinguishable from every other unauthorized credential, while a valid
   token completes Hello admission and queues its batch without changing the
   ClickHouse persistence implementation.
10. The stricter metadata reader is also exercised by collector-admission
    corruption tests. Those tests inspect the persisted last-use scalar
    directly after deliberately corrupting fanout, rather than asking the
    hardened administrative reader to accept the corrupt record.

Validation on pushed commit `72b1b11`:

```sh
make proto
go test ./... -count=1
go test -race \
  ./internal/control ./internal/auth ./internal/collectoradmission \
  ./internal/server ./cmd/open-splunk-server -count=1
go test ./internal/auth \
  -run '^(TestConcurrentCollectorTokenCreatesRespectTotalRecordLimit|TestConcurrentCollectorTokenRevocationsPreserveRetentionLimit)$' \
  -count=20
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache-token-retention-20260729 \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m \
    --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102 \
    --max-issues-per-linter=0 --max-same-issues=0
npm run lint
npm run typecheck
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
go mod tidy
git diff --check
```

All repository Go tests, the touched-package race suite, 20 repeated
concurrent create/revoke runs, vet, build, reproducible protobuf generation,
frontend lint/type/tests, and module normalization passed. The CI-pinned
golangci-lint v2.12.2 ratchet reported `0 issues`, and `go mod tidy` produced no
module-file diff. The digest-pinned ClickHouse suite passed every subtest in
54.35 seconds.

Three independent final reviewers verified the exact 4,143-insertion and
87-deletion staged diff before and after with SHA-256
`ffd111c19c90462972ea69ee58212e6bdffd1ed97df0dc9184721e922ac8ecec`.
They reviewed transaction serialization, capacity math, query plans,
corruption bounds, rollback, HTTP secrecy, cursor behavior, runtime
authorization, documentation, migration/model parity, and cross-package
coverage; no actionable P0/P1/P2 finding remains. The `simplify` pass factored
one shared victim query and retained the reusable two-phase child-loader
pattern.

GORM remains confined to the SQLite control plane. ClickHouse event storage and
SPL execution continue to use their native bounded implementation. The
previous Go dependency upgrade is already committed on `main` as `347a015`;
this slice leaves `go.mod` and `go.sum` canonical and unchanged.

Historical pause point at that checkpoint:

1. Do not begin another implementation slice until the user gives further
   instructions.
2. The exact mixed numeric target identified here is now complete at
   `a03aa33`; use the latest checkpoint above as the active pause state.
3. Preserve the GORM-only SQLite control-plane boundary; do not introduce GORM
   into ClickHouse persistence.
4. Continue test-first checkpoints, pinned ClickHouse acceptance, frozen
   adversarial review, and commit/push after each cohesive green unit.

## Previous checkpoint: input-scoped collector durability

Date: 2026-07-29

Committed and pushed checkpoint:

- `e312ae9` — input-scoped checkpoints, WAL source marks, pending-WAL resume,
  terminal disposition, hostile-state validation, and deterministic
  shared-file crash/restart acceptance.

This slice closes the collector compatibility boundary where two logical file
inputs intentionally observe the same physical file:

1. Durable checkpoints are now keyed by the collision-safe pair
   `(input_id, FileIdentity.TrackingKey())`. The configured input ID is carried
   through discovery, copy-truncate generations, batching, WAL source marks,
   startup resume overlays, terminal acknowledgments, and local oversized-event
   disposition.
2. WAL append/recovery and cumulative acknowledgment coalesce by
   `(input_id, exact file generation)` with struct keys and deterministic
   input/identity/event ordering. Planning remains `O(M + K log K)` for `M`
   compact marks and `K` distinct input/file generations, outside the queue
   mutex.
3. Checkpoint format v2 persists the input ID and rejects nonempty v1 state
   explicitly. This is intentional for the greenfield, pre-release project;
   there is no migration path to preserve. Empty v1 state upgrades on its next
   write.
4. Both loaded and newly written checkpoints now require a canonical
   protocol ID, canonical positive-generation SHA-256 identity, nonempty path,
   signed-file-range offset, at most a 1 MiB fingerprint prefix, and a
   structurally safe line cursor. Batched writes validate before mutation and
   again after monotonic cursor normalization, so the store cannot persist
   state its own loader rejects.
5. A file first discovered empty must establish and durably record a nonempty
   distinguishing fingerprint before its first event is emitted. Fingerprint
   or checkpoint-write failure now fail-stops that poll and retries without
   advancing the tailer.
6. The canonical 128-byte ASCII protocol-ID grammar is owned by the shared
   `internal/protocolid` package and reused by collector configuration, input
   durability, ingestion validation, and the GORM fleet control plane.
7. Product-level acceptance uses one real file/inode through two differently
   routed inputs. It proves independent historical ingestion, one deterministic
   mixed-input pending WAL batch, canceled-handler shutdown, startup overlay
   recovery without reread, two terminal checkpoints, and zero duplicate
   deliveries.
8. The pinned ClickHouse legacy-retention fixture now derives its event time
   from the current wall clock rather than a fixed old timestamp. Its
   deliberately tiny 1.5 ms TTL can no longer race asynchronous deletion, while
   the native millisecond-rounding assertion remains unchanged.

Validation on pushed commit `e312ae9`:

```sh
make proto
go test ./... -count=1
go test -race ./internal/collector/... -count=1
go test ./internal/collector \
  -run '^TestE2ECollectorCheckpointsAreScopedByInput$' -count=10
go test -race ./internal/collector \
  -run '^TestE2ECollectorCheckpointsAreScopedByInput$' -count=3
go vet ./...
go build ./...
golangci-lint v2.12.2 run \
  --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run lint
npm run typecheck
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
go mod tidy
git diff --check
```

All repository Go tests, the complete collector race suite, repeated
shared-file acceptance, vet, build, reproducible protobuf generation, frontend
lint/type/tests, and module normalization passed. Pinned golangci-lint v2.12.2
reported `0 issues`; `go mod tidy` produced no module-file diff. The
digest-pinned ClickHouse suite passed every subtest in 59.11 seconds.

Three independent adversarial passes found and drove fixes for hostile
checkpoint allocation/state, noncanonical input configuration, a
physical-identity-only sender fake, a mixed-input pending-WAL coverage gap,
empty-file fingerprint failure swallowing, post-normalization cursor
corruption, zero-line cursor overflow, and a canceled Store-handler race. The
second frozen review found no remaining P0/P1/P2 issue. Every reviewer verified
the exact 2392-insertion/277-deletion staged diff with SHA-256
`a769e8099a5a0b91e481ab302ce05cab4b063c8303087b44feaf94b8d58356a4`.

The `simplify` review consolidated the protocol identifier grammar instead of
adding a third copy; no remaining reuse, maintainability, or scaling issue was
actionable in the final diff. GORM remains confined to the SQLite control
plane, and ClickHouse continues to use its native persistence path.

Explicit pause point:

1. Do not begin another implementation slice until the user gives further
   instructions.
2. Overlapping file-input globs and hard links across distinct input IDs now
   retain independent durable cursors and WAL recovery.
3. Nonempty checkpoint v1 state is deliberately unsupported before the first
   release; reset it rather than adding a migration.
4. Continue test-first work, pinned ClickHouse acceptance, frozen adversarial
   review, and commit/push after each cohesive green unit.

## Previous checkpoint: authenticated GORM collector administration

Date: 2026-07-29

Committed and pushed checkpoints:

- `8161f2d` — bounded process-owned collector liveness snapshots;
- `f7a06b7` — migration-backed GORM catalog indexes and query-plan coverage;
- `c84de56` — bounded batched GORM fleet hydration;
- `f3fc981` — signed snapshot-bound collector catalog cursors;
- `125b2bc` — the complete tenant-scoped GORM collector catalog; and
- `782da43` — authenticated HTTP/protobuf collector administration, runtime
  wiring, durable-capacity enforcement, and end-to-end acceptance.

This slice completes the planned backend boundary for collector fleet
administration:

1. `collectorfleet.Catalog` reads the SQLite control plane through GORM. It
   performs bounded tenant-scoped filtering, exact optional counts, stable
   indexed ordering, four-record keyset pages, and batched child hydration.
   Signed purpose-separated cursors bind the request, durable catalog revision,
   liveness digest, continuation key, and optional total.
2. Each get or list captures the process liveness snapshot exactly once.
   Online/stale state is derived from the complete exact lease; a nil runtime
   is deliberately offline. Continuations fail closed after a durable revision
   or liveness change rather than mixing snapshots.
3. Four authenticated administrator routes expose list, get, display-name
   update, and state mutation. Tenant identity comes only from the authenticated
   principal, malformed or non-canonical protobuf state is rejected before the
   service call, and `SERVER_FEATURE_COLLECTOR_ADMIN` is advertised only when
   the complete service is configured.
4. Mutations use durable optimistic versions and return only the durable
   administration snapshot. They do not perform a post-commit telemetry read,
   so corrupt or concurrently changing operational telemetry cannot turn a
   committed administrator action into an apparent failure.
5. A hard limit of 256 durable collector identities per tenant bounds text
   filtering, sorting, exact counts, and hydration. Existing collectors can
   reconnect at capacity, disabled-state precedence is preserved, and
   concurrent new claims cannot cross the limit. Capacity rejection maps to
   gRPC `ResourceExhausted` and rolls back token-use and fleet writes from the
   admission transaction.
6. Real authenticated HTTP, native bufconn gRPC, migrated SQLite/GORM, and
   database-reopen acceptance proves route authorization, feature discovery,
   live/offline projection, mutations, durable persistence, and continuation
   invalidation together.
7. GORM remains confined to the SQLite control plane. ClickHouse persistence
   and SPL execution retain their native bounded implementation.

Validation on pushed commit `782da43`:

```sh
make proto
go test ./... -count=1
go test -race \
  ./internal/collectorfleet ./internal/collectoradmission \
  ./internal/server ./cmd/open-splunk-server ./internal/ingest -count=1
go vet ./...
go build ./...
GOLANGCI_LINT_CACHE=/private/tmp/open-splunk-golangci-cache-20260729-collector-admin \
  go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All repository Go tests, the focused race suite, vet, build, reproducible
protobuf generation, frontend type/lint/tests, and the repository-wide pinned
Go lint ratchet passed; the linter reported `0 issues`. The digest-pinned
ClickHouse suite passed all subtests in 59.31 seconds. Adversarial review found
and drove fixes for unbounded tenant-wide infix work, duplicated trusted
liveness validation, redundant full protobuf sizing, duplicated boundary
cloning and version checks, and a lint-shadowed built-in. Security and API
reviews additionally exercised authentication, tenant isolation, malformed
protobufs, cursor/total/revision combinations, transaction rollback, output
bounds, and serialization-permit release. Three independent final reviewers
verified the exact 260301-byte staged diff with SHA-256
`dfe3731bddf5d68e1958b881cdaac75c2a7d5437c4374639998564bea1acb503`;
no actionable P0/P1/P2 finding remains.

Explicit pause point:

1. Do not begin another implementation slice until the user selects it.
2. When work resumes, preserve the GORM-only SQLite control-plane boundary and
   keep ClickHouse on its native path.
3. Continue with test-first checkpoints, pinned ClickHouse acceptance,
   adversarial review, and commit/push after each working unit.

## Previous checkpoint: production coalesced collector heartbeat lifecycle

Date: 2026-07-28

Committed and pushed checkpoints:

- `76a6191` — bounded latest-wins heartbeat runtime with exact durable-lease
  fencing, monotonic online/stale state, and serialized shutdown; and
- `88bdf16` — production activation, coalesced heartbeat admission, final
  release drain, durable disconnect ordering, and forced-shutdown integration.

This slice replaces synchronous per-heartbeat SQLite writes with one bounded
runtime that preserves the persisted GORM fleet contract:

1. A stream becomes heartbeat-active only after its durable lease is admitted
   and its exact process lease is current, but before `CollectorReady` is sent.
   Activation failure releases both authorities and sends no Ready response.
2. Heartbeats are detached and normalized at the RPC boundary, then stored in
   a capacity-bounded latest-wins map. One worker flushes at most one snapshot
   per collector per cycle, and one five-second deadline bounds the whole cycle
   rather than each entry.
3. Every offer, flush, release, and retry compares the complete trusted
   tenant/collector/boot/stream/generation lease. A failed in-flight flush is
   retained for the waiting exact Release, while successor activation prevents
   a predecessor completion from being requeued or deleted.
4. Release marks the exact lease offline, drains its latest accepted snapshot,
   and completes before durable Disconnect clears the lease. Final write
   failures and pending snapshots discarded at a release deadline are
   asynchronously observable even when a later idempotent cleanup retry
   succeeds.
5. Runtime ownership is explicit. Disabled collector transport starts no
   worker; configured transport validation happens before either persistence
   plane opens; startup failure closes the worker immediately; normal shutdown
   first joins all gRPC handlers and then idempotently closes the runtime.
6. The 35-second collector shutdown envelope reserves 15 seconds for graceful
   transport drain and 20 seconds for forced handler cleanup. Cleanup in turn
   reserves ten seconds for heartbeat Release and ten for SQLite Disconnect,
   covering the five-second busy timeout plus transaction and retry slack.
7. Real migrated SQLite/GORM and native bufconn gRPC tests prove immediate
   Goodbye drain, forced `grpc.Server.Stop` cleanup, exact offline persistence,
   reopen behavior, and the absence of post-Close heartbeat resurrection.

Validation on pushed commit `88bdf16`:

```sh
go test ./... -count=1
go test -race \
  ./internal/collectorfleet ./internal/ingest ./cmd/open-splunk-server \
  ./internal/collector ./internal/collector/sender -count=1
go vet ./...
go build ./...
/private/tmp/open-splunk-golangci-lint-v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All repository tests, touched-package race tests, vet, build, and the
repository-wide zero-lint ratchet passed. The pinned ClickHouse acceptance
suite passed all subtests in 58.34 seconds. Independent lifecycle, efficiency,
and reuse reviews found and drove fixes for a per-entry timeout amplification,
an inactive exact-lease requeue loss, final-release error masking, insufficient
SQLite cleanup slack, late transport validation, and one startup worker leak.
The stable diff has no remaining P0/P1/P2 finding.

Next implementation checkpoints recorded at that point:

1. Add a bounded storage-side GORM fleet catalog with batched child loading,
   migration-backed sort/filter indexes, signed snapshot-bound keyset cursors,
   and SQLite query-plan tests.
2. Wire the four authenticated collector administrator routes and advertise
   `SERVER_FEATURE_COLLECTOR_ADMIN` only after get/list/mutations and liveness
   semantics are complete.
3. Repeat real HTTP/native-gRPC/SQLite/reopen acceptance, the pinned
   ClickHouse suite, and independent adversarial review.

## Previous checkpoint: durable collector runtime fencing

Date: 2026-07-28

Committed and pushed checkpoints:

- `7216009` — atomic token revalidation, token-use recording, enabled-state
  validation, and durable lease allocation;
- `070b47a` — fresh credential and exact-lease authorization in one read-only
  GORM snapshot; and
- `48c8b7d` — production startup, gRPC lifecycle, heartbeat, batch, and cleanup
  wiring for the exact durable lease.

This slice makes the persisted GORM fleet lease non-bypassable in production:

1. Server startup generates one cryptographically random boot epoch, opens the
   migrated GORM fleet store, and invalidates every prior-boot active lease
   before constructing the collector listener. Startup fails closed if that
   invalidation cannot commit.
2. A validated `CollectorHello` is mapped into a bounded detached snapshot.
   The admission coordinator uses one immediate GORM transaction to freshly
   revalidate the bound bearer token, record its last use, verify enabled fleet
   state, and allocate the exact tenant/collector/boot/stream/generation lease
   before `CollectorReady`.
3. Admission through process activation is serialized per trusted
   tenant/collector identity. This prevents an older committed generation from
   becoming process-current after a newer durable generation committed but did
   not reach activation.
4. The process registry never invents a durable generation. It retains the
   highest observed generation per administratively provisioned collector and
   conditionally releases the complete lease plus its process activation
   token, so delayed cleanup cannot remove or revive a successor.
5. Every heartbeat and event batch freshly revalidates credential state,
   authorized indexes, and the exact enabled durable lease from one GORM
   snapshot before operational work. A stale or disabled lease aborts before
   ClickHouse. Heartbeats are synchronously and conditionally persisted with
   the server receive time and stream request sequence.
6. Deferred disconnect uses a detached bounded context, the complete durable
   lease, and a stable server timestamp. It retries transient failures three
   times; commit-ambiguous cleanup is safe because the exact conditional
   mutation is idempotent and a successor cannot be cleared.
7. Boot, Hello, heartbeat, lease, timestamp, duration, collection, enum, and
   optional-value mappings fail closed on lossy or unbounded protobuf state.
   Canonical index names use the control-plane 255-byte contract instead of
   the unrelated 128-byte protocol-ID bound.
8. Real migrated SQLite/GORM plus native bufconn gRPC tests prove
   Hello/Ready, exact lease persistence, full heartbeat projection,
   administrator disable, pre-ClickHouse batch fencing, and active
   Goodbye-to-disconnect cleanup. Forced-order tests cover reverse durable
   generation activation and token expiry while waiting on the finalizer.

Validation on pushed commit `48c8b7d`:

```sh
go test ./... -count=1
go test -race \
  ./internal/ingest ./cmd/open-splunk-server \
  ./internal/collector ./internal/collector/sender -count=1
go vet ./...
go build ./...
/private/tmp/open-splunk-golangci-lint-v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All Go packages passed. The touched runtime packages passed under the race
detector, vet and build passed, pinned golangci-lint v2.12.2 reported
`0 issues`, and the pinned ClickHouse store suite passed all subtests.
Independent adversarial review found and drove fixes for reverse-generation
activation, stale acceptance time after finalizer wait, best-effort one-shot
cleanup, lossy enum conversion, and two false-positive tests. The stable diff
has no remaining P0/P1/P2 lifecycle or coverage finding.

Next implementation checkpoints:

1. Add fleet-only GORM administrator reads and partial CAS mutations so display
   updates and state changes preserve unspecified fields without loading
   operational children. Disable must still commit when runtime or telemetry
   rows are corrupt.
2. Add a bounded latest-wins, generation-conditional heartbeat coalescer and a
   monotonic process liveness tracker with explicit stale grace and shutdown
   semantics.
3. Add a bounded storage-side fleet catalog with batched child loading,
   migration-backed sort/filter indexes, signed snapshot-bound keyset cursors,
   and SQLite query-plan tests.
4. Wire the four authenticated collector administrator routes and advertise
   `SERVER_FEATURE_COLLECTOR_ADMIN` only after get/list/mutations and liveness
   semantics are complete.
5. Repeat real HTTP/native-gRPC/SQLite/reopen acceptance, the pinned
   ClickHouse suite, and independent adversarial review.

## Previous checkpoint: durable GORM collector fleet persistence

Date: 2026-07-28

Committed and pushed checkpoint:

- `e51c27e` — normalized collector-fleet persistence, durable stream leases,
  administrator state, and bounded telemetry through explicit GORM models.

This slice implements the persisted fleet primitive required by the collector
identity and fencing contract:

1. SQLite migration `0013_collector_fleet.sql` adds tenant-scoped fleet,
   runtime, capability, authorized-index, input-registration, and input-health
   tables. Checked-in SQL remains the sole schema authority; production never
   calls `AutoMigrate`.
2. Explicit GORM models make keys, relationships, bounds, and named checks
   legible beside the Go domain. Tests compare every model-declared named
   check with the authoritative migration and spot-check the critical active
   lease and observation-pair invariants.
3. `collectorfleet.Store` provides durable claim, heartbeat, conditional
   disconnect, administrator update, boot invalidation, and bounded get
   operations. Claims allocate a monotonic lease generation and require the
   already authenticated collector identity; this package does not implement
   trust-on-first-use.
4. Every mutable runtime operation is fenced by the exact tenant, collector,
   server boot ID, lease generation, and stream ID tuple. Telemetry revisions
   and administrator optimistic-lock versions are independent and monotonic.
5. Disablement is fail-safe and authoritative even if unrelated persisted
   runtime or child rows are corrupt. Re-enablement requires a valid inactive
   runtime, so it cannot revive a dormant lease. Terminal revision capacity
   is reserved so disconnect, disable, and boot invalidation can still fence
   a live collector at `MaxInt64`.
6. Process startup can durably invalidate prior-boot leases without allowing a
   corrupt disabled row to block healthy collectors. This invalidation is
   mandatory before the server admits collector traffic.
7. Public timestamps, identifiers, encoded payloads, strings, collections,
   relationships, aggregates, and child cardinalities are bounded. Reads and
   writes fail closed on constraint-valid corruption rather than projecting a
   partially authoritative fleet view.
8. Ordinary reads use a read-only WAL transaction. Mutations use immediate
   SQLite transactions where ordering matters, and child reads are capped at
   their protocol maximum plus one so corrupt databases cannot force
   unbounded allocations.

This is intentionally an unwired persistence primitive. Production must not
sequence token refresh, token-use recording, enabled-state validation, and
`Claim` as separate transactions: administrator disablement or credential
changes could interleave between them. The integration layer must perform
those decisions and durable lease allocation in one immediate SQLite
transaction before replacing the current process-local admission result.

Validation on pushed commit `e51c27e`:

```sh
go test ./... -count=1 -timeout=20m
go test -race ./internal/collectorfleet ./internal/control -count=1
go vet ./...
go build ./...
/private/tmp/open-splunk-golangci-lint-v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
git diff --check
```

All Go packages passed, the focused race suite passed, vet and build passed,
and pinned golangci-lint v2.12.2 reported `0 issues` with fresh caches.
Independent adversarial reviewers additionally repeated the collector-fleet
suite 30 times normally and 10 times under the race detector. They reviewed
tenant isolation, takeover and cleanup races, disable/re-enable ordering,
restart invalidation, revision saturation, corruption handling, timestamp and
byte bounds, and GORM/migration parity; no P0/P1/P2 findings remain. The pinned
ClickHouse GradeThis compatibility, inspection-service, and inspection-route
acceptance tests also passed with
`clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.

Next implementation checkpoints:

1. Add one immediate-transaction admission coordinator that revalidates the
   bound token, records last use, verifies fleet enabled state, and allocates
   the durable lease atomically.
2. Invalidate prior-boot leases before opening collector traffic, then replace
   process-local heartbeat, batch, goodbye, and deferred-cleanup checks with
   the exact durable lease boundary.
3. Add a bounded latest-wins heartbeat coalescer whose flush remains
   generation-conditional and whose shutdown cannot resurrect stale state.
4. Expose bounded fleet get/list and administrator mutation APIs only after
   production fencing is non-bypassable; add cursor snapshot and SQLite
   query-plan coverage for the actual sort/filter shapes.
5. Re-run the full Go, race, vet, build, pinned lint, pinned ClickHouse, and
   real HTTP/native-gRPC/SQLite restart gates, followed by independent
   adversarial re-review.

## Previous checkpoint: collector identity and fencing contract

Date: 2026-07-28

This design checkpoint resolves the security and compatibility choices that
must precede a persisted GORM collector fleet:

1. `collector_id` already controls stored provenance and the durable
   `(tenant, collector, sequence)` ingestion namespace. Production currently
   leaves the authorization binding empty, so any valid token can claim
   another well-formed ID and reserve its future sequence. Independent
   adversarial review classified this as a present P1 integrity/availability
   defect rather than a future fleet-only concern.
2. Native credentials are explicitly bound by an administrator, never through
   trust-on-first-use. New tokens require a bounded collector ID. A migration
   keeps legacy rows nullable only so existing databases open successfully;
   unbound credentials fail closed on native gRPC until they are bound once
   under optimistic locking or, preferably, revoked and rotated.
3. A nonempty binding is immutable and cannot be cleared. It is intentionally
   not unique because safe token rotation may briefly leave two active
   credentials bound to the same collector.
4. Authentication returns the binding as trusted state and Hello must match
   it before readiness, last-use recording, visibility reservation, or
   ClickHouse insertion. Client `instance_id` remains display metadata.
5. Before fleet persistence, the singleton server uses an in-memory
   `(tenant_id, collector_id)` registry with a boot epoch and monotonic
   generation. A new validated stream supersedes and fences the old stream;
   every post-ready message and conditional cleanup checks the lease.
6. A message admitted under the current lease may finish if a takeover or
   credential update races afterward. Messages reaching the boundary later
   fail before durable work. Heartbeats and batches both refresh token
   authorization.
7. The subsequent GORM fleet slice will replace the process-local registry
   with transactionally allocated durable leases and linearizable
   administrator disablement. Fleet telemetry will use server receive time,
   boot invalidation, monotonic liveness deadlines, bounded payloads, and
   coalesced generation-conditional writes separate from administrator CAS
   versions.

Required prerequisite acceptance coverage includes migration/model parity and
reopen behavior; immutable one-way binding; match/mismatch and legacy
fail-closed runtime streams; cross-token provenance/sequence isolation;
simultaneous takeover barriers; stale heartbeat, batch, and cleanup fencing;
revocation/scope refresh; server-restart boot invalidation; and tenant
isolation. The existing real HTTP/native-gRPC/SQLite runtime fixture will be
extended rather than replaced.

Next implementation checkpoints:

1. Add the nullable authoritative SQL column, explicit GORM mapping, domain
   projection, create requirement, one-way legacy binding, and authentication
   result with unit, migration, concurrency, corruption, and reopen tests.
2. Accept, validate, project, and mask the existing
   `bound_collector_id` administrator protobuf field, then map it through the
   production authorizer and prove legacy failure and exact Hello matching
   through real HTTP and gRPC.
3. Add the process-local superseding lease registry and adversarial concurrent
   stream tests before any collector fleet table or feature advertisement.
4. Re-run the full Go, race, vet, build, pinned lint, and pinned ClickHouse
   compatibility gates, then perform independent adversarial re-review.

## Previous checkpoint: durable ingestion-token last use

Date: 2026-07-28

Committed and pushed checkpoints:

- `f81dc75` — migration, GORM projection, and monotonic token-use persistence;
- `b681348` — one synchronous token-use write per accepted native stream;
- `003e9aa` — administrator projection, optional-time sorting, and stale-page
  detection; and
- `6c0b329` — production adapter/wiring plus real HTTP, gRPC, SQLite, and
  restart proof.

This slice completes the Phase 3 ingestion-token creation/revocation/last-seen
contract without prematurely advertising a collector-fleet API:

1. SQLite migration `0011_ingestion_token_last_used.sql` adds nullable
   `last_used_at_unix_micro` with a check that every recorded use is at or
   after token creation. Existing rows upgrade to `NULL`. SQL migrations
   remain authoritative; the explicit GORM model and schema-parity tests map
   the column without calling `AutoMigrate`.
2. `auth.CollectorToken.LastUsedAt` is safe metadata and is projected by
   create/get/list as zero until the first accepted stream. The persisted
   value survives database and process-store reopen.
3. `RecordCollectorTokenUse` performs one atomic GORM `UpdateColumn`. It keeps
   the maximum observation under concurrent connections and older/equal
   clocks, requires an active and unexpired token, and collapses inactive
   states behind one internal error category.
4. Token use is operational telemetry. Recording it never increments the
   administrator optimistic-lock version or changes `updated_at`, so active
   collectors cannot invalidate unrelated administrator metadata updates.
5. The acceptance time is captured from the server clock after bearer
   authentication and complete `CollectorHello` identity/protocol validation,
   and before `CollectorReady`. A wall-clock correction behind the token's
   durable creation timestamp is clamped to that lower bound; it cannot move a
   newer observation backward or falsely deauthenticate the already-validated
   credential.
6. The ingestion service passes only the safe token subject ID and server
   acceptance time to a narrow recorder. It never passes plaintext or digest
   material and does not write for invalid authentication, malformed Hello,
   invalid stream allocation, heartbeats, batch reauthorization, or
   individual batches.
7. A recorder failure prevents readiness. Inactive-token races map to a
   sanitized `Unauthenticated`; cancellation and deadline identities retain
   their transport categories; all other persistence failures become a
   generic retryable `Unavailable` without exposing SQLite diagnostics.
8. The production adapter deliberately translates auth-layer inactive errors
   into the ingestion boundary's credential sentinel. Production always wires
   the process-owned token store into the service while preserving the
   control database's single close owner.
9. The existing administrator protobuf `last_used_at` field is now populated
   and the existing `INGESTION_TOKEN_SORT_BY_LAST_USED_AT` mode is enabled.
   Unused values follow the established optional-time order and token ID is
   the deterministic tie-breaker in both directions.
10. Signed administrator page tokens bind all filters, sort direction, page
    size, and an exact snapshot. Last-use timestamps are part of that snapshot
    even though they do not alter token versions, so a telemetry update cannot
    produce a mixed or reordered continuation page.
11. A real runtime test opens the migrated control database through production
    key derivation, creates a scoped token through authenticated HTTP, admits a
    native gRPC collector stream using the one-time secret, observes the exact
    server timestamp through authenticated HTTP get/list, closes the handler
    and database, reopens both with the persisted master key, and observes the
    same timestamp and unchanged CAS metadata.
12. Independent adversarial reviews covered migration compatibility,
    SQLite/GORM matched-row behavior, concurrency, clock rollback, error
    secrecy, stream ordering, runtime wiring, resource shutdown, optional-time
    sorting, timestamp validation, response bounds, and cursor snapshots. One
    P2 clock-rollback rejection was found, fixed with the atomic
    creation-boundary clamp, stress-regressed, and re-reviewed. No P0/P1/P2
    findings remain.

Validation on pushed commit `6c0b329`:

```sh
go test ./... -count=1
go test -race \
  ./internal/auth ./internal/control ./internal/ingest \
  ./internal/server ./cmd/open-splunk-server \
  -count=1
go vet ./...
go build ./...
/private/tmp/open-splunk-golangci-lint-v2.12.2 \
  run --timeout=10m --max-issues-per-linter=0 --max-same-issues=0
git diff --check
```

All Go packages and the combined auth/control/ingestion/server/runtime race
suites passed. Vet and build passed, and pinned golangci-lint v2.12.2 reported
`0 issues`. Focused clock-rollback, concurrent-monotonicity, admission,
projection, sorting, paging, and reopen tests were additionally repeated
between 20 and 50 times under normal and race builds. The listener-based full
and race suites were run outside the filesystem/network sandbox after their
initial sandbox attempts were denied loopback binds.

This slice does not change SPL parsing, planning, generated ClickHouse SQL, or
event insertion, and its real stream intentionally stops after readiness.
The preceding pinned all-ten-query GradeThis gate therefore remains the
relevant ClickHouse compatibility evidence.

Next priorities:

1. Define and implement non-bypassable collector identity binding and instance
   fencing before persisting or advertising collector fleet administration.
2. Once identity is durable, add a read-only persisted fleet catalog with
   bounded, coalesced heartbeat writes and explicit stale/offline semantics;
   defer administrative disablement until a collector cannot evade it by
   choosing a new ID.
3. Continue Phase 3 with reports/dashboards, per-index user permissions, HEC
   compatibility, and expanded RBAC/audit search after their unresolved
   contracts are made explicit.
4. Retain the lifecycle backlog: bounded ingestion-token tombstones, surfaced
   export-deletion failures, physical search-history cleanup, completion of
   deleting-index state, and a bounded WebSocket shutdown dependency.
5. Keep every SPL change behind unit, adversarial, and pinned ClickHouse
   compatibility evidence.

## Previous checkpoint: bounded GORM control plane

Date: 2026-07-28

Committed and pushed checkpoints:

- `cddeb7a` — index-definition persistence through GORM;
- `af31a35` — app-workspace persistence through GORM;
- `5996973` — ingestion-token persistence through GORM;
- `b490c80` — saved-search persistence through GORM; and
- `baad652` — terminal and pending search-history persistence through GORM.

The bounded mutable SQLite stores now share one explicit persistence model:
checked-in SQL migrations remain the sole schema authority, while GORM maps
reviewable records and queries onto the existing modernc SQLite connection.
No production path calls `AutoMigrate`.

1. Every converted store has an explicit model whose table, column, primary
   key, unique key, index, nullability, and representable check metadata is
   compared with the authoritative migrated schema. Upgrade/reopen coverage
   protects existing databases instead of testing only empty-schema creation.
2. Index definitions and app workspaces retain tenant scope, canonical
   identifiers, lifecycle constraints, default-index referential checks,
   optimistic versions, immediate writer transactions, and signed
   revision-bound keyset pagination.
3. Ingestion tokens retain digest-only persistence, one-time plaintext-secret
   return, bounded index-scope resolution, active/expiry checks, deterministic
   ordering, optimistic update/revoke behavior, and atomic scope replacement.
   Failure-injection tests prove a partial scope update rolls back.
4. Saved searches retain owner and app-workspace scope, definition
   canonicalization, ID-collision retry, exact counts, deterministic signed
   keyset paging, optimistic updates, and app archival/deletion triggers.
   Query-plan tests prove owner-scoped keyset indexes are used without a
   temporary sort.
5. Search history maps terminal jobs and pending attempts separately. GORM now
   covers lifecycle admission, idempotency, completion, interrupted-attempt
   recovery, capacity pruning, clearing, counts, and every supported keyset
   order while retaining immediate writer transactions and canonical protobuf
   checksums.
6. Persisted terminal and pending records are treated as untrusted data.
   Checksum-valid but semantically invalid protobufs and corrupt stored scopes
   return internal corruption errors rather than the invalid-client sentinel;
   request validation and context cancellation retain their original error
   identities. Regression tests exercise Get, List, Begin, Complete, duplicate
   completion, and recovery paths.
7. Concurrent tests cover token compare-and-swap, history capacity and
   idempotency, saved-search mutation, and cursor behavior. Cursor keys remain
   stable across database reopen, and returned domain values are detached from
   mutable ORM records.
8. Independent adversarial reviews were performed after each conversion. The
   sole P2 found in the final search-history review—the persisted-corruption
   classification issue described above—was fixed and stress-regressed. Final
   re-review found no remaining P0/P1/P2 issue in the converted stores.

Validation on pushed commit `baad652`:

```sh
go test ./... -count=1
go test -race \
  ./internal/auth ./internal/control ./internal/savedobjects \
  ./internal/searchhistory ./internal/server ./cmd/open-splunk-server \
  -count=1
go vet ./...
go build ./...
/private/tmp/open-splunk-golangci-lint-v2.12.2 run --timeout=10m
git diff --check
```

All Go packages and the focused control-plane race suites passed. Vet and
build passed, and the pinned repository-wide golangci-lint v2.12.2 run
reported `0 issues`. Verification ran from an isolated worktree anchored at
the pushed commit while the next Phase 3 slice was developed separately.

This checkpoint changes only SQLite persistence. It does not change SPL
parsing, planning, generated ClickHouse SQL, or execution, so the immediately
preceding pinned all-ten-query GradeThis gate remains the relevant ClickHouse
compatibility evidence.

Next priorities:

1. Complete ingestion-token last-use visibility: store a monotonic accepted
   stream timestamp without changing administrator CAS metadata, record it
   once per protocol-valid stream before readiness, and expose the existing
   administrator field and deterministic sort.
2. Define collector identity/fencing and heartbeat coalescing before exposing
   collector fleet administration; token last-use does not imply a durable
   fleet registry.
3. Continue Phase 3 with reports/dashboards, per-index user permissions, HEC
   compatibility, and expanded RBAC/audit search after their unresolved
   contracts are made explicit.
4. Retain the lifecycle backlog: bounded ingestion-token tombstones, surfaced
   export-deletion failures, physical search-history cleanup, completion of
   deleting-index state, and a bounded WebSocket shutdown dependency.
5. Keep every SPL change behind unit, adversarial, and pinned ClickHouse
   compatibility evidence.

## Previous checkpoint: persistent GORM app workspaces

Date: 2026-07-28

Committed and pushed checkpoints:

- `af31a35` — tenant-scoped app workspace persistence through GORM, with SQL
  migrations retained as the sole schema authority;
- `3632cb3` — the complete authenticated administrator app API;
- `a5ab97a` — live active-app bootstrap from the persistent catalog; and
- `40b00ac` — production runtime wiring, stable purpose-separated cursor keys,
  and real HTTP/SQLite restart proof.

This slice establishes the first persistent Phase 3 app workspace from schema
through ordinary browser bootstrap:

1. SQLite migration `0010_app_workspaces.sql` defines `app_workspaces`,
   `app_default_indexes`, and tenant catalog revisions, including tenant-local
   slug uniqueness, globally unique generated app IDs, positive versions,
   lifecycle/timestamp checks, foreign keys, lookup indexes, and triggers that
   keep active default indexes searchable. The migration also bridges legacy
   saved-search namespaces without breaking arbitrary pre-app identifiers and
   prevents a referenced app from being deleted.
2. `internal/control.AppCatalog` uses explicit GORM models and transactions but
   never calls `AutoMigrate`; checked-in SQL remains the reviewable source of
   truth. Model-tag/schema parity and v9-to-v10 upgrade tests keep the GORM
   mapping synchronized with the migration.
3. App IDs are canonical random `app_` identifiers. Slugs are immutable and
   tenant-local; definitions carry a display name, optional canonical
   description, as many as 128 active/searchable default indexes, and an
   authored optional time-range intent whose field presence is preserved
   without resolving wall-clock values during persistence.
4. Create, get, update, state transition, list, and hard delete are
   tenant-scoped and detached. Mutations use optimistic versions and
   immediate SQLite writer transactions. Each tenant is bounded to 256 apps.
   An active app cannot outlive a disabled or archived default index, and hard
   deletion requires archived state, an exact version, the exact canonical
   slug as confirmation, and no saved-search dependency.
5. Listing uses signed composite-keyset continuations over a tenant catalog
   revision. Any create, update, state transition, or delete invalidates an
   older continuation rather than silently duplicating or skipping rows.
   Filters, sort direction, page size, tenant, and required revision are bound
   into the cursor, and total counts are opt-in and exact.
6. The protobuf administrator surface exposes exact POST routes for create,
   get, list, update, state transition, and delete. Exact route/method
   resolution and Host/Origin checks precede administrator authentication;
   authentication precedes content type, admission, decoding, and storage.
   Tenant scope and actor identity come only from the authenticated principal.
7. Requests reject recursive unknown fields, invalid presence, unsupported
   masks, mutable slugs, malformed selectors, padded or oversized page tokens,
   invalid states, and inexact delete confirmations. Service results are
   deeply validated and detached before bounded atomic serialization. Stable
   status categories do not expose persistence diagnostics.
8. Ordinary `/api/v1/system/bootstrap` uses a separate read-only
   `AppCatalog.ListActiveApps` contract. It passes only the server's fixed
   tenant, requires one complete result within the hard 256-app bound,
   validates and detaches every field, rejects corrupt or duplicate IDs/slugs,
   emits active state, and sorts by immutable `(slug, app_id)`.
9. Live catalog and static bootstrap apps cannot both be configured. With a
   live catalog, selection precedence is an active configured default, then an
   active request preference, then the first sorted active app. A missing or
   archived request preference therefore cannot displace a valid configured
   default. Bootstrap remains an ordinary route and does not require an
   administrator bearer token.
10. Production constructs one adapter for both the privileged mutation
    surface and ordinary read-only projection. It borrows the process-owned
    control database and has no competing close lifecycle. Known catalog
    errors map to fixed transport categories; unknown persistence/corruption
    errors retain server-side identity but become generic HTTP failures.
11. The persisted inner catalog cursor and the outer administrator transport
    cursor use two stable, purpose-separated keys derived from the verified
    server master key. The GORM catalog and HTTP handler clone their keys, and
    temporary caller/master-key buffers are cleared on success and failure.
12. A real end-to-end test opens the migrated SQLite database, constructs the
    production adapter and real HTTP handler, authenticates administrator
    create/archive/list calls, observes active apps through unauthenticated
    ordinary bootstrap, closes and reopens the database/adapter/handler, and
    continues an administrator page token issued before restart. It also
    proves tenant isolation, archived-app exclusion, persisted state, stable
    inner and outer keys, caller-key erasure, and single database ownership.
13. Adversarial review closed server-side selection fallback and aliasing
    hazards while the slice was being built. The final independent combined
    review found no remaining P0/P1/P2 issue in tenancy, privilege boundaries,
    bounds/completeness, deterministic selection, adapter conversion, key
    lifecycle, restart paging, database ownership, or startup cleanup.

Validation on pushed commit `40b00ac`:

```sh
go test ./... -count=1
go test -race \
  ./internal/control ./internal/server ./cmd/open-splunk-server \
  -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m --max-issues-per-linter=0 --max-same-issues=0
npm run lint
npm run typecheck
npm run test:frontend
make proto
git diff --check
```

All Go packages and the focused race suites passed. Vet, build, pinned
repository-wide lint with `0 issues`, frontend lint/typecheck, all 47
release/build tests, all 136 frontend tests, Buf format/lint, reproducible
protobuf generation, and the clean-tree check passed. Verification ran from
an isolated worktree anchored at the pushed commit so parallel follow-up GORM
work could not affect the evidence. The installed Node runtime was newer than
the repository's exact engine pin and emitted an engine warning, but every
frontend gate passed. `npm ci` continued to report the already-documented
three high-severity transitive Next.js/PostCSS/Sharp findings; no unsafe
breaking force-fix was applied.

This control-plane-only slice does not execute ClickHouse or change SPL
behavior, so it relies on the immediately preceding pinned all-ten-query
GradeThis inspection/compatibility gate rather than starting an unrelated
container. The next GORM work should keep each remaining store independently
reviewable: ingestion tokens, saved searches, and search history still use
direct `database/sql` at this checkpoint.

Next priorities:

1. Convert the remaining bounded SQLite control-plane stores to explicit GORM
   models without introducing `AutoMigrate` or weakening their transaction,
   cursor, scope, secret, lifecycle, and concurrency contracts.
2. Continue Phase 3 with collector fleet operations, reports/dashboards,
   per-index permissions/retention completion, HEC compatibility, and expanded
   RBAC/audit search.
3. Retain the lifecycle backlog: bounded ingestion-token tombstones, surfaced
   export-deletion failures, physical search-history cleanup, completion of
   deleting-index state, and a bounded WebSocket shutdown dependency.
4. Keep every SPL change behind unit, adversarial, and pinned ClickHouse
   compatibility evidence; this GORM work does not alter that contract.
5. Do not guess unresolved capacity, hardware, concurrent-load, Windows, or
   dashboard-scope product decisions when they materially affect a slice.

## Previous checkpoint: authenticated administrator search inspection

Date: 2026-07-28

Committed and pushed checkpoints:

- `25d809c` — the administrator-only protobuf inspection contract and
  reproducible generated Go and TypeScript messages;
- `97396af` — defensive validation of arbitrary inspection-service results
  before transport publication;
- `ea1ccdc` — the exact authenticated HTTP route, principal-derived scope,
  bounded atomic serialization, and feature advertisement;
- `a94b835` — production runtime wiring with an isolated Explainer lifecycle;
  and
- `d17566d` — the pinned all-ten-query GradeThis HTTP integration, detached
  comparison oracle, and CI gate.

This slice completes the Phase 2 administrator plan-inspection path from one
real completed job through the authenticated protobuf transport:

1. `search_inspection_api.proto` defines exact POST
   `/api/v1/search/jobs/inspect`. The request contains only
   `search_job_id`; tenant and owner selectors are deliberately absent. The
   response contains the detached logical stages, referenced fields and final
   output shape; the literal-free structured physical plan; generated SQL;
   raw structured EXPLAIN; and its diagnostic query ID. The compiler argument
   vector, result rows, owner IDs, snapshots, and mutable planner state are
   absent. Generated Go and TypeScript messages and the TypeScript barrel are
   committed from the protobuf source of truth.
2. The SQL and raw EXPLAIN are administrator-sensitive. SQL remains
   parameterized and arguments are not separately exposed, but ClickHouse may
   render any query-bound tenant, index, or predicate value into raw EXPLAIN.
   The contract and transport documentation therefore classify the entire
   response as privileged diagnostic data rather than describing only
   individual fields as sensitive.
3. `searchinspection.ValidateResult` distrusts an arbitrary service
   implementation before serialization. It requires canonical contiguous
   stages, supported operators, resolved and canonically ordered field sets,
   valid source coordinates and output-kind invariants, allowlisted physical
   nodes and indexes, consistent part/granule counters, and bounded UTF-8
   metadata. It reparses raw EXPLAIN through
   `queryexec.ParseExplainPlan` and requires exact equality with the supplied
   physical projection. Every failure wraps the fixed
   `ErrInspectionFailed` category without echoing SQL, EXPLAIN, IDs, fields,
   or dependency diagnostics.
4. The validation ceilings match the producing service and parser: 256 KiB
   SQL; one MiB EXPLAIN with at most 4,096 nonempty lines and 16 KiB per line;
   a 128-byte diagnostic query ID; 256 logical stages, 1,024 fields per stage,
   4,096 final fields, 16,384 projected field occurrences, and one MiB of
   logical strings; plus 4,096 physical nodes, 256 reads, 4,096 accumulated
   read headers, 4,096 indexes, 64 keys per index, 16 KiB per physical
   metadata string, and one MiB of physical strings.
5. The server registers the route only when a non-nil inspection service is
   configured, including typed-nil normalization, and handler construction
   fails closed if that administrative service lacks browser
   authentication. Exact path and method resolution precede Host/Origin
   checks and administrator authentication; authentication precedes content
   type, request admission, protobuf decoding, and service work. Wrong paths
   and methods remain 404/405 without invoking authentication, while rejected
   credentials do not read the body or consume admission capacity.
6. Tenant and owner scope come solely from the detached authenticated browser
   principal. Both middleware and the handler's defensive boundary require
   the administrator role and an exact match with the handler's configured
   tenant/owner; protobuf input and the ordinary fixed search scope cannot
   select either value. The route accepts one canonical, nonempty,
   control-free, unpadded UTF-8 job ID of at most 256 bytes, rejects unknown
   protobuf fields, caps the body at 16 KiB, and continues to reject the
   ordinary search-job `include_plan` and `include_generated_sql` flags.
7. Response construction shares the server's fail-fast serialization gate
   and is atomic under cancellation. The handler validates the complete
   service result, deeply converts every logical and physical field, checks
   the protobuf size before publication, and uses a bounded codec with an
   eight-MiB ceiling that releases its permit on every path. Bootstrap
   advertises `SERVER_FEATURE_PLAN_INSPECTION` exactly when the service is
   available.
8. Transport errors are stable and sanitized: malformed input is 400, missing
   jobs are 404, expired jobs are 410, not-ready or unavailable results are
   409, unsupported or execution-limited inspections are 422, inspection
   capacity is 429, cancellation/deadline is 408, and closed or unavailable
   storage is 503. Exhausted response-serialization capacity also fails fast
   with 503. Invalid dependency output, inspection failure, and unknown errors
   return a generic 500 without leaking diagnostic content.
9. Production startup now builds the native ClickHouse options once and uses
   the same validated connection-configuration snapshot for the shared query
   connection and `queryexec.NewExplainer`. The Explainer still owns two
   dedicated native lanes, so administrator diagnostics cannot reserve
   ordinary search/export connections. Construction validates borrowed
   search/compiler/options dependencies before opening lanes, rejects nil and
   typed-nil factories, and closes the Explainer if later service construction
   fails.
10. Runtime shutdown drains HTTP first, then closes the WebSocket, analysis,
    and export services before the inspection runtime. Inspection shutdown is
    concurrent-safe and idempotent: it first stops service admission, cancels
    and joins active inspections, and only then closes the isolated Explainer.
    Search jobs, the event store, and the shared ClickHouse connection remain
    alive until their borrowers release them.
11. The pinned ClickHouse integration freezes merges, stores the canonical
    GradeThis corpus plus deterministic poison cohorts, and validates the
    exact five-part layout before evidence is accepted. It executes all ten
    canonical searches through the real `searchjobs.Manager`, inspects each
    through the real service and isolated Explainer, and sends an authenticated
    bearer request through the exact HTTP route.
12. For every search, an independent protobuf mapper compares every response
    field with a completely detached recording of the exact internal result.
    The clone oracle mutates every nested logical and physical slice to prove
    independence. The integration also requires exactly two authoritative
    snapshot reads per job, ten route/service calls, and ten unique diagnostic
    query IDs. The digest-pinned GradeThis CI selector now includes
    `internal/server`, and its serial job budget is 20 minutes.
13. Adversarial review found and closed two P2 contract wording gaps around
    raw EXPLAIN values and whole-response privilege; one P1 omission that left
    the HTTP integration outside the pinned CI selector; one P2 shallow-copy
    oracle that could alias nested result slices; and one P2 CI timeout below
    the legal serial three-package budget. The selector, full deep-copy oracle
    with an independence race test, and 20-minute job cap close those findings.
    Independent final reviews found no remaining P0/P1/P2 issue in the
    contract, validator, route, runtime, integration, or CI boundary.

Validation on the pushed implementation:

```sh
go test ./... -count=1
go test -race \
  ./internal/searchinspection ./internal/server ./cmd/open-splunk-server \
  -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec ./internal/searchinspection ./internal/server \
    -run '^(TestGradeThis(CompatibilityV0_1|InspectionService)AgainstClickHouse|TestSearchInspectionRouteGradeThisAgainstClickHouse)$' \
    -count=1 -timeout=6m -p=1
go test -race ./internal/server \
  -run '^TestGradeThisInspectionRouteResultCloneIsIndependent$' -count=1
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
npm run typecheck
npm run test:frontend
make proto
git diff --check
```

The full Go suite, focused race suite, complete pinned three-package
GradeThis selector, and clone-oracle race test passed. The exact route
integration completed in 7.71 seconds; fixture cleanup left no ClickHouse
container behind. Vet, build, repository-wide Go lint with zero issues,
frontend lint/typecheck, all 47 release/build tests, all 136 frontend tests,
Buf format/lint plus reproducible protobuf generation, CI YAML parsing, and
`git diff --check` also passed.

Next priorities:

1. Keep this route administrator-only and retain its validator, lifecycle,
   corpus, and pinned-CI gates while continuing the remaining product phases.
2. Select the next bounded Phase 3 slice from per-index retention and
   permissions, index/app administration, ingestion-token and collector fleet
   operations, reports/dashboards, HEC compatibility, or expanded RBAC and
   audit search.
3. Continue Phase 4 with migration upgrades, backup/restore and disaster
   recovery, load shedding and fair/per-user scheduling, alerts and scheduled
   searches, and packaging/installers/upgrades/signed releases.
4. Retain the lifecycle backlog: bounded ingestion-token tombstones, surfaced
   export-deletion failures, physical search-history cleanup, completion of
   deleting-index state, and a bounded WebSocket shutdown dependency.
5. Do not guess unresolved capacity, hardware, concurrent-load, Windows, or
   dashboard-scope product decisions when they materially affect a slice.

## Previous checkpoint: administrator identity boundary

Date: 2026-07-28

Committed and pushed checkpoint:

- `8a66f10` — a fail-closed administrator browser identity and role boundary,
  secure operator-provisioned credential loading, protected control-plane
  routes, and real-process integration harness authentication.

This slice establishes the identity prerequisite for exposing the internal
search-inspection service without exposing raw SQL or ClickHouse plans to the
ordinary single-user search surface:

1. `internal/auth` now owns an immutable browser principal with exact
   tenant/owner scope and an explicit ordinary-user or administrator role.
   The fixed bearer authenticator accepts only bounded 32–512-byte token68
   credentials, stores only SHA-256, compares fixed-size digests in constant
   time, detaches all identity strings, honors cancellation, and redacts
   diagnostic formatting.
2. All five index-administration and all five ingestion-token routes are in
   one exact administrator-route set. Handler construction fails closed when
   either administrative service is present without a non-nil authenticator,
   including typed-nil implementations.
3. The HTTP order is now exact path/method, Host/Origin trust, administrator
   authentication, content type, request admission, protobuf decoding, and
   service work. Unknown routes and wrong methods therefore remain 404/405
   without invoking authentication; rejected credentials do not read request
   bodies, acquire the API gate, decode protobuf, or call a control service.
4. Authorization parsing requires one unambiguous case-insensitive `Bearer`
   field, rejects duplicate or case-variant header keys, multiple values,
   comma joining, extra whitespace, controls, non-ASCII token characters,
   malformed padding, short values, and oversized values before calling the
   authenticator. The reusable header is removed from a cloned authorized
   request before route middleware or handlers receive it.
5. Missing, malformed, and incorrect credentials return a generic 401 plus
   `WWW-Authenticate`; authenticated non-administrators and exact
   tenant/owner mismatches return 403; cancellation/deadline returns 408; and
   backend or corrupt-principal failures return a generic 503. All use the
   no-store JSON envelope and disclose neither credentials nor backend errors.
6. A valid detached administrator principal is stored in request context for
   the next inspection route. Ordinary search, saved-search, history, export,
   WebSocket, bootstrap, health, and static routes remain outside this
   administrator gate and do not invoke the authenticator.
7. Normal server startup now requires
   `-administrator-token-file`. The existing regular file must be owned by the
   effective server user, have exactly one hard link, use exactly 0400 or 0600
   permissions, contain one valid token with at most one LF or CRLF terminator,
   and have no special bits, final-component symlink, or macOS extended ACL.
   Missing or unsafe files fail startup; the server never creates or chmods
   the credential.
8. Loading uses pre-open `Lstat`, `O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC`, descriptor
   identity and metadata checks, a 514-byte bounded read, and post-read
   descriptor/path checks. On macOS, descriptor-based `fgetattrlist` checks
   before and after the read reject direct and inherited ACL entries that can
   grant access despite mode 0600. Plaintext buffers are cleared immediately
   after constructing the digest-only authenticator.
9. Browser HTTP is loopback-only until the server supports HTTPS. The former
   insecure trusted-network flag is retained only as a deprecated compatibility
   flag and cannot bypass this boundary. Remote browser exposure requires a
   later HTTPS-aware listener or trusted-proxy design that preserves the exact
   Host/Origin boundary.
10. The sustained-load and vertical real-process harnesses now provision
    independent CSPRNG administrator tokens in 0600 files, pass only file
    paths to server processes, attach credentials only to administrative
    protobuf calls, reuse them safely across server restarts, and assert that
    process logs never contain them.
11. Adversarial review initially found one P2 macOS ACL bypass: an
    `everyone allow read` ACL can coexist with reported mode 0600. That finding
    is closed by the descriptor ACL checks and direct/inherited regression
    fixtures. Two independent rereviews found no remaining P0/P1/P2 issue in
    the identity, route, runtime-file, ACL, or integration boundaries.

Operator provisioning example:

```sh
umask 077
openssl rand -base64 48 > administrator.token
chmod 0600 administrator.token
# macOS only, if the containing directory applies inherited ACLs:
chmod -N administrator.token

open-splunk-server \
  -administrator-token-file=administrator.token
```

The file is loaded once. Rotation uses a newly provisioned safe file, atomic
replacement, and a server restart. `-verify-embedded-release` still exits
before credential loading.

Validation on the pushed implementation:

```sh
go test ./... -count=1
go test -race \
  ./internal/auth ./internal/server ./cmd/open-splunk-server -count=1
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -o /tmp/open-splunk-server-linux-amd64 \
  ./cmd/open-splunk-server
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
git diff --check
```

The full Go suite passed, the focused race suite passed, the Linux release
target cross-built without CGO, and repository-wide Go lint reported zero
issues.

Follow-up status:

The inspection contract, validator, exact administrator route, isolated
runtime lifecycle, and pinned all-ten-query HTTP integration identified by
this checkpoint's former resume list are complete in `25d809c` through
`d17566d` and documented in the latest checkpoint above.

## Previous checkpoint: measured GradeThis plans and schema decision

Date: 2026-07-28

Committed and pushed checkpoints:

- `1961f9d` — deterministic same-scope/out-of-time, foreign-tenant,
  foreign-index, and adjacent-partition load cohorts;
- `e409f89` — exact ten-query results plus bounded native ClickHouse scan
  evidence over that multipart load; and
- `dde9092` — the same corpus executed through real completed search jobs and
  the internal inspection service, with exact physical-plan contracts and a
  pinned CI gate.

The related control-plane modernization is also complete at `cddeb7a`: index
persistence uses GORM over the exact existing modernc SQLite connection while
SQL migrations remain authoritative and `AutoMigrate` remains disabled.

This slice closes the Phase 2 storage-tuning measurement loop without changing
the event schema:

1. The exact canonical twenty-event corpus traverses the production collector
   decoder and ClickHouse Store once. Four independent 7,500-row poison
   cohorts are then inserted for same-scope/out-of-time, foreign-tenant,
   foreign-index, and adjacent-partition isolation.
2. Merges are stopped before insertion. The pinned image must expose exactly
   five level-zero parts: June has one part, 7,500 rows, and two marks; July
   has four parts, 22,520 rows, and eight marks. Part, row, mark, partition,
   and merge-level drift fails the fixture before query evidence is accepted.
3. All ten exact GradeThis result, schema, ordering, aggregation, and paging
   oracles remain unchanged. Two manual repetitions produced the same native
   progress values shown below. These values are observational, not latency or
   progress goldens; the executable contract requires nonzero evidence below
   the ceilings of 100 scanned rows and 4 MiB scanned bytes:

   ```text
   follow-trace                   40 rows    9,627 bytes
   errors-and-warnings            40 rows   20,151 bytes
   raw-error-fragment             20 rows   16,566 bytes
   severity-counts                20 rows      800 bytes
   frequent-errors                20 rows    8,948 bytes
   volume-by-severity             86 rows    1,721 bytes
   server-errors-by-route         47 rows    9,519 bytes
   responses-by-route-and-status  20 rows    8,875 bytes
   slow-routes                    20 rows    8,875 bytes
   top-messages                   20 rows    1,232 bytes
   ```

4. Each exact SPL source now runs through the real query executor and Manager
   to a completed job. The inspection Service reads that same authoritative
   snapshot before and after EXPLAIN, compiles exactly once, and returns a
   uniquely identified structured PLAN. No hand-built snapshot or synthetic
   target row can make the inspection proof diverge from the semantic corpus.
5. Every search has exactly one MergeTree read. Exact safe physical columns
   are pinned per search. Every plan reports MinMax pruning from five parts and
   granules to three, partition evidence of three to three, and primary-key
   pruning from three to one. `idx_visibility_seq` is selected after that
   pruning but remains one-to-one. Only `server-errors-by-route` additionally
   selects `idx_field_names`, also one-to-one. No search selects
   `idx_trace_id` or `idx_raw_text`; unexpected indexes, keys, columns, or
   counters fail with fixed metadata-free errors.
6. The pinned GradeThis CI job now runs both the exact query-execution corpus
   and the search-inspection physical-plan corpus, serially, against the same
   digest-pinned ClickHouse image. CI therefore exercises the physical
   contract alongside the semantic half.
7. Independent adversarial reviews found and closed disconnected synthetic
   inspection targets, ignored partition/column/skip evidence, contradictory
   successful Store results containing rejected events, and missing CI
   coverage. Final rereviews reported no remaining P0/P1/P2 finding.

Schema decision:

- Do **not** add a schema, ordering-key, projection, promoted-field, or
  skip-index migration for this corpus.
- The existing ordering key
  `(tenant_id, index_name, toStartOfHour(event_time), event_time, event_id)`
  already reduces every measured query to one part/granule after time and
  scope pruning, with exact results and very small scans.
- The existing trace/raw indexes are not selected by the current compiler
  predicates, and forcing a different predicate solely to activate them would
  risk changing SPL equality or case-insensitive free-text semantics.
  `idx_field_names` is selected only once and performs no additional pruning
  after the primary key. There is therefore no measured benefit that
  outweighs migration and write-amplification risk.
- Keep the executable corpus as the regression gate. Reconsider physical
  storage only when a representative larger corpus or production measurements
  demonstrate a concrete reduction while preserving exact SPL behavior and
  chronological ordering.

Validation on the pushed implementation:

```sh
go test ./... -count=1
go test -race \
  ./internal/queryexec ./internal/searchinspection \
  ./internal/testsupport/gradethiscorpus -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec ./internal/searchinspection \
    -run '^TestGradeThis(CompatibilityV0_1|InspectionService)AgainstClickHouse$' \
    -count=2 -timeout=12m -p=1 -v
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
git diff --check
```

The two pinned integrations produced identical evidence in both repetitions.
Repository-wide Go lint reported zero issues.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `dde9092`.
2. Keep `searchinspection` internal until an authenticated administrator
   identity and role boundary exists.
3. Add that administrator boundary as a separate test-first checkpoint. Do
   not couple identity establishment to raw plan exposure.
4. Then add a dedicated administrator-only inspection route with bounded
   protobuf projections, principal-derived tenant/owner scope, isolated
   Explainer lifecycle ownership, and a pinned route integration.
5. Continue Phase 3/4 HEC, reports/dashboards, RBAC expansion, audit search,
   alerts, scheduled searches, and packaging without weakening the exact
   GradeThis/compiler/lifecycle gates.

## Previous checkpoint: bounded structured physical plans

Date: 2026-07-28

Committed and pushed checkpoint:

- `1a3d9d0` — fixed structured ClickHouse PLAN output, a bounded safe physical
  projection, and pinned compatibility/adversarial coverage.

This test-first slice turns the internal inspection primitive into measurable,
machine-readable physical evidence without weakening its administrator-only
boundary:

1. The Explainer now issues only fixed
   `EXPLAIN PLAN json = 1, description = 1, indexes = 1, actions = 0,
   header = 1` around the unchanged sealed compiler SQL. Its outer settings
   retain the exact execution cap and pin condition-cache, skip-index-on-read,
   and full-text-index behavior for reproducible physical analysis.
2. ClickHouse must return exactly one non-null String row containing one JSON
   PLAN envelope. The row is normalized into at most 4,096 nonempty,
   control-free UTF-8 lines, with 16 KiB per line and one MiB total. Multiple
   rows, plaintext plans, truncated JSON, trailing values, Actions, malformed
   structure, cancellation, and driver errors fail atomically before
   publication.
3. `queryexec.ParseExplainPlan` returns a detached projection of bounded node
   types, physical MergeTree read columns, index types/names/keys, and
   initial/selected part and granule counts. It deliberately omits
   descriptions, conditions, node IDs, column types, arguments, and other
   literal-bearing fields.
4. A streaming token preflight enforces the one-envelope shape, 4,096 total
   nodes, 1,024 children per node, 4,096 accumulated read headers and indexes,
   and 64 keys per index before typed collections are materialized. Every field
   consumed by the typed projection is matched with the same case folding as
   `encoding/json`; ambiguous exact or case-variant duplicates are rejected.
   The subsequent typed decode and iterative traversal are linear under the
   existing one-MiB input ceiling and cap projected MergeTree reads at 256.
5. The pinned ClickHouse suite proves all legitimate optimizer shapes used by
   supported compiler output: `ReadNothing` for an empty MergeTree, a
   `ReadFromMergeTree` with no Indexes for a constant-false predicate, a legal
   greater-than-64-node table/sort pipeline, populated reads with MinMax and
   PrimaryKey evidence, and chronological compiler queries with inner
   settings.
6. `searchinspection.Result` now publishes the safe `PhysicalPlan` beside the
   administrator-sensitive raw PLAN. The service reruns the complete public
   parser at its arbitrary Explainer boundary and still requires an unchanged
   postflight execution snapshot before returning either representation.
7. Three independent adversarial passes found and closed acceptance of
   non-JSON/multi-row results, cancellation after parsing, rejection of valid
   empty/index-free plans, quadratic repeated-subtree parsing, an artificial
   depth cap below legal compiler output, decode-time wide-array allocation
   amplification, and ambiguous duplicate projected fields. Final rereviews
   reported no remaining P0/P1/P2 finding.

Validation on the pushed implementation:

```sh
go test ./... -count=1
go test -race ./internal/queryexec ./internal/searchinspection -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec \
    -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1 -timeout=6m
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/searchinspection \
    -run '^TestServiceAgainstClickHouse$' -count=1 -timeout=6m
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
git diff --check
```

The final pinned query-executor and inspection integrations passed in 13.831
and 6.540 seconds. Repository-wide Go lint reported zero issues.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `1a3d9d0`.
2. Reuse `internal/testsupport/gradethiscorpus` as the exact ten-query semantic
   oracle; do not create or weaken a parallel corpus.
3. Add deterministic same-tenant/index, foreign-tenant, foreign-index,
   adjacent-partition, and out-of-time load cohorts, then capture real
   execution progress and structured physical evidence without latency
   goldens.
4. Require the existing exact result oracle to remain unchanged while proving
   bounded scanned rows/bytes, selected parts/granules, physical columns, and
   actual index use for each query.
5. Add a schema/order/projection/index/promoted-field migration only if the
   measured corpus demonstrates a concrete improvement without changing SPL
   semantics or chronological ordering. Otherwise record the evidence-backed
   rejection and continue the remaining architecture phases.

## Previous checkpoint: bounded internal search inspection

Date: 2026-07-28

Committed and pushed checkpoint:

- `e43ea10` — access-scoped completed-search plan inspection with a detached
  logical projection and bounded ClickHouse EXPLAIN.

This test-first slice completes the internal administrator-diagnostic layer
without exposing sensitive plans through the unauthenticated browser API:

1. `searchinspection.Service` accepts only a canonical tenant/owner scope and
   search-job ID, performs a metadata-only lookup of one completed, unexpired
   execution snapshot, and never acquires or extends a retained-result lease.
   A second authoritative lookup after EXPLAIN must equal the exact first
   snapshot, so expiry, tombstone cleanup, or execution-scope drift prevents
   atomic publication.
2. The service rebuilds the retained search with its exact effective indexes,
   absolute time range, search anchor/timezone, `_indextime` cutoff, and
   visibility cutoff. It compiles exactly once and passes the resulting sealed
   `CompiledQuery`, including its argument slice, unchanged to `Explainer`.
3. The returned logical plan is a detached projection containing only
   canonical operator names, sorted logical read/write field names, bounded
   final shape, and exact half-open UTF-8 source coordinates. SPL text,
   predicates, literal values, regexes, JSON paths, labels, arguments,
   snapshots, result rows, and mutable logical-plan pointers are omitted.
4. Projection is capped at 256 stages, 1,024 fields per stage, 4,096 final
   static fields, 16,384 total projected field occurrences, one MiB of
   projected strings, and the parser's 16 KiB source ceiling. Generated SQL
   must retain its compiler seal and remain within 256 KiB. EXPLAIN output is
   revalidated against `queryexec`'s canonical one-MiB/4,096-row contract at
   the arbitrary dependency boundary.
5. Admission is fail-fast at two concurrent inspections before snapshot,
   planning, compilation, or timer work. Every admitted operation has an exact
   ten-second deadline. `Close` synchronously stops admission, cancels every
   registered operation, joins dependency calls, is concurrent/idempotent, and
   can be retried after a caller's close deadline expires.
6. Search-job execution snapshots now own canonical instant-aware equality,
   and the manager and inspection service share hard 256-entry index-scope and
   256-byte job-ID ceilings. Custom job-ID generators must produce nonempty,
   unpadded, control-free UTF-8 IDs, preventing the manager from creating a job
   that inspection cannot address.
7. `queryexec.ValidateExplainResult` is the reusable buffered-result contract.
   The concrete Explainer continues to enforce rows, lines, UTF-8, controls,
   and cumulative bytes while streaming; inspection performs one independent
   scan for arbitrary Explainer implementations rather than redundantly
   rescanning the concrete result twice.
8. Planner field resolution now rejects the complete Unicode control category,
   including C1 controls, so every accepted field name can be projected without
   a later diagnostic-only incompatibility.
9. The package deliberately has no protobuf, HTTP, server-runtime,
   feature-advertisement, or UI seam. Generated SQL and ClickHouse plans can
   reveal schema detail or rendered bind values and remain internal until a
   genuine administrator identity/role boundary exists.
10. Three-way correctness, confidentiality/security, and efficiency/reuse
    review found and closed snapshot-field comparison drift, duplicated
    operator metadata and EXPLAIN validation, a manager/inspection index-scope
    mismatch, noncanonical generated job IDs, redundant postflight rescans,
    projection ceilings that rejected valid accumulated outputs, and
    Close/cancellation publication races. Final targeted rereviews reported no
    remaining finding.

Validation on the pushed implementation:

```sh
go test ./... -count=1
go test -race \
  ./internal/searchinspection ./internal/plan ./internal/clickhouse \
  ./internal/searchjobs -count=1
go test -race ./internal/queryexec \
  -run '^Test(ValidateExplainResult|Explainer(Buffers|Accepts|RejectsDriver|RequiresExact|BoundsRendered|Detaches|PreservesExact|RejectsInvalidState|RequiresUnchanged|RejectsMalformed|EnforcesText|Sanitizes|Cancellation|HasTwo|CloseCancels|CloseSanitizes|ClosedState)|ExplainTestFixtures)' \
  -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/searchinspection \
    -run '^TestServiceAgainstClickHouse$' -count=1 -timeout=6m
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
git diff --check
```

The exact pinned ClickHouse service integration passed in 7.052 seconds.
Repository-wide Go lint reported zero issues.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `e43ea10`.
2. Keep inspection internal. Do not add protobuf, HTTP, runtime advertisement,
   or UI wiring before the Phase 3 administrator identity/role boundary.
3. Exercise the internal service against the pinned ten-query/load corpus and
   retain bounded evidence for plan shape, scanned rows/bytes, partitions,
   ordering-key use, text indexes, and promoted versus dynamic fields.
4. Add a schema, ordering, projection, skip-index, or promoted-field migration
   only when that evidence demonstrates a concrete improvement without
   changing SPL results or chronological ordering.
5. Finish the Phase 3 administrator identity boundary before exposing
   inspection, then continue through HEC, alerts, scheduled search, and
   packaging without weakening current compiler, lifecycle, and corpus
   regressions.

## Previous checkpoint: sealed and bounded ClickHouse EXPLAIN

Date: 2026-07-27

Committed and pushed checkpoint:

- `27c0781` — compiler-sealed SQL and a bounded, isolated ClickHouse EXPLAIN
  primitive.

This test-first slice completes the transport-independent foundation for
administrator search inspection without exposing it through the currently
unauthenticated browser API:

1. `clickhouse.Compiler.Compile` now places a private SHA-256 seal over the
   generated SQL. `CompiledQuery.HasValidSQLSeal` proves that query structure
   is the unchanged output of the main compiler; constructing or changing
   public `CompiledQuery` fields cannot forge that provenance.
2. `queryexec.NewExplainer` owns two dedicated native ClickHouse lanes, each
   with a one-connection pool and an independent deadline coordinator.
   Ordinary search/export connections are never reserved for diagnostics.
   Admission is fail-fast before SQL hashing, argument inspection, timer
   creation, or lifecycle registration, so a burst cannot create unbounded
   waiter state.
3. The only accepted statement shape is
   `EXPLAIN SELECT * FROM (<sealed compiler SQL>) AS __os_explain_input
   SETTINGS max_execution_time = N`. The fixed outer setting defeats the
   pinned driver's looser deadline-derived protocol setting and safely wraps
   compiler queries that already contain chronological `UNION ALL` relations
   and inner settings.
4. Explain execution is capped at two concurrent calls, ten seconds, one
   thread, 128 MiB memory, five million input rows, one GiB input bytes, 4,096
   result rows/groups, 16 KiB per line, one MiB result text, 256 KiB raw
   compiler SQL, and one MiB after conservative parameter rendering. Query
   cache use is disabled and every overflow mode throws.
5. Argument admission accepts only the exact scalar inventory emitted by the
   compiler, checks exact placeholder cardinality, rejects formatters,
   `driver.Valuer`, named scalars, pointers, collections, nils, and typed nils,
   and passes one detached snapshot to the driver. Neither arguments nor
   driver/query detail are returned or logged.
6. The custom native transport overlays the exact earlier request deadline on
   driver read/write deadlines before the initial native query write, expires
   the socket immediately on cancellation, restores driver deadlines on
   release, preserves the stale-socket `syscall.Conn` probe, and performs TLS
   itself because clickhouse-go bypasses TLS when a custom dialer is present.
   Failed deadline restoration closes the socket rather than returning a
   poisoned connection to the lane.
7. Construction rejects HTTP, custom dialers/strategies/settings, JWT
   callbacks, invalid connection strategies, unsafe or callback-bearing TLS,
   invalid endpoints, and plaintext non-loopback addresses. TLS configuration
   is detached once per lane and reused immutably rather than deep-cloned on
   every reconnect. Fractional configured execution limits retain their exact
   Go deadline while the ClickHouse setting is conservatively rounded up.
8. Results are completely buffered and validated before publication: exactly
   one non-null `explain String` column, valid bounded UTF-8 text without
   controls, a separate bounded diagnostic query ID, and atomic failure on
   cancellation, stream errors, malformed schema, or limit violations.
   `Close` is concurrent/idempotent, cancels and joins admitted calls, closes
   both lanes, and returns the closed state rather than capacity once shutdown
   has begun.
9. Independent adversarial and reuse/quality/efficiency reviews found and
   closed abandoned native-handshake sockets, off-loopback plaintext, the
   socket/context deadline publication race, deadline-restore poisoning,
   unbounded pre-admission waiter work, fractional timeout rounding, repeated
   TLS/settings cloning, and Close-versus-capacity error precedence. No
   remaining P0/P1/P2 finding was reported on the committed snapshot.

Validation on the pushed checkpoint:

```sh
go test ./... -count=1
go test -race ./internal/queryexec ./internal/clickhouse -count=1
go test ./internal/queryexec \
  -run 'TestExplainerBoundsInitialNativeQueryWriteAndReusesLanes|TestExplainerClosesFailedHandshakeAndRetriesAnotherAddress|TestExplainerHasTwoIsolatedRequestLanes|TestSanitizeExplainQueryErrorHandlesSocketContextDeadlineRace' \
  -count=20
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/queryexec \
    -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1 -timeout=6m
go vet ./...
go build ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
npm run lint
git diff --check
```

The pinned ClickHouse suite passed in 17.957 seconds. Repository-wide Go lint
reported zero issues.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `27c0781`.
2. Build an internal-only `internal/searchinspection` service. Resolve an
   exact tenant/owner completed, unexpired execution snapshot without
   acquiring a result lease or extending retention; build its detached
   execution plan, compile it once, and pass that exact sealed
   `CompiledQuery` unchanged to `Explainer`.
3. Return only a detached safe logical-plan projection, generated SQL,
   EXPLAIN text, and diagnostic query ID. Never expose compiler arguments,
   mutable SPL/plan pointers, execution snapshots, retained results, or
   storage/driver errors. Give the service its own fail-fast concurrency
   bound, exact cancellation, shutdown ownership, output limits, and sanitized
   stable errors.
4. Keep the service internal. Do not add protobuf, HTTP, runtime feature
   advertisement, or UI wiring until a genuine administrator identity/role
   boundary exists; ClickHouse plans can inline bound search values.
5. Use the internal service in the pinned ten-query/load corpus to establish
   plan, scan, partition, ordering-key, text-index, and promoted-field
   baselines. Add a schema/index migration only when that evidence supports
   it.
6. Then finish the Phase 3 administrator identity boundary before exposing
   inspection, and continue through HEC, alerts, scheduled search, and
   packaging without weakening the existing SPL/compiler regressions.

## Previous checkpoint: bounded search suggestions and connected time semantics

Date: 2026-07-27

Committed and pushed checkpoints:

- `cd9d012` — bounded SPL completion context;
- `9115465` — time/index-scoped ClickHouse field suggestions;
- `2a82932` — side-effect-free cross-layer suggestion composition;
- `076ff43` — exact protobuf search-suggestions route and runtime wiring; and
- `7eba237` — backend/browser time-picker semantic alignment.

These test-first slices complete two Phase 2 compatibility gaps:

1. `POST /api/v1/search/suggestions` now validates the same detached tenant,
   app, authorized index scope, requested scope, and half-open time range used
   by search admission without creating or retaining a job. It composes
   bounded static SPL completions with ClickHouse field candidates selected
   only from the authorized index/time window.
2. Completion context preserves the active base-search or pipeline-`search`
   predicate prefix, caps parser/editor work and candidate/result bytes, and
   rejects malformed cursor, metadata, dependency, UTF-8, and field-path
   states before publication. The ClickHouse query bounds both eligible and
   ineligible metadata work, preserves ASCII-fold/exact ordering, validates
   the full fetched metadata row, and permits valid cross-row parent/child
   field names while rejecting durable within-row ancestor collisions.
3. The route is exact POST-only protobuf, shares search authorization, copies
   all scopes before concurrent use, maps only sanitized errors, participates
   in runtime shutdown, and returns a defensively validated bounded response.
   A composed-handler regression proves static completion creates no search
   job and performs no storage lookup.
4. The executable time subset now includes `@d`, bounded `-Nd@d`, and exact
   earliest-only `0`. Day offsets apply before local-midnight snapping,
   repeated midnight selects its first occurrence, and skipped civil
   dates/midnights fail closed. `0` resolves to
   `1900-01-01T00:00:00Z`, never the Unix epoch.
5. Browser validation accepts every published preset and the backend's
   zero-padded offset grammar, measures authored bytes before normalization,
   matches Go's Unicode trim set, preserves relative/calendar intent and the
   effective IANA timezone, and avoids false rejection when a millisecond
   client clock cannot prove a nanosecond server interval. The server remains
   authoritative for mixed anchor/timezone-dependent ordering and tzdb lookup.
6. Unit, race, frontend, TypeScript, Go/frontend lint, server-adapter, and
   pinned ClickHouse suggestion coverage includes maximum-width metadata,
   long ineligible prefixes, Unicode/escape parity, poison rows outside the
   returned prefix, DST 23/25-hour days, a repeated midnight, a skipped date,
   first-invalid grammar/bounds, and pre-1970 all-time resolution.
7. Independent adversarial review found and closed the repeated-midnight fold,
   raw-byte/zero-padding mismatch, lossy `now`/elapsed nanosecond comparisons,
   Go/JavaScript trim mismatch, browser/backend tzdb mismatch, metadata
   ancestor-collision, prefix-poison, and work-before-limit cases. No P0/P1/P2
   finding remains on `7eba237`.

Validation on the pushed time checkpoint:

```sh
go test ./... -count=1
go test -race ./internal/searchtime ./internal/server -count=1
npm run test:frontend
npm run typecheck
npm run lint
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m ./...
git diff --check
```

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `7eba237`.
2. Finish the transport-independent, read-only, bounded ClickHouse `EXPLAIN`
   primitive and its pinned integration coverage. Do not expose it through an
   HTTP/protobuf route while the server has no administrator authentication
   boundary; plans may contain rendered bound values.
3. Build an administrator-facing inspection service over completed, unexpired,
   access-scoped execution snapshots and safe logical-plan projection. Keep
   generated SQL, arguments, and EXPLAIN text out of ordinary job/history
   responses.
4. Add an authenticated administrator route only after the Phase 3 identity
   and role boundary exists, then tune schema/order/text/materialized fields
   against the real query and load corpus.
5. Keep the mixed binary-aggregate, suggestion poison/bounds, and time
   transition regressions when changing adjacent compiler or search code.
6. Keep `eventstats` behind the stable aggregate library. Finish Phase 2 before
   proceeding through Phase 3/4 HEC, alerts, scheduled search, and packaging.

## Previous checkpoint: bounded side-effect-free SPL validation

Date: 2026-07-27

Search-validation, ClickHouse-regression, and adversarial-hardening checkpoint
(committed and pushed):
`1919e2b`

This test-first slice implements
`POST /api/v1/search/validate` without creating a search job:

1. The exact POST-only protobuf route accepts `ValidateSearchRequest` and
   returns `ValidateSearchResponse`. It shares search-definition resolution
   with creation: SPL must be nonblank and NUL-free, earliest/latest must
   resolve to a valid half-open time range, and the requested indexes must
   normalize to active, search-enabled catalog indexes. Search presentation
   metadata remains unsupported.
2. The handler passes the exact detached tenant, normalized authorized scope,
   requested scope, and resolved range to the manager. The manager shares
   asynchronous-job parsing, logical planning, authorization, and ClickHouse
   compilation helpers, including one immutable clock anchor for `now()` and
   other time-dependent planning. Validation uses a local zero visibility
   cutoff only to compile the same scoped plan; it never asks storage for a
   visibility snapshot.
3. A valid HTTP 200 response includes trimmed normalized SPL, sorted unique
   effective indexes, sorted unique logical read fields, and the predicted
   Events, Statistics, or Time Series result kind. Read fields are derived
   from the accepted logical plan rather than final result columns: write-only
   eval, rename, and aggregate outputs are omitted unless a later operator
   reads them.
4. SPL parse, planning, compiler, and in-query index-scope failures are
   successful HTTP 200 validation results with `valid = false` and an error
   diagnostic. Invalid results expose no normalized SPL, indexes, fields, or
   result kind. Definition, time, requested-scope authorization, request-size,
   deadline, capacity, service, and internal failures remain non-2xx transport
   errors with sensitive details removed.
5. Parse, planning, and compiler diagnostics retain exact half-open source
   ranges into the original UTF-8 SPL. Byte offsets are zero-based; lines and
   Unicode-scalar columns are one-based. The new shared, fail-closed
   diagnostic projection carries both start and end coordinates consistently
   through validation HTTP, retained search history/jobs, and WebSocket
   projection.
6. Validation creates no ID or job, admits nothing to the job queue, retains
   no manager metadata, writes no journal/history record, takes no storage
   snapshot, executes no SQL, and exposes neither generated SQL nor compiler
   arguments. SQL is constructed only transiently by compilation to prove
   that the accepted SPL is executable by the pinned backend.
7. Request, scope, parser token/depth, plan analysis, and compiler work bounds
   remain authoritative. Logical read analysis independently fails closed on
   typed nil, unknown or malformed nodes, empty fields, excess depth, and
   excess nodes. Synchronous validations have a separate `MaxConcurrent`
   gate, fail fast instead of queuing when full, honor caller and manager
   cancellation, and participate in shutdown ownership without leaking an
   operation or gate slot.
8. Result-shape classification is now one source-ordered SPL helper shared by
   validation and HTTP/WebSocket projection. This prevents a later
   transformation such as `timechart | table` from being classified
   differently by the validation response and executed result transport.
   Scope slices are copied only after admission, and safe response metadata is
   detached before publication.
9. The required pinned query-executor gate exposed a pre-existing binary
   `_raw` aggregate regression introduced by Unicode text eligibility:
   chronological `earliest`/`latest` and scalar String `min`/`max` had reused
   the text-only input needed by `values`/`list`, turning selected binary bytes
   into null. The compiler now uses byte-preserving scalar/candidate aliases
   for chronological and extrema aggregates and a distinct UTF-8-eligible
   alias for `values`/`list`, including when both classes occur in one `stats`
   command. Chronological and extrema results again publish `Bytes`;
   binary-only `values`/`list` remains empty/logically absent as required by
   the published text-aggregate contract.

Validation completed on the exact pushed tree:

```sh
go test ./... -count=1
go test -race \
  ./internal/clickhouse ./internal/queryexec ./internal/spl ./internal/plan \
  ./internal/searchjobproto ./internal/searchjobs ./internal/searchhistory \
  ./internal/searchws ./internal/server -count=1
golangci-lint run --timeout=5m
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
    -run '^TestExecutorAndManagerAgainstClickHouse/(stats_earliest_and_latest_preserve_binary_raw_through_manager|stats_values_retains_typed_multivalue_transport)$' \
    -count=1 -timeout=5m -v
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
    -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1 -timeout=5m
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=5m
git diff --check
```

The ordinary suite passed in 22.36 seconds, the targeted race suite in
19.42 seconds, and repository-wide Go lint reported zero issues. The targeted
binary aggregate regression passed in 7.545 seconds; the full pinned
ClickHouse `26.3.17.4` query-executor and Store suites passed in 13.138 and
53.521 seconds respectively.

Two independent adversarial reviewers audited correctness and efficiency.
Their findings were applied before the final gates: make the shutdown
concurrency fixture deadline-bounded and fail-safe, share fail-closed
diagnostic range validation/projection, avoid premature input cloning, and
separate binary-preserving extrema/chronological inputs from text-only
multivalue inputs. Reuse, quality, and efficiency simplify passes found and
applied the shared result classifier and analysis/projection consolidation.
No review blocker remains on `1919e2b`.

Resume steps recorded at this checkpoint:

1. Confirm `main` and `origin/main` are at `1919e2b`, following the
   concatenation implementation/publication commits `875ddad`, `bc80006`, and
   `0b3f073`.
2. Add bounded `/search/suggestions` support and index/time-scoped field
   completion on top of the shared validation/admission contract; static
   browser completions are not the Phase 2 field-autocomplete service.
3. Preserve the connected time-picker contract now pinned at the protobuf and
   browser boundaries: Today uses `@d`/`now`, Yesterday uses
   `-1d@d`/`@d`, and All time uses earliest-only `0`/`now`. Day snaps apply
   the offset before local-midnight snapping, choose the first occurrence of a
   repeated midnight, and fail closed for skipped civil dates or midnights.
   Keep browser validation, IANA timezone handoff, retained authored intent,
   and the backend's `1900-01-01` earliest-data sentinel synchronized.
4. Complete administrator-only generated-SQL/ClickHouse-`EXPLAIN` inspection
   and schema, ordering-key, text-index, and materialized-field tuning against
   the real query and load corpus. Do not expose plans or sensitive bound
   values to ordinary users.
5. Keep the new mixed binary-aggregate regressions whenever changing
   chronological, extrema, `values`, or `list` lowering.
6. Keep `eventstats` behind the stable aggregate library. Finish Phase 2 before
   proceeding through the plan's Phase 3/4 index administration, RBAC, HEC,
   alerts, scheduled-search, and packaging work.

## Previous checkpoint: bounded SPL1 period concatenation

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`875ddad`

ClickHouse execution and pinned-integration checkpoint (committed and pushed):
`bc80006`

This test-first slice implements bounded `left . right [ . value ...]`
concatenation:

1. The lexer accepts the official `first_name." ".last_name` spelling and
   fully separated bare-operand forms such as `first . last`. It deliberately
   preserves contiguous dotted event paths, escaped paths, decimal literals,
   quoted dots, and literal periods in base and pipeline `search`. Half-spaced
   ambiguous bare forms are rejected, `concat(...)` remains unsupported, and
   SPL2 `+` is not added to the SPL1 grammar.
2. A chain becomes one flat, source-ordered scalar node with two through 32
   operands. One query may contain at most 256 operand occurrences, including
   independently charged nested expressions. The parser and planner retain
   complete source ranges and reject direct Boolean results and resource
   overflow. Planner and compiler trust boundaries independently recheck
   forged arity, typed nil, unsupported enums, operand type, and query
   budgets before execution, including predicate-contained expressions.
3. Concatenation binds tighter than comparison and looser than a scalar
   primary or function call. It composes inside and around supported scalar
   functions and in `where`, conditional predicates and values, and
   `count(eval(...))`, while the existing `NOT`, `AND`, and `OR` precedence
   remains unchanged.
4. Fixed String operands are identities and fixed numbers use their typed
   ClickHouse spelling without a `Float64` round trip. Empty String is a value.
   Missing, explicit-null, or statically null input makes the complete result
   null. Direct fixed Boolean, canonical time, and multivalue operands are
   rejected; callers must use an explicitly supported conversion such as
   `tostring` for Boolean output.
5. Dynamic runtime String and physical numeric variants are accepted.
   Unrestricted Dynamic input also accepts a validated, at-most-4-KiB
   `decimal/v1` envelope. Dynamic Boolean, null, missing, array, object, other
   tag, and malformed or oversized Decimal input produces null. Proven text
   and numeric Dynamic domains omit irrelevant dispatch.
6. Decimal spelling is pinned rather than conflated: tagged
   `decimal/v1` retains its validated payload exactly, including trailing
   fractional zeroes, while physical Decimal uses ClickHouse `toString`;
   pinned integration asserts that `toDecimal64('12.3400', 4)` renders
   `"12.34"` and tagged `"123.4500"` remains `"123.4500"`.
7. Concatenation preserves byte provenance. Binary-declared `_raw` can be
   copied between String delimiters byte-for-byte, but the combined provenance
   makes a later UTF-8 consumer return null. Ordinary typed Strings remain
   text-eligible, and duplicate provenance guards are combined once.
8. The compiler emits one scalar parameterized ClickHouse `concat(...)`,
   retains arguments in source order, binds every calculated or Dynamic
   operand once, and never uses `ARRAY JOIN`. One occurrence has a conservative
   4 MiB output ceiling, all occurrences share a 16 MiB per-row ceiling, each
   occurrence has a 64 KiB generated-SQL ceiling, and the whole-query 256 KiB
   ceiling remains independent.
9. Unrestricted Dynamic lexical conversion reserves 4 KiB per occurrence
   against a 64 KiB query-wide Decimal-parsing budget now shared by
   concatenation and `tostring`. Domain-proven text and numeric conversions do
   not reserve that work.
10. Unit and pinned ClickHouse coverage includes official and spaced syntax,
    operand order, fixed and Dynamic scalar matrices, maximum unsigned
    integers, tagged and physical Decimals, null and missing propagation,
    direct and explicit Boolean behavior, multivalue and object rejection,
    sequential eval, `if`, `where`, `stats`, binary raw provenance, exact and
    first-invalid limits, forged plans, source-once SQL, and `EXPLAIN` proof
    that row expansion is absent. The helper is registered in the pinned Store
    suite.

Validation completed on the execution checkpoint:

```sh
go test ./... -count=1
golangci-lint run ./...
go test -race ./internal/clickhouse -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=5m
git diff --check
```

All listed gates pass on `bc80006`; repository-wide Go lint reports zero
issues, and the pinned ClickHouse `26.3.17.4` Store suite includes the
concatenation corpus. The synchronized compatibility and editor publication
also passes `npm run test:frontend`, `npm run typecheck`, `npm run lint`, and
`git diff --check`.

Immediate resume steps:

1. Confirm `main` and `origin/main` retain `875ddad` followed by `bc80006`,
   with the compatibility contract and editor help synchronized to that
   execution checkpoint.
2. Add the already-prototyped `/search/validate` backend route without creating
   a search job, sharing create-search parsing, time/index authorization,
   planning, and source-located diagnostics.
3. Add bounded `/search/suggestions` support and index/time-scoped field
   completion on top of the same admission contract; static browser
   completions are not the Phase 2 field-autocomplete service.
4. Resolve the connected time-picker mismatch: Today, Yesterday, and All-time
   currently publish `@d`, `-1d@d`, and `0`, while `internal/searchtime`
   admits only RFC3339, `now`, and bounded negative offsets. Define and
   implement the missing backend semantics or stop publishing invalid forms.
5. Complete administrator-only generated-SQL/ClickHouse-`EXPLAIN` inspection
   and schema, ordering-key, text-index, and materialized-field tuning against
   the real query and load corpus. Do not expose plans or sensitive bound
   values to ordinary users.
6. Keep `eventstats` behind the stable aggregate library. Finish Phase 2 before
   proceeding through the plan's Phase 3/4 index administration, RBAC, HEC,
   alerts, scheduled-search, and packaging work.

## Previous checkpoint: bounded timezone-aware SPL `relative_time`

Date: 2026-07-27

Backend-neutral specifier checkpoint (committed and pushed):
`6e18333`

Parser and logical-plan checkpoint (committed and pushed):
`421ba4d`

Unsnapped-semantics and shared-boundary hardening checkpoint (committed and
pushed):
`72b7936`

ClickHouse compiler, pinned integration, and adversarial hardening checkpoint
(committed and pushed):
`2a1245c`

This test-first slice implements bounded
`relative_time(time, "specifier")`:

1. The parser accepts exactly one scalar time value and one quoted literal
   specifier, treats the function name case-insensitively, preserves complete
   source ranges, and leaves a bare field named `relative_time` ordinary.
   Parser, planner, predicate validation, and compiler independently reject
   forged arity, a calculated or unquoted specifier, Boolean input, typed nil,
   unsupported enums, malformed syntax, magnitude overflow, and resource
   overflow.
2. `internal/splrelativetime` owns the backend-neutral grammar. A specifier may
   contain a signed pre-snap offset, a snap, and a signed post-snap offset, in
   that order; either an offset alone or a snap alone is valid. Offset-only
   forms such as `-1h` preserve the input's minute, second, and fractional
   components. Only an explicit snap such as `-1h@h` rounds to a boundary.
3. Supported offset and snap units are seconds, minutes, hours, days, weeks,
   months, quarters, and years, with the documented lowercase aliases.
   `@w`, `@week`, `@w0`, and `@w7` snap to Sunday, while `@w1` through `@w6`
   select Monday through Saturday. A program has at most three operations;
   unsupported uppercase, whitespace, millisecond, repeated-snap, or
   additional-offset syntax fails before execution.
4. Second, minute, and hour offsets are elapsed durations. Day and week
   offsets are local-calendar day changes; month, quarter, and year offsets
   are local-calendar month changes with pinned end-of-month and leap-year
   clamping. Operations execute strictly left to right, so
   `-1mon@mon+7d` first moves one calendar month, then snaps to the month, then
   moves seven local days.
5. Every calendar operation and snap uses the search's effective immutable
   IANA timezone. Subday snaps use the instant's historical offset, including
   non-hour offsets and both occurrences of a fall-back hour. Calendar and
   elapsed arithmetic remain distinct across spring-forward and fall-back.
   A skipped civil date or an unrepresentable historical boundary fails
   closed to null instead of wrapping or silently moving in the wrong
   direction.
6. Canonical time and fixed finite numeric inputs are interpreted at
   nanosecond precision; `now()` composes through its immutable whole-second
   search anchor. Dynamic runtime integers avoid `Float64` conversion, finite
   numeric variants and bounded tagged decimals use the numeric path, and
   known numeric/text Dynamic domains omit impossible tagged parsing.
   Statically null input returns typed null. Fixed String, Boolean, and
   multivalue input is rejected; Dynamic String, Boolean, list, object,
   timestamp tag, null, missing, non-finite, overflowing, invalid, or
   oversized decimal input returns null.
7. The supported instant policy is the canonical inclusive
   1900-01-01-through-2262-01-01 UTC domain shared by search storage and SPL.
   Every input, intermediate result, and final result is checked. Nonzero
   positive offsets must move forward, nonzero negative offsets must move
   backward, and snaps must not move forward; these direction checks catch
   ClickHouse wrap and clamp behavior that can otherwise produce an
   in-policy-looking wrong result.
8. Calendar work is admitted only after the instant reaches the Go-derived
   1900-01-01 local boundary for the selected embedded-IANA location. This
   avoids consulting ClickHouse calendar components or offsets in its clamped
   pre-1900 local range. UTC, Los Angeles, Amsterdam, Kathmandu, Dublin, and
   Apia coverage pins ordinary, fractional-hour, historical-second,
   lower-bound, daylight-saving, and skipped-date behavior across the Go and
   ClickHouse timezone runtimes.
9. One specifier is capped at 1 KiB, 1,024 work units, and three operations.
   One query may total 16,384 specifier work units and 256 operations.
   Open-schema tagged-decimal timestamp conversion is capped at 4 KiB per call
   and 64 KiB per result row across both `relative_time` and `strftime` calls.
   Each scalar lowering has a 64 KiB generated-SQL ceiling beneath the existing
   256 KiB whole-query ceiling.
10. The compiler binds the timezone and every authored magnitude as ordinary
    query arguments, binds each source and predecessor once, nests operations
    linearly, and never expands rows. Exact `Int256` tick arithmetic handles
    elapsed offsets; preguarded `addDays` and `addMonths` handle calendar
    offsets. A shared Unix-seconds-to-`DateTime64(9)` conversion keeps
    `strftime` and `relative_time` type behavior aligned while preserving
    exact nonzero Dynamic integer handling.
11. Unit and pinned ClickHouse coverage includes every offset unit, every snap
    unit and weekday spelling, offset/snap/post-offset order, fractional
    unsnapped values, month-end and leap-year clamps, spring/fall transitions,
    ambiguous hours, historical offsets, skipped dates, exact policy
    boundaries, intermediate excursions, fixed and Dynamic types, tagged
    decimal bounds, `now`, `strptime`, nested `strftime`, predicates,
    conditionals, projection, aggregation, later eval, forged plans, lazy
    caches, linear SQL, input-once behavior, and query-wide budgets. The
    opt-in helper is explicitly registered in the pinned Store suite.
12. Independent correctness, quality, reuse, and efficiency review drove the
    final hardening: offset-only programs no longer synthesize a seconds snap;
    specifier operations are privately stored; year bounds and diagnostic
    text have one canonical owner; unit-specific magnitude limits prevent
    impossible work; unsafe ClickHouse truncation and arithmetic paths have
    range and direction fences; historical local admission uses embedded Go
    timezone data; Dynamic integer and decimal paths are exact or bounded;
    known Dynamic domains avoid unnecessary parsing; every unit has a real
    result assertion; and the shared `strftime` conversion has direct
    nonzero signed/unsigned regression coverage.

Validation completed on the compiler checkpoint:

```sh
go test ./... -count=1
golangci-lint run ./...
go test -race ./internal/clickhouse -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=5m
git diff --check
```

All listed gates pass on `2a1245c`. Repository-wide Go lint reports zero
issues. The final exact ordinary Go suite passed in 21.118 seconds, the
ClickHouse race suite passed in 5.033 seconds, and the pinned ClickHouse
26.3.17.4 Store suite passed in 53.774 seconds with the Dublin historical
local-boundary, all-unit offset, oversized-decimal, and shared-`strftime`
regressions included.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain, in order, `6e18333`, `421ba4d`,
   `72b7936`, and `2a1245c`. Preserve unexpected local changes and keep the
   compatibility contract and editor publication synchronized with this
   execution checkpoint.
2. If initial SPL-surface completion remains the priority, research and
   implement bounded eval concatenation next. It is the remaining initial
   eval surface named by `docs/product-architecture-plan.md`; pin operator
   grammar, coercion, null, Dynamic, multivalue, UTF-8, output-size, nesting,
   and ClickHouse behavior before adding parser support.
3. Add the already-prototyped `/search/validate` backend route without creating
   a search job, sharing create-search parsing, time/index authorization,
   planning, and source-located diagnostics. Then add bounded
   `/search/suggestions` support and index/time-scoped field completion on top
   of the same admission contract; static browser completions are not the
   Phase 2 field-autocomplete service.
4. Resolve the connected time-picker mismatch before calling inline time-range
   support complete: Today, Yesterday, and All-time currently publish `@d`,
   `-1d@d`, and `0`, while `internal/searchtime` admits only RFC3339, `now`,
   and bounded negative offsets. Either define and implement snap/epoch
   semantics at the backend boundary or stop publishing those invalid forms;
   do not silently approximate them.
5. Complete administrator-only generated-SQL/ClickHouse-`EXPLAIN` inspection
   and schema, ordering-key, text-index, and materialized-field tuning against
   the real query and load corpus. Do not expose plans or sensitive bound
   values to ordinary users.
6. Keep `eventstats` behind the stable aggregate library. Finish Phase 2 before
   proceeding through the plan's Phase 3/4 index administration, RBAC, HEC,
   alerts, scheduled-search, and packaging work. Scheduled-search relative-time
   variants and per-event `time()` remain outside this ad-hoc search contract.

## Previous checkpoint: bounded timezone-aware SPL `strptime`

Date: 2026-07-27

Backend-neutral format validation checkpoint (committed and pushed):
`da587a4`

Parser and logical-plan checkpoint (committed and pushed):
`c9221de`

ClickHouse compiler checkpoint (committed and pushed):
`4d34c8a`

Pinned ClickHouse integration checkpoint (committed and pushed):
`983e125`

Editor checkpoint (committed and pushed):
`fe4b7bc`

Adversarial correctness, resource, and reuse checkpoint (committed and
pushed):
`4966a7d`

Exact-one-parser and case-insensitive-meridiem checkpoint (committed and
pushed):
`825c1e4`

Compatibility checkpoint (committed and pushed):
`7dd3209`

This test-first slice implements bounded `strptime(value, "format")`:

1. The parser accepts exactly one scalar value and one quoted literal format,
   treats the function name case-insensitively, preserves complete source
   ranges, and leaves a bare field named `strptime` ordinary. Parser, planner,
   predicate validation, and compiler independently reject forged arity,
   nonliteral formats, Boolean input, typed nil, unsupported enums, malformed
   directives, and resource overflow.
2. Parsing requires exactly one complete numeric calendar date. The supported
   variables are `%%`; `%Y`, `%m`, `%d`, `%F`; `%H`, `%I`, `%M`, `%S`, `%p`,
   `%T`; `%z`; and `%Q`, `%3Q`, `%6Q`, `%3N`, `%6N`, `%f`. Incomplete,
   duplicate, ambiguous, locale-dependent, named-zone, colon-offset,
   two-digit-year, ISO-week, epoch-input, nine-digit, unknown, and dangling
   variables are rejected rather than delegated to backend defaults.
3. Numeric month, day, hour, minute, and second fields may be unpadded. `%p`
   accepts any ASCII case of `AM` or `PM`; `%z` accepts compact signed offsets.
   Fractions accept one through their declared three- or six-digit width. A
   terminal literal dot plus `%Q` or explicit 3/6-digit `%Q`/`%N` may be
   omitted together; `%f` remains required.
4. Fixed String and Dynamic runtime String values parse to nullable `Float64`
   Unix seconds after microsecond parsing. Statically null input returns typed
   null. Fixed numeric, Boolean, canonical-time, and multivalue input is
   rejected; Dynamic non-String, null, missing, invalid-calendar, mismatched,
   trailing, and oversized input returns null without throwing. The public
   numeric result follows the documented v0.1 binary-`Float64` precision model.
5. Inputs without `%z` use the search's effective IANA timezone; an explicit
   offset takes precedence. Authored civil dates are checked before timezone
   conversion and must be from 1971-01-01 through 2299-12-31 inclusive, with a
   representable resulting instant. A supported civil date may therefore
   legitimately convert to a 1970 instant. Pinned ClickHouse behavior
   normalizes daylight-saving gaps forward and chooses the earlier fall-back
   occurrence.
6. One format is independently capped at 4 KiB and 4,096
   literal-code-point/directive work units. One runtime input is capped at
   4 KiB. Across a query, calls may total 16,384 format work units and 64 KiB
   of date parsing per row; each lowering also has a 64 KiB generated-SQL
   ceiling beneath the whole-query ceiling.
7. The compiler binds the exact-match input-shape regular expression, Joda
   patterns, and timezone as arguments. It references input once, extracts
   authored date fields once, selects the optional-fraction primary or
   fallback pattern before parsing, executes exactly one parser per value, and
   never expands rows.
8. Pinned ClickHouse coverage includes fixed and Dynamic types, invalid and
   trailing input, millisecond and microsecond precision, omitted terminal
   fractions, unpadded fields, case-insensitive meridiem, compact offsets,
   civil-date boundaries, UTC and America/Los_Angeles summer/winter behavior,
   daylight-saving gaps/folds, predicates, projection, aggregation, later
   eval, nested `strftime`, and query-wide budgets.
9. Adversarial review removed `strftime` output-policy leakage by introducing
   a common lexer with caller-owned limits, pinned independent `strptime`
   limits, removed unused fraction/offset metadata, allocated the format cache
   lazily, enforced authored civil bounds before timezone conversion,
   preserved linear SQL/input-once behavior, accepted lowercase and mixed-case
   meridiem, and replaced eager fallback parsing with capture-directed
   exact-one-parser routing.
10. The suggestion to merge SPL and plan scalar enums or centralize every
    scalar descriptor was declined for this slice: explicit exhaustive
    conversion and independent validation remain forged-IR trust boundaries.
    The initial Float64 microsecond-identity concern was also withdrawn because
    the result intentionally follows the repository's documented Float64
    numeric model; the compatibility contract now states that precision edge.
11. Editor highlighting recognizes only parenthesized `strptime`, and eval and
    where completions advertise the bounded nullable-Unix-seconds signature.

Validation completed on the final implementation:

```sh
go test ./... -count=1
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=5m
git diff --check
```

All gates pass. Repository-wide Go lint reports zero issues. The frontend
corpus contains 128 application tests and 47 release/build tests. Type
checking and frontend lint pass. The final pinned Store suite passed in
56.808 seconds after lowercase/mixed-case meridiem coverage and
exact-one-parser routing.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain, in order, `da587a4`, `c9221de`,
   `4d34c8a`, `983e125`, `fe4b7bc`, `4966a7d`, `825c1e4`, and `7dd3209`.
   Preserve unexpected local changes.
2. Begin `relative_time` as the next independent researched slice. Pin its
   relative-specifier grammar, search-time anchor, calendar-versus-duration
   arithmetic, timezone and daylight-saving behavior, precision,
   invalid/null/type behavior, resource limits, and ClickHouse portability
   before adding parser support.
3. Preserve the existing `now()`, `strftime`, and `strptime` trust-boundary
   validation and replay-stable search scope; do not infer scheduled-search or
   per-event wall-clock semantics for `relative_time`.

## Previous checkpoint: bounded timezone-aware SPL `strftime`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`fe94e37`

ClickHouse compiler and pinned integration checkpoint (committed and pushed):
`28c27e2`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`7bf4f6f`

Compatibility and editor checkpoint (committed and pushed):
`1e78bf4`

This test-first slice implements bounded `strftime(time, "format")`:

1. The parser accepts exactly one scalar time value and one quoted literal
   format, treats the function name case-insensitively, preserves complete
   source ranges, and leaves a bare field named `strftime` ordinary. Parser,
   planner, predicate validation, and compiler independently reject forged
   arity, a nonliteral format, Boolean input, malformed variables, typed nil,
   unsupported enums, and format/resource overflow.
2. `internal/spltimeformat` owns a backend-neutral, locale-stable directive
   model. One format is capped at 4 KiB of UTF-8, 4,096 literal-code-point plus
   directive work units, and 16 KiB of conservative output. A query may total
   16,384 work units and 64 KiB of conservative output per row across all
   calls; each lowering also has a 64 KiB SQL ceiling.
3. The supported variables are `%%`; `%Y`, `%y`, `%G`, `%g`; `%m`, `%b`,
   `%B`; `%d`, `%e`, `%j`; `%V`, `%w`, `%a`, `%A`; `%H`, `%I`, `%M`, `%S`,
   `%p`, `%T`; `%F`, `%s`; `%z`, `%:z`; `%Q`, `%N`, `%f`; and explicit
   3/6/9-digit `%Q`/`%N` widths. Locale-dependent, timezone-abbreviation,
   malformed, and pinned-ClickHouse-incompatible variants are rejected rather
   than delegated to process or database configuration.
4. Canonical time input retains `DateTime64(9)`. Fixed numeric input is Unix
   seconds with nanosecond flooring, including correct negative fractional
   and pre-epoch `%s` behavior. `now()` composes as its immutable search-start
   integer. Fixed String, Boolean, and multivalue values are rejected;
   statically null input is null.
5. Dynamic input formats only a finite runtime number inside the supported
   timestamp range. Runtime String, Boolean, list, object, tagged value, null,
   missing, non-finite, and overflowing numeric values return null without
   parsing or throwing.
6. `plan.Scope`, `plan.Query`, completed execution snapshots, replay planning,
   field-summary cache keys, and field-page cursors carry the effective IANA
   search timezone. Omitted user timezone remains UTC. Invalid, server-local,
   host-specific POSIX/leap-second, overlong, and unknown zone names fail at
   admission or the planner/compiler trust boundary.
7. One shared success-only IANA loader embeds fallback timezone data and
   process-caches immutable named locations for admission, planning, and
   compilation. Invalid attacker-controlled names are never cached. The
   timestamp is evaluated once and localized once per call rather than once
   per generated format fragment.
8. The compiler binds the timezone and formatter fragments as query arguments,
   preserves apostrophe/Unicode/percent literals, never expands rows, and
   uses portable custom lowering where ClickHouse's percent and Joda dialects
   differ. `%M` is not delegated to the pinned percent formatter, `%s` uses
   exact nanosecond arithmetic, and `%g`, `%e`, `%w`, and offsets have explicit
   implementations.
9. ISO `%G`/`%g` use the week-based year, not the calendar year. A dedicated
   2021-01-01 integration fixture proves `2020`, week 53, preventing a
   midyear-only test from masking the Joda `Y` versus `x` distinction.
10. The pinned ClickHouse corpus covers every supported directive, UTC and
    America/Los_Angeles summer/winter offsets, ISO year boundaries, zero and
    negative fractional epochs, nanoseconds, `now()`, Unicode/apostrophe/
    percent/empty literals, fixed and Dynamic types, predicates, projection,
    aggregation, later eval, and replay.
11. Independent quality, reuse, and efficiency reviewers found six concrete
    correctness, resource, and duplication issues; all were fixed: ISO
    week-year lowering, planner IANA validation, repeated timezone loading,
    per-fragment localization, retained duplicate format text, and a custom
    test slice comparator. The broader suggestion to merge parser and plan
    scalar enums was not applied because the explicit exhaustive conversion is
    the forged-IR trust boundary.
12. Editor highlighting recognizes only parenthesized `strftime`, eval and
    where completions advertise the exact timezone-aware signature, and the
    compatibility contract records the full directive, type, null, precision,
    timezone, portability, snapshot, and resource behavior.

Validation completed on the current implementation:

```sh
go test ./... -count=1 -timeout=5m
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=5m
git diff --check
```

All gates pass. Repository-wide Go lint reports zero issues. The frontend
corpus contains 127 application tests and 47 release/build tests. The complete
pinned Store suite passed in 55.030 seconds after the ISO-boundary and
single-localization fixes.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `fe94e37`, `28c27e2`, `7bf4f6f`,
   and `1e78bf4`. Preserve unexpected local changes.
2. Treat `strptime` and `relative_time` as independent researched slices.
   Pin their literal grammar, effective timezone, precision, invalid/null/type
   behavior, resource limits, and ClickHouse portability before adding parser
   support.
3. Do not infer `strptime` support from the formatter directive set: parsing
   has different ambiguity, range, default-field, DST-gap/fold, and failure
   semantics that require their own contract and pinned differential corpus.
4. Keep scheduled-search variants and per-event `time()` outside the ad-hoc
   `now()`/`strftime` contract until those execution surfaces are explicitly
   designed.

## Previous checkpoint: immutable search-start SPL `now`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`33134e9`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`7d52001`

Adversarial search-anchor correctness checkpoint (committed and pushed):
`1314fc9`

Adversarial replay-fingerprint and reuse checkpoint (committed and pushed):
`df5c13a`

Compatibility and editor checkpoint (committed and pushed):
`8d4911c`

This test-first slice implements search-scoped `now()`:

1. The parser accepts a case-insensitive zero-argument call in eval and where
   expressions, preserves its complete range, and leaves a bare field named
   `now` ordinary. Parser, planner, predicate validation, and scalar lowering
   independently reject forged arguments and invalid function enums.
2. `now()` returns a present, non-null fixed `Int64` containing the whole Unix
   second at which the ad-hoc search was admitted. Subseconds are truncated,
   every occurrence in one search is identical, and supported numeric
   comparison, conditional, rounding, and default `tostring` composition
   remains available.
3. `plan.Scope` and `plan.Query` carry an explicit immutable `SearchStart`
   timestamp. It is sourced from the job's persisted `CreatedAt` value and is
   intentionally independent from the index-time and storage-visibility
   cutoffs. Missing anchors fail at both planning and compiler trust
   boundaries.
4. Completed execution snapshots preserve the search-start timestamp.
   Rebuilding plans for field catalogs, field summaries, timelines, and
   exports therefore reproduces the original value instead of consulting a
   later wall clock.
5. The compiler binds one signed whole-second argument per occurrence and
   emits `CAST(? AS Int64)` rather than ClickHouse `now()` or `now64()`.
   Projection, stats, and later eval stages retain the same query-local compile
   context.
6. Search-scoped constants and the existing MATCH/LIKE resource ledgers now
   live in one atomically initialized shared compile context. Fresh relation
   states carry one pointer, eliminating manual search-anchor copies and
   partially initialized pattern maps.
7. The execution-snapshot fingerprint includes `SearchStart`. A changed anchor
   invalidates both field-page cursors and field-summary cache entries instead
   of reusing output compiled with a different `now()` value.
8. Unit and pinned ClickHouse coverage separates search start from the storage
   cutoff and checks repeated calls, `_time<=now()`, `tostring`, projection,
   stats, later eval, replay preservation, missing anchors, forged arity, and
   cache/cursor invalidation.
9. Independent correctness, reuse, and efficiency reviewers found six
   actionable issues: storage-cutoff coupling, mutable-state anchor copying,
   zero-arity sentinel handling, partially initialized compile context, and a
   missing replay-fingerprint field, plus duplicated custom-scope compiler
   fixtures. All were fixed. The remaining explicit SPL-to-plan enum conversion
   stays as a deliberate trust boundary.
10. Editor highlighting recognizes only parenthesized `now`, eval and where
    completions describe the fixed search-start Unix second, and the
    compatibility specification distinguishes scheduled-search behavior,
    per-event `time()`, and the still-unsupported `relative_time`, `strftime`,
    and `strptime` slices.

Validation completed on the current implementation:

```sh
go test ./... -count=1
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. Repository-wide Go lint reports zero issues. The frontend
corpus contains 126 application tests and 47 release/build tests. The pinned
Store suite proves that `now()` uses a deliberately different search-admission
timestamp rather than the index-time cutoff.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `33134e9`, `7d52001`, `1314fc9`,
   `8d4911c`, and `df5c13a`. Preserve unexpected local changes.
2. Treat `relative_time`, `strftime`, and `strptime` as independent semantic
   slices with their own literal grammar, timezone, precision, null/type, and
   ClickHouse portability contracts.
3. Keep scheduled-search variants and per-event `time()` outside the ad-hoc
   `now()` contract until those execution surfaces are explicitly designed.

## Previous checkpoint: bounded typed SPL `like`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`faa88c1`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`93ec477`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`d758a07`

Compatibility and editor checkpoint (committed and pushed):
`8accd61`

This test-first slice implements bounded `like(value, "pattern")`:

1. The parser accepts exactly one scalar value and one quoted literal wildcard
   pattern, preserves complete source ranges, treats function names
   case-insensitively, and leaves a bare field named `like` ordinary. The
   Boolean result composes through `where`, comparison, `NOT`/`AND`/`OR`,
   `if`, `case`, `mvcount`, and default `tostring`.
2. Matching is case-sensitive and covers the whole input. `%` matches zero or
   more Unicode code points, including newlines; `_` matches one Unicode code
   point. Empty, prefix, suffix, substring, Unicode, newline, and case edges
   are pinned against ClickHouse 26.3.17.4.
3. Decoded SPL patterns use ClickHouse-compatible backslash semantics:
   backslash escapes `%`, `_`, or backslash, and remains literal before an
   ordinary character. An unpaired terminal backslash is rejected before
   ClickHouse can raise `CANNOT_PARSE_ESCAPE_SEQUENCE`.
4. Fixed String input is matched directly. Fixed numeric, Boolean, and
   canonical-time values use their supported text spelling. Dynamic runtime
   String is supported; Dynamic numbers, Booleans, arrays, objects, tagged
   values, null, missing fields, and binary provenance produce nullable
   Boolean null. Fixed `Array(String)` fails with
   `SPL_UNSUPPORTED_MULTIVALUE_USAGE`.
5. Authored and normalized patterns are each capped at 4 KiB. One pattern is
   capped at 4,096 wildcard/literal work units, and all occurrences in a query
   are capped at 16,384 work units. Adjacent unescaped `%` runs collapse
   without expanding the pattern.
6. Each calculated input has a conservative 4 MiB byte ceiling. Independently,
   all LIKE occurrences may total at most 16 MiB of conservative wildcard
   scanning per row, so many cheap patterns cannot multiply a large scan
   without limit.
7. String-size metadata survives eval and projection plus retaining `rex` and
   `spath` misses, `stats BY`, and string-preserving `min`, `max`, `earliest`,
   and `latest`. Fixed Boolean and numeric formatting use type-specific bounds
   instead of the durable 1 MiB String fallback.
8. The compiler validates quoted-literal provenance at its own trust boundary,
   caches normalized patterns, binds each as a query argument, references the
   input once, lowers directly to ClickHouse `like()`, and enforces a separate
   64 KiB generated-SQL ceiling per call.
9. Shared planner literal validation is typed-nil-safe for both `like` and
   `match`. Shared compiler operand/result lowering and one consolidated
   pattern-budget state remove duplicated trust and state propagation paths
   without coupling the two pattern dialects.
10. Independent reuse, quality, and efficiency reviewers found nine actionable
    defects or maintainability risks; all were fixed. Two broader suggestions
    were deliberately not applied: duplicated parser/plan enums retain an
    explicit trust-boundary conversion, and the forged aggregate-pattern test
    remains necessary because the 16 KiB source ceiling prevents an equivalent
    parsed query from reaching that budget.
11. Editor highlighting recognizes only parenthesized `like`, and `where` and
    `eval` completions advertise the exact whole-string `%`/`_` contract. The
    compatibility specification records per-pattern, aggregate-pattern,
    per-input, aggregate-scan, Dynamic/null, escape, and ClickHouse boundaries.

Validation completed on the current implementation:

```sh
go test ./internal/splwildcard ./internal/spl ./internal/plan \
  ./internal/clickhouse -run 'Like|Match' -count=1
go test ./... -count=1
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All gates pass. Repository-wide Go lint reports zero issues. The frontend
corpus contains 125 application tests and 47 release/build tests. The pinned
Store suite, including every LIKE escape, Unicode, newline, fixed conversion,
Dynamic/null/container, and binary-provenance edge, passed in 56.31 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `faa88c1`, `93ec477`, `d758a07`,
   and `8accd61`. Preserve unexpected local changes.
2. Pin the next scalar slice's Splunk signature, fixed/Dynamic type behavior,
   null and multivalue behavior, resource limits, and ClickHouse lowering
   before adding parser tests.
3. Keep calculated LIKE patterns, direct Boolean eval assignment, multivalue
   matching, case-insensitive LIKE variants, and broader wildcard dialects as
   separate compatibility slices.

## Previous checkpoint: bounded typed SPL `match`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`527a4ca`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`5f604b8`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`4233a5a`

Compatibility and editor checkpoint (committed and pushed):
`39c0fd4`

This test-first slice implements bounded `match(value, "regex")`:

1. The parser accepts exactly one scalar value and one quoted literal pattern,
   preserves complete source ranges, treats function names case-insensitively,
   and leaves a bare field named `match` ordinary. The call is a first-class
   Boolean predicate for `where`, comparisons, `NOT`/`AND`/`OR`, `if`, and
   `case`.
2. Dedicated AST and logical-plan enums carry the operation. Shared exhaustive
   Boolean-function traits keep parser, planner, and compiler consumers in
   parity. Every trust boundary rejects forged arity, nonliteral patterns,
   Boolean input, invalid enums, typed nil, excess depth/nodes, and cycles.
3. Matching is case-sensitive substring search by default. Inline flags are
   supported; normalization explicitly disables ClickHouse's default dot-all
   mode while allowing user `(?s)` to opt back in. Empty and zero-width
   patterns are valid.
4. Non-multiline PCRE `$` is normalized to match strict end or immediately
   before one final newline. `\z` remains strict-end-only, and `(?m)$` retains
   line-end behavior. Unit and pinned ClickHouse tests prove all three.
5. Fixed String input is matched directly. Fixed numeric, Boolean, and
   canonical-time scalars use their supported text spelling. Dynamic runtime
   String is supported; Dynamic numbers, Booleans, arrays, objects, tagged
   values, null, missing fields, and text-ineligible binary input produce
   nullable Boolean null. Fixed `Array(String)` fails with
   `SPL_UNSUPPORTED_MULTIVALUE_USAGE`.
6. Direct Boolean assignment remains outside search-mode `eval`; `tostring`,
   `if`, `case`, predicate composition, and explicit Boolean comparison consume
   the result. Centralized compiler diagnostics now consistently say
   “Boolean result” rather than the obsolete null-predicate-only wording.
7. Authored and normalized pattern text are each capped at 4 KiB. A compact-AST
   estimator caps the post-repeat RE2 program at 4,096 work units before
   simplification or compilation can expand counted repetitions. The compiler
   also caps the sum of all `match` occurrences at 16,384 work units.
8. The shared bounded-RE2 parser is used by both `match` and `rex`; `rex` now
   receives the same 4,096-work-unit counted-repeat protection without changing
   its capture-sensitive `$` contract. Adversarial `a{1000}` repetition tests
   prove rejection without first materializing the expanded program.
9. The compiler validates and normalizes a pattern once per expression,
   caches the typed result while still counting every occurrence, binds the
   normalized text as a query argument, references the scalar source once, and
   enforces a separate 64 KiB generated-SQL ceiling.
10. Scalar compilation propagates conservative maximum String bytes through
    literals, conditionals, coalesce, case conversion, substring, tostring,
    eval, projection, bucket, and rename. Always-consuming `replace` uses a
    saturating input/replacement product. `match` rejects calculated input that
    may exceed 4 MiB, closing the near-1-GiB replacement-amplification path
    before regex execution.
11. Compiler and pinned integration coverage exercises substring and anchored
    search, case and dot flags, final-newline anchors, empty/zero-width
    patterns, fixed conversion, Dynamic/null/missing/container/binary behavior,
    composition, source occurrence, multivalue rejection, forged plans,
    counted-repeat bombs, aggregate regex work, and calculated-input
    amplification.
12. Independent reuse, quality, and efficiency reviewers reported nine
    actionable findings, all applied: shared validation/classification,
    compiler caching, exhaustive Boolean traits, type-correct diagnostics,
    named limit assertions, bounded RE2 reuse, counted-repeat program budgets,
    calculated-input bounds, and PCRE-compatible `$` semantics.

Validation completed on the current implementation:

```sh
go test ./internal/splregex ./internal/spl ./internal/plan \
  ./internal/clickhouse \
  -run 'Match|ExtractionPattern|ScalarFunctionBooleanTraits' \
  -count=1 -timeout=3m
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero issues.
The frontend corpus contains 124 application tests and 47 release/build tests,
and the production static export generated all 11 pages. The pinned Store
suite, including the `$`/`\z`/multiline-anchor corpus, passed in 50.04 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `527a4ca`, `5f604b8`, `4233a5a`,
   and `39c0fd4`. Preserve unexpected local changes.
2. Select the next bounded scalar slice only after pinning its Splunk
   signature, fixed/Dynamic type behavior, null and multivalue behavior,
   resource limits, and ClickHouse lowering.
3. Keep broader regex dialect support, calculated patterns, direct Boolean
   eval assignment, multivalue matching, formatted `tostring`, arithmetic,
   concatenation, and canonical-time conversion as separate compatibility
   slices.

## Previous checkpoint: bounded typed SPL `mvcount`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`f68cacc81694b3f65ccb0838a082770b437a94ac`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`738fabf8282aec2feeb9e82f0e4884d4e640a978`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`39966596c7584ecef7976fbb7a697cf1eb12abca`

Compatibility and editor checkpoint (committed and pushed):
`a4a07e3aa20c2d8d3c45b0cd7986f5e7b7b6503e`

This test-first slice implements bounded `mvcount(value)`:

1. The parser accepts a case-insensitive call with exactly one scalar
   argument, including Boolean null predicates, nesting, and predicate use.
   It preserves complete source ranges and leaves a bare field named
   `mvcount` ordinary.
2. A dedicated AST and logical-plan enum carries the operation. Parser,
   planner, and compiler trust boundaries independently reject forged arity
   and enums, typed-nil arguments, excessive depth/nodes, and cycles through a
   shared unary structural-validation harness.
3. A non-null fixed scalar returns `UInt64(1)`. Missing, explicit-null,
   projected-away, and statically null input returns nullable `UInt64` null.
   Fixed `Array(String)` returns its immediate length and maps empty to null.
4. Runtime `Array(Dynamic)` counts immediate members whose type is not `None`;
   empty and all-null arrays return null. Immediate nested arrays and objects
   count atomically. Other Dynamic arrays use physical length. Dynamic
   String, number, and Boolean values return one; ordinary objects and
   flattened object parents return null.
5. Valid `bytes/v1`, `timestamp/v1`, `duration/v1`, and `decimal/v1` tagged
   scalar envelopes return one. Exact-key shape, tag-specific payload grammar,
   and a 1 MiB payload ceiling are required. Live raw-storage tests prove each
   malformed tag fails closed to null without exposing its payload.
6. Dynamic input is metadata-presence guarded before binding, so absent sparse
   fields avoid value loading and dispatch. General/text input binds once;
   numeric-only calculated input skips a redundant singleton-array lambda.
   The shared non-null Array(Dynamic) cardinality helper keeps `mvcount` and
   aggregate `count(field)` aligned.
7. Every result carries a one-or-null cardinality invariant through eval
   assignments and projections. Any outer `mvcount` is compiled away as an
   identity, so 24 nested calls collapse to the exact SQL of one call.
8. No lowering uses `ARRAY JOIN` or changes row cardinality. Each call has a
   separate 64 KiB generated-SQL ceiling in addition to the 256 KiB
   whole-query limit; tagged-payload validation has its independent 1 MiB
   runtime bound.
9. Compiler and pinned integration coverage exercises literals, Boolean
   predicates, `MaxUint64`, fixed aggregates, text and numeric calculated
   domains, empty/all-null/mixed/nested arrays, missing/null/object parents,
   all four valid tagged scalars, all four malformed tagged scalars,
   predicates, exact source occurrence, and `EXPLAIN actions=1`.
10. Independent reuse, quality, and efficiency reviewers reported six
    actionable findings, all applied: shared tagged-envelope recognition,
    shared Dynamic-array cardinality, shared unary forged-plan coverage,
    fail-closed tagged payload validation, pre-binding sparse presence guards,
    direct numeric-domain lowering, and nested cardinality identity collapse.

Validation completed on the current implementation:

```sh
go test ./internal/clickhouse \
  -run 'TestCompileEval(MVCount|ToString|LowerUpper|LenLength)|TestCompileStats|TestFieldSummary' \
  -count=1
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
golangci-lint run ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero issues.
The frontend corpus contains 122 application tests and 47 release/build tests,
and the production static export generated all 11 pages. The pinned
Store/compiler suite, including the raw malformed-envelope corpus, passed in
49.919 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `f68cacc`, `738fabf`, `3996659`,
   and `a4a07e3`. Preserve unexpected local changes.
2. Select the next bounded scalar slice only after pinning its Splunk
   signature, fixed/Dynamic type behavior, null and multivalue behavior,
   resource limits, and ClickHouse lowering.
3. Keep broader multivalue functions, formatted `tostring`, negative or
   calculated `round` precision, arithmetic, concatenation, and canonical-time
   conversion as separate compatibility slices.

## Previous checkpoint: bounded typed SPL `ceil`/`ceiling` and `floor`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`ddcdc89393f8046109081d33ff8549b34351c067`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`e98484671194149d388ecab053a70d19ab56f509`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`25dcf9b069b9c30d768dfde5e198be679b49a9bf`

Compatibility and editor checkpoint (committed and pushed):
`92bcd210265938426a3df3a9b485f56dd0c9c8c1`

Final zero-lint cleanup checkpoint (committed and pushed):
`2f318e9d9f50eb4afb57cd4078d038201a104966`

This test-first slice implements bounded `ceil(value)`, its exact
`ceiling(value)` alias, and `floor(value)`:

1. The parser accepts case-insensitive calls with exactly one scalar argument,
   preserves each complete source range, supports nesting and predicate use,
   and leaves bare fields named `ceil`, `ceiling`, and `floor` ordinary.
   Boolean null-predicate consumption and invalid arity fail with source-
   located diagnostics.
2. Dedicated AST and logical-plan enums normalize `ceil` and `ceiling` to one
   operation while keeping `floor` distinct. Parser, planner, and compiler
   trust boundaries independently reject forged arity and enums, missing and
   typed-nil arguments, Boolean escape, excessive depth/nodes, and cycles.
3. Fixed `Int64` and `UInt64` input is an exact type-preserving identity,
   including `MaxUint64`. Fixed `Float64` uses pinned ClickHouse `ceil` or
   `floor`. Missing, explicit-null, projected-away, and statically null input
   returns nullable Float64 null. Fixed String, Boolean, canonical time, and
   multivalue inputs fail explicitly.
4. Dynamic physical integers retain their exact runtime type. Validated
   integral `decimal/v1` payloads inside signed `Int256` return exact Int256;
   coverage proves `9007199254740993` does not collapse through Float64.
   Other validated Decimal and physical floating/Decimal variants convert to
   finite Float64 before rounding.
5. Dynamic String, Boolean, null, arrays, objects, malformed or oversized
   Decimal envelopes, and non-finite values fail closed to null without
   payload exposure. Fixed multivalue input remains a source-located
   `SPL_UNSUPPORTED_MULTIVALUE_USAGE` error.
6. Float64 behavior is pinned on ClickHouse `26.3.17.4`:
   `ceil(1.2)=2`, `ceil(-1.2)=-1`, `floor(1.2)=1`, and
   `floor(-1.2)=-2`. Literal and Dynamic coverage also preserves the negative
   zero sign bit of `ceil(-0.2)`.
7. `round`, `ceil`, and `floor` share one typed numeric-rounding operation
   helper and one non-Boolean unary validator. Optional round precision is a
   typed `*uint8` configuration rather than loosely coupled SQL fragments and
   argument slices.
8. Each successful `ceil` or `floor` output carries an integral-numeric
   invariant through eval assignments, projections, and renames. Any outer
   `ceil` or `floor` is therefore compiled away as an identity; 24 alternating
   calls collapse to one runtime operation and one Dynamic source reference.
9. Dynamic source expressions bind once, numeric-only comparison lowering
   omits String/Boolean/container redispatch, atomic comparisons remain
   scalar, and no form uses `ARRAY JOIN` or changes event cardinality. The
   common numeric-rounding SQL ceiling is 64 KiB in addition to the 256 KiB
   whole-query ceiling.
10. Compiler and pinned integration coverage exercises aliases, full-width
    integers, fractional and exact Decimals, signed zero, nesting, predicates,
    fixed aggregates, open- and closed-schema missing fields, nulls,
    unsupported variants, malformed envelopes, exact source occurrence, and
    `EXPLAIN actions=1`.
11. Independent reuse, quality, and efficiency reviewers reported six
    actionable findings, all applied: typed helper configuration, shared
    unary validation, shared malformed-Decimal fixtures, a correctly named
    shared complexity bound, constant-depth integral composition, and the
    closed-schema missing-field fix. The last finding prevented valid
    `stats ... | eval ceil(absent)` pipelines from failing as String misuse.
12. The versioned compatibility contract cites the existing official Splunk
    mathematical-function and ClickHouse rounding references and records the
    alias, type, null, Decimal, signed-zero, optimization, and resource
    boundaries. Editor completion advertises all three signatures; syntax
    highlighting recognizes them only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero
issues. The frontend corpus contains 121 application tests and 47
release/build tests. The production static export generated all 11 pages, and
the final pinned Store/compiler run passed in 44.342 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `ddcdc89`, `e984846`, `25dcf9b`,
   `92bcd21`, and `2f318e9`. Preserve unexpected local changes.
2. Select the next bounded scalar slice only after pinning its Splunk
   signature, fixed/Dynamic type behavior, null and multivalue behavior,
   resource limits, and ClickHouse lowering. `mvcount` is the leading small
   candidate because typed multivalue results already exist.
3. Keep negative/calculated `round` precision, `tostring` formatting,
   canonical-time conversion, arithmetic, concatenation, and implicit String
   conversion as separate compatibility slices.
4. Keep Dynamic/container `coalesce` and `case`, heterogeneous conditionals,
   wildcard count, broader conditional count names, and `eventstats` as
   separate reviewed contracts.

## Previous checkpoint: bounded typed SPL `round`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`3120e80f5d8d02682f4e1e6c7b57d6bb725e3eff`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`4a6eceabfa05ad51d703dc63b0a92f8f95c05c57`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`00ddd33fe9b93035a95e4ed8fbed7b19f4151614`

Compatibility and editor checkpoint (committed and pushed):
`fe951150051fe12152c4dacc03e223cb41cdd100`

This test-first slice implements bounded `round(value[, precision])`:

1. The parser accepts a case-insensitive call with one or two scalar
   arguments, preserves its complete source range, supports nesting and
   predicate use, and leaves a bare field named `round` ordinary. Precision
   defaults to zero and, in version 0.1, must be a literal integer from 0
   through 18. Invalid precision fails at its own range with
   `SPL_UNSUPPORTED_ROUND_PRECISION`.
2. A dedicated AST and logical-plan enum carries the operation without
   stringly backend dispatch. Parser, planner, and compiler trust boundaries
   reject forged arity and enums, missing and typed-nil arguments, invalid
   precision, Boolean null-predicate escape, excessive depth/nodes, and
   cycles. Parser and planner share one pure SPL precision validator.
3. Fixed `Int64` and `UInt64` are exact type-preserving identities for every
   accepted non-negative precision, including `MaxUint64`. Fixed `Float64`
   uses ClickHouse `round`; missing or null becomes nullable Float64 null.
   Fixed String, Boolean, and canonical time fail with
   `SPL_UNSUPPORTED_ROUND_VALUE_TYPE`, while fixed multivalue fails with
   `SPL_UNSUPPORTED_MULTIVALUE_USAGE`.
4. Dynamic physical integers retain their exact runtime type. Physical
   Float/Decimal variants and validated fractional `decimal/v1` values
   convert to finite Float64 and return Dynamic Float64. Dynamic Strings,
   Booleans, null, arrays, objects, malformed/oversized Decimal envelopes,
   and non-finite values return null.
5. Mathematically integral validated `decimal/v1` payloads inside signed
   `Int256` use the exact lexical integer path and return Dynamic Int256.
   Integration coverage proves adjacent `9007199254740992` and
   `9007199254740993` remain distinct instead of collapsing through Float64.
6. Float64 behavior is explicitly pinned to binary-double, halfway-to-even
   rounding on ClickHouse `26.3.17.4`: `3.5 -> 4`, `2.5 -> 2`,
   `-2.5 -> -2`, `2.555,2 -> 2.56`, `15.275,2 -> 15.28`, and
   `17.275,2 -> 17.27`.
7. Dynamic lowering binds its source once and binds explicit precision as a
   second lambda input. Distinct nested precision coverage proves
   inner-to-outer argument order with `round(round(2.51,1),0) -> 2`.
   Every call has a 64 KiB SQL ceiling in addition to the 256 KiB whole-query
   ceiling.
8. One Dynamic-domain enum replaces contradictory text-only/numeric-only
   booleans. A `round` output is numeric-only, so nested calls and predicates
   omit String, Boolean, container, and tagged-Decimal redispatch. Physical
   numeric conversion uses guarded direct Dynamic casts rather than String
   formatting/parsing.
9. Compound Dynamic comparisons bind each operand once, fixing placeholder
   counts and superlinear SQL expansion. Atomic field/literal comparisons
   stay scalar and avoid per-row singleton arrays. The shared binding helper
   serves both text-only and generic Dynamic comparisons.
10. No lowering uses `ARRAY JOIN`, expands multivalue members, or changes
    event cardinality. The isolated ClickHouse corpus executes fixed and
    Dynamic rounding, full-width integers, fractional and exact Decimal
    values, nested precision, null/missing/container rejection, malformed
    envelopes, predicates, fixed aggregate output, and `EXPLAIN actions=1`.
11. Independent reuse, quality, and efficiency reviewers reported ten
    concrete improvements, all applied: shared precision and non-Boolean
    validation, shared comparison binding, one Dynamic-domain enum, explicit
    test dedup identities, correct nested bind order, exact tagged integers,
    direct numeric casts, reduced runtime dispatch, numeric-only comparison,
    and scalar atomic comparisons.
12. The compatibility contract cites official Splunk mathematical-function
    and ClickHouse rounding documentation and records every type/null/precision
    boundary. Editor completion advertises literal precision 0 through 18;
    syntax highlighting recognizes `round` only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run ./...
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero
issues. The frontend corpus contains 120 application tests and 47
release/build tests. The production static export generated all 11 pages, and
the final pinned Store/compiler run passed in 42.718 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `3120e80`, `4a6ecea`, `00ddd33`,
   and `fe95115`. Preserve unexpected local changes.
2. Implement bounded numeric `ceil`/`ceiling` and `floor` next only after
   pinning fixed and Dynamic return types, exact-integer identity, Decimal
   conversion, non-finite/null behavior, aliases, and ClickHouse lowering
   against the pinned target.
3. Keep negative/calculated `round` precision, `tostring` formatting,
   canonical-time conversion, arithmetic, concatenation, implicit String
   conversion, and `mvcount` as separate compatibility slices.
4. Keep Dynamic/container `coalesce` and `case`, heterogeneous conditionals,
   wildcard count, broader conditional count names, and `eventstats` as
   separate reviewed contracts.

## Previous checkpoint: typed default SPL `tostring`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`256a0e0fe6d001ad2771f0f6f5b3b4d036a3fb6e`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`46737fc32b6167dabdb4af466e0c844425f573aa`

Adversarial correctness, reuse, and efficiency checkpoint (committed and
pushed):
`a4ba7c2a6f51c549467f6affc56f0011c504a1be`

Compatibility and editor checkpoint (committed and pushed):
`35e7cb5929f02d066fab7b6633ffecba02b4af12`

This test-first slice implements bounded default `tostring(value)`:

1. The parser accepts a case-insensitive call with exactly one scalar
   argument, preserves its complete source range, supports nesting and
   predicate use, permits Boolean null-predicate consumption, and leaves a
   bare field named `tostring` ordinary. A second argument fails at its own
   range with `SPL_UNSUPPORTED_TOSTRING_FORMAT`; zero or three-plus arguments
   fail arity validation.
2. A dedicated AST and logical-plan enum carries the conversion without
   stringly backend dispatch. Parser, planner, and compiler trust boundaries
   independently reject forged arity, invalid enums, missing and typed-nil
   arguments, excessive depth/nodes, and cycles.
3. Fixed String input is an identity conversion. Fixed `Int64`, `UInt64`, and
   `Float64` use exact ClickHouse textual spelling. Fixed Boolean values and
   supported `isnull`/`isnotnull` results produce capitalized `"True"` or
   `"False"`. Missing, explicit-null, and statically null input returns null.
4. Dynamic runtime String, integer, floating-point, and Boolean values follow
   the same contract. Exact `decimal/v1` input preserves its validated
   payload spelling without `Float64` precision loss. The envelope must
   contain exactly its type/value keys, remain within the 4 KiB lexical
   ceiling, and match the Decimal grammar; malformed or oversized envelopes
   return null without exposing their payload.
5. Dynamic null, arrays, objects, and other tagged containers return null.
   Fixed `Array(String)` fails with `SPL_UNSUPPORTED_MULTIVALUE_USAGE`.
   Canonical time values fail with `SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE`
   until their timezone and precision behavior is separately pinned.
6. String identity retains `_raw` text-eligibility provenance:
   `tostring(_raw)` preserves binary-declared bytes, while a downstream
   Unicode text function still returns null. No lowering uses `ARRAY JOIN`,
   maps multivalue members, or changes event cardinality.
7. General Dynamic conversion binds its source once and dispatches only
   String, Boolean, validated Decimal, and physical numeric variants.
   Dynamic text-only producers use direct nullable String extraction rather
   than repeating broad dispatch. Fixed nullable Boolean uses one scalar
   `transform` instead of a singleton `arrayMap`.
8. Every call has a 64 KiB SQL ceiling in addition to the 256 KiB whole-query
   ceiling. Nested calls grow linearly, input bindings retain source order,
   and the text-only fast path references its complex child once.
9. Compiler unit coverage pins fixed and Dynamic values, full-width unsigned
   integers, exact Decimal detection, Boolean spelling, null/missing behavior,
   container rejection, `_raw` provenance, text-only dispatch, exact source
   occurrence, SQL growth, source-located diagnostics, typed nils, forged
   arity, and cycles.
10. The isolated ClickHouse corpus executes String, signed, `MaxUint64`,
    Float64, exact Decimal with trailing zeroes, Boolean, null, missing,
    predicate, binary `_raw`, Dynamic containers, and a directly inserted
    malformed Decimal envelope. It also proves no `ArrayJoin` with
    `EXPLAIN actions=1`.
11. Independent reuse, quality, and efficiency reviewers found five concrete
    improvements, all applied: generic unary scalar validation, a shared
    scalar-function integration harness, direct text-only Dynamic extraction,
    scalar nullable-Boolean transformation, and validated exact Decimal
    support. The quality review's Decimal finding prevented a semantic
    numeric value from being silently treated as a container.
12. The compatibility contract cites official Splunk conversion behavior and
    ClickHouse type-conversion behavior. It explicitly reserves formatted
    `binary`, `hex`, `commas`, and `duration` modes plus canonical time
    conversion for separately pinned slices. Editor completion advertises the
    default-only and capitalized-Boolean boundary; syntax highlighting
    recognizes `tostring` only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero
issues. The frontend corpus contains 119 application tests and 47
release/build tests. The production static export generated all 11 pages, and
the final pinned Store/compiler run passed in 41.556 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `256a0e0`, `46737fc`, `a4ba7c2`,
   and `35e7cb5`. Preserve unexpected local changes.
2. Implement bounded numeric `round` next only after pinning Splunk's optional
   precision argument, fixed and Dynamic numeric behavior, return types,
   null/non-numeric behavior, half-way rounding rule, full-width integer
   edges, and exact ClickHouse lowering against the pinned target.
3. Keep `tostring` formatting, canonical-time conversion, concatenation,
   implicit String conversion, `ceil`, `floor`, and `mvcount` as separate
   compatibility slices.
4. Keep Dynamic/container `coalesce` and `case`, heterogeneous conditionals,
   wildcard count, broader conditional count names, and `eventstats` as
   separate reviewed contracts.

## Previous checkpoint: typed SQLite-compatible SPL `substr`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`264211d3fc7d045846972bdd7f0f1d940fcc9acd`

Initial ClickHouse compiler and integration checkpoint (committed and pushed):
`c167e5882568611c834ceb2537f947dd59882b0c`

Adversarial reuse, quality, and efficiency checkpoint (committed and pushed):
`726810fb1be624afd076c264de8263b80951d82b`

Compatibility and editor checkpoint (committed and pushed):
`1895e3a103d2e458d9be9394137f4b93a2782d64`

Zero-lint cleanup checkpoint (committed and pushed):
`47db412073f6c50a20dd3d956fbb80fe2f3fb230`

Extreme-unsigned ClickHouse hardening checkpoint (committed and pushed):
`46ca648871865780da759c0de74c37bfa95ce07d`

This test-first slice implements bounded
`substr(value, start[, length])`:

1. The parser accepts a case-insensitive call with two or three arguments,
   preserves its complete source range, supports nesting and predicate use,
   and leaves a bare field named `substr` ordinary. The start and optional
   length must be literal integers in compatibility version 0.1; fields,
   nested expressions, floating-point values, and Booleans fail at their own
   ranges with `SPL_UNSUPPORTED_SUBSTRING_INDEX`.
2. A dedicated AST and logical-plan enum carries the operation without
   stringly backend dispatch. Parser, planner, and compiler trust boundaries
   independently reject forged arity, invalid enums, missing or typed-nil
   values and indexes, nonliteral indexes, Boolean null-predicate escape,
   excessive depth/nodes, and cycles.
3. Start and length follow the SQLite semantics which Splunk documents:
   indexes count UTF-8 code points from one, negative starts count from the
   right, zero is the virtual position before the first code point, omitted
   length returns through the end, and negative length selects code points
   immediately preceding start. The half-open interval is clipped rather
   than approximately delegated to ClickHouse.
4. The full signed 64-bit range and non-negative unsigned 64-bit range are
   supported. Interval arithmetic avoids signed negation of `MinInt64` and
   uses `Int128` before already-clipped offsets are converted to native
   substring argument types.
5. Fixed String input produces fixed String output. Dynamic runtime String
   produces nullable fixed String. Missing, explicit null, and Dynamic
   numbers, Booleans, arrays, objects, or other containers return null.
   Fixed numeric, Boolean, and canonical time input fails with
   `SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE`.
6. Fixed `Array(String)` fails with `SPL_UNSUPPORTED_MULTIVALUE_USAGE`;
   Dynamic arrays return null. Binary-declared `_raw` returns null even for
   ASCII bytes, `replace(_raw, ...)` preserves that provenance, and no lowering
   uses `ARRAY JOIN` or changes row cardinality.
7. Literal intervals which are statically identical to ClickHouse semantics
   lower directly to `substringUTF8`. Common positive starts, safe omitted
   lengths, zero lengths, start zero, and negative lengths with non-negative
   starts avoid a separate `lengthUTF8` scan and higher-order operations.
8. Negative starts with explicit non-zero length use one source/index binding
   plus one code-point-count binding. The same fallback handles unsigned
   native arguments above `MaxInt64`: pinned ClickHouse `26.3.17.4` proves
   that a native `MaxUint64` offset is otherwise reinterpreted as negative.
   The fallback computes in `Int128`, references the source once, and contains
   two singleton `arrayMap` calls rather than the original three.
9. Every call has a 64 KiB SQL ceiling in addition to the 256 KiB whole-query
   ceiling. Nested calls grow linearly, bind order remains source/start/length,
   and generated predicates omit generic Dynamic decimal, numeric, Boolean,
   and container branches.
10. Compiler unit coverage pins positive, zero, and negative starts; omitted,
    positive, zero, and negative lengths; Unicode; null and missing values;
    fixed and Dynamic types; multivalue rejection; `_raw` provenance; exact
    source occurrence; native fast paths; generic SQL shape; full-width
    indexes; bind order; nesting; source-located diagnostics; typed nils; and
    forged cycles.
11. The isolated ClickHouse corpus executes the full SQLite interval matrix
    on `😀abcdef`, including far clipping, `MinInt64`, `MaxUint64`, a
    `MaxUint64` length, and `MaxInt64+1` paired with `MinInt64`. It also covers
    unsupported Dynamic variants, canonical fixed String, predicates,
    binary `_raw`, and `EXPLAIN actions=1` proof of no `ArrayJoin`.
12. Independent reuse, quality, and efficiency reviewers found eight concrete
    improvements, all applied: shared text-input validation, shared integer
    literal recognition, shared forged-plan harnesses, reuse of the existing
    argument matcher, a real fixed-String integration case, native literal
    fast paths, removal of one fallback singleton array, and explicit
    full-width unsigned guarding. The unrestricted linter then found and
    drove five shadowing cleanups plus two proof-scoped conversion guards.
13. The compatibility contract cites Splunk, SQLite, and ClickHouse sources.
    Editor completion advertises the literal-index/SQLite boundary, while
    syntax highlighting recognizes `substr` only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero
issues. The frontend corpus contains 118 application tests and 47
release/build tests. The production static export generated all 11 pages, and
the final pinned Store/compiler run passed in 43.411 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `264211d`, `c167e58`, `726810f`,
   `1895e3a`, `47db412`, and `46ca648`. Preserve unexpected local changes.
2. Implement bounded scalar `tostring` next only after pinning Splunk's
   default, `hex`, `commas`, and duration-format contracts; fixed and Dynamic
   type behavior; canonical time behavior; multivalue rejection; and the
   exact ClickHouse lowering against the pinned target.
3. Keep concatenation, field-driven or fractional substring indexes,
   `mvcount`, implicit String conversion, locale-sensitive case mapping,
   normalization, and full case folding as separate compatibility slices.
4. Keep Dynamic/container `coalesce` and `case`, heterogeneous conditionals,
   wildcard count, broader conditional count names, and `eventstats` as
   separate reviewed contracts.

## Latest checkpoint: typed SPL UTF-8 `len` / `length`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`64004dcc45e329692aea83d0467cd2a26a5ca94c`

ClickHouse compiler, integration, and adversarial-review checkpoint (committed
and pushed):
`e3a32e2fad2dee31abd77d540f1fa43b1bd13b47`

Compatibility and editor checkpoint (committed and pushed):
`5aebc70d14057946aba3ff4a5cae5605cd5b27f8`

This test-first slice implements `len(value)` and its exact `length(value)`
alias:

1. The parser accepts either case-insensitive name with exactly one scalar
   argument, preserves the authored alias and source range in the AST,
   supports nesting and predicate use, and leaves bare fields named `len` or
   `length` ordinary.
2. Both names lower to one dedicated logical-plan enum. Parser, planner, and
   compiler trust boundaries reject missing/additional arguments, nil and
   typed-nil values, invalid enums, Boolean null-predicate escape, excessive
   depth/nodes, and cycles before recursive lowering.
3. The function counts UTF-8 code points, not bytes. Empty String returns
   zero. Fixed String returns `UInt64`; Dynamic runtime String returns
   nullable `UInt64`. Missing, explicit null, and Dynamic runtime numbers,
   Booleans, arrays, objects, or other containers return null.
4. Fixed numeric, Boolean, and canonical time input fails with the
   source-located `SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE` diagnostic rather
   than applying an implicit conversion.
5. Splunk documents `len` as scalar-only. Fixed `Array(String)` fails with
   `SPL_UNSUPPORTED_MULTIVALUE_USAGE`; Dynamic runtime arrays return null.
   The function never maps members, applies `ARRAY JOIN`, or changes event
   cardinality.
6. Fixed String and `_raw` inputs share the text-function provenance helper
   introduced by the Unicode case slice. Binary-declared `_raw` therefore
   returns null even when its bytes are ASCII, and `replace(_raw, ...)`
   retains the same guard.
7. ClickHouse lowering uses `lengthUTF8`. Dynamic input compiles directly to
   `lengthUTF8(dynamicElement(value, 'String'))`: pinned ClickHouse
   `26.3.17.4` proves the extraction is `Nullable(String)`, returns null for
   String-mismatched variants, and produces `Nullable(UInt64)` without a
   runtime type branch or singleton array.
8. Generated SQL references each Dynamic source once, omits generic Dynamic
   numeric/decimal/Boolean comparison branches when compared with fixed
   numeric literals, and grows linearly through nested text functions. Every
   call has a separate 64 KiB SQL ceiling in addition to the 256 KiB
   whole-query ceiling.
9. Compiler unit coverage pins both aliases, Unicode literals, fixed and
   Dynamic String inputs, empty/null/missing values, Dynamic non-strings and
   containers, fixed multivalue rejection, binary `_raw`, replace provenance,
   numeric predicates, exact source occurrence, bind order, nested SQL growth,
   source-located diagnostics, typed nils, and forged cycles.
10. The isolated ClickHouse corpus executes `München`/`Straße` code-point
    counts, both aliases, sequential nesting with `lower`, empty String,
    Dynamic unsupported types, fixed String output from `stats min(host)`,
    numeric `where` predicates, binary `_raw`, and `EXPLAIN actions=1` proof
    of no `ArrayJoin`. It runs with common-Variant inference disabled and
    short-circuit evaluation enabled.
11. Independent reuse, quality, and efficiency review found four concrete
    improvements, all applied: lower/upper and len now share unary text-input
    validation and provenance helpers; forged unary compiler plans share one
    trust-boundary harness; the fixed-String integration case now truly uses
    `min(host)` rather than a Dynamic event field; and Dynamic len uses direct
    nullable extraction rather than a singleton `arrayMap`.
12. The compatibility contract cites Splunk's official text-function
    semantics and ClickHouse's `lengthUTF8` reference. Editor completion
    advertises code-point behavior, while syntax highlighting recognizes
    `len` and `length` only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero issues.
The frontend corpus contains 117 application tests and 47 release/build tests.
The production static export generated all 11 pages, and the final pinned
Store/compiler run passed in 45.380 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `64004dc`, `e3a32e2`, and
   `5aebc70`. Preserve any unexpected local changes.
2. Implement bounded scalar `substr` next only after pinning its SQLite-style
   positive/zero/negative start and optional positive/zero/negative length
   semantics, UTF-8 indexing, null/type behavior, and resource limits against
   the pinned ClickHouse target.
3. Keep multivalue substring behavior, concatenation, `mvcount`, implicit
   String conversion, locale-sensitive case mapping, normalization, and full
   case folding as separate compatibility slices.
4. Keep Dynamic/container `coalesce` and `case`, `tostring`, heterogeneous
   conditionals, wildcard count, broader conditional count names, and
   `eventstats` as separate reviewed contracts.

## Previous checkpoint: typed Unicode SPL `lower` and `upper`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`8e68c7e7516833e4975b4dc31c47f41ce3dcbcdb`

Initial ClickHouse compiler checkpoint (committed and pushed):
`3d9d5f86537cb8e938c20ed27c7ac9bff25941b2`

Pinned integration and adversarial-hardening checkpoint (committed and pushed):
`53b1f5564aac8bdc52e5b1085eecd05f7038ab6d`

Final type contract, compatibility, and editor checkpoint (committed and
pushed):
`8e4cf5fb8a9ab1795a227b98347032ee651d5b20`

This test-first slice implements Unicode-aware `lower(value)` and
`upper(value)`:

1. The parser accepts case-insensitive calls with exactly one scalar argument,
   preserves source ranges, supports nesting and sequential eval assignments,
   and leaves bare fields named `lower` or `upper` ordinary. Missing,
   additional, malformed, over-depth, and over-budget arguments fail before
   planning.
2. Dedicated logical-plan enum values retain the call through the typed scalar
   IR. Parser, planner, and compiler trust boundaries reject invalid enums,
   zero/two-argument forged calls, nil and typed-nil arguments, Boolean
   null-predicate escape, excessive nodes/depth, and cycles before recursive
   lowering.
3. The fixed input contract follows Splunk's documented string boundary:
   fixed `String`, fixed `Array(String)`, missing, and null are supported.
   Fixed numeric, Boolean, and canonical time input fails with the
   source-located `SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE` diagnostic rather
   than silently applying the separate `tostring` conversion.
4. Runtime Dynamic `String`, `Array(String)`, and homogeneous all-String
   `Array(Dynamic)` are supported. Array(Dynamic) is normalized to
   Array(String). Runtime numbers, Booleans, objects, missing/null values,
   heterogeneous arrays, and null-containing arrays fail closed to Dynamic
   None rather than inheriting ClickHouse's generic conversion behavior.
5. Multivalue conversion is member-wise and preserves order and cardinality.
   It uses no ordinary `ARRAY JOIN`, event-row expansion, sorting, or
   aggregation. Fixed arrays bind once, validate every member with
   `isValidUTF8`, and publish the existing empty/logically-absent multivalue
   representation when invalid.
6. Scalar `_raw` conversion uses its stored UTF-8 provenance guard, so
   binary-declared raw bytes return null even when the bytes happen to be
   ASCII. `replace` now retains that provenance. Statistics String
   normalization carries the same guard into `values(_raw)` and `list(_raw)`,
   preventing an aggregate from laundering binary raw bytes into a UTF-8
   function.
7. ClickHouse lowering uses `lowerUTF8` and `upperUTF8` on the pinned
   `26.3.17.4` target. The contract is Unicode-aware but not locale-aware
   collation, normalization, or full case folding, and records ClickHouse's
   documented caveat for mappings whose encoded byte length changes.
8. Eval/where comparisons remain case-sensitive after conversion. Dynamic
   text results carry a narrow text-only domain marker, so direct comparisons
   bind each operand once and omit the generic Dynamic numeric, extended
   decimal, and Boolean branches. Fixed and Dynamic multivalue results remain
   unsupported in ordinary scalar comparisons.
9. Dynamic and fixed multivalue calls bind each child expression through a
   singleton higher-order expression. Nested calls therefore grow linearly,
   and tests pin one source occurrence. Every call has a 64 KiB generated-SQL
   ceiling in addition to the 256 KiB whole-query ceiling.
10. Compiler unit coverage pins fixed and Dynamic strings, null, unsupported
    fixed types, Dynamic non-strings and containers, physical Array(Dynamic),
    fixed and Dynamic multivalues, binary raw, replace provenance,
    aggregate-raw provenance, case-sensitive predicates, single-evaluation
    shape, bind order, linear nesting, no row expansion, exact diagnostics,
    typed nils, and forged cycles.
11. The isolated ClickHouse corpus executes Unicode scalars (`MÜNCHEN`,
    `Straße`), sequential calls, Dynamic multivalues, fixed stats
    multivalues, unsupported runtime types, missing/null values, case-sensitive
    predicates, binary `_raw`, binary raw through `values` and `list`, and
    `EXPLAIN actions=1` proof of no `ArrayJoin`. It runs with common-Variant
    inference disabled and short-circuit evaluation enabled.
12. Independent reuse, quality, and efficiency review found three concrete
    issues and each was fixed: duplicated integration parse/build/compile
    setup now shares an index-parameterized helper; fixed String arrays now
    retain the raw UTF-8 safety boundary; and direct Dynamic text comparisons
    no longer duplicate the full conversion or retain irrelevant type
    branches. The final documentation review additionally removed implicit
    fixed numeric/Boolean stringification.
13. The compatibility contract cites Splunk's official text-function
    reference and ClickHouse's String-function reference. Editor completion
    advertises both exact signatures and Unicode/multivalue behavior, while
    highlighting recognizes them only in function position.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero issues.
The frontend corpus contains 116 application tests and 47 release/build tests.
The production static export generated all 11 pages, and the final pinned
Store/compiler run passed in 46.628 seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `8e68c7e`, `3d9d5f8`, `53b1f55`,
   and `8e4cf5f`. Preserve any unexpected local changes.
2. Select the next bounded eval slice from `len`/`substr` or
   `round`/`ceil`/`floor` only after writing its executable type, null,
   Dynamic, multivalue, Unicode/precision, and resource contract.
3. Keep locale-sensitive case mapping, Unicode normalization, full case
   folding, and broader implicit string conversion as explicit future
   compatibility work requiring differential evidence.
4. Keep Dynamic/container `coalesce` and `case`, `tostring`, heterogeneous
   conditionals, wildcard count, broader conditional count names, and
   `eventstats` as separate reviewed contracts.

## Previous checkpoint: ordered typed SPL `case`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`1ee3b7e3e12fc4758474878785db4dbdd3a37319`

ClickHouse compiler and pinned execution checkpoint (committed and pushed):
`ae24b1c4b83fd18b2641561aaef0b35fab7f1ec5`

Compatibility, editor, and checkpoint update (committed and pushed):
`f1f2e8348e9034bca8a8a114072f1d745561b1d9`

Repository-wide Go lint cleanup checkpoints (committed and pushed):
`53a4dccb9ea1606bb203e3d6a498bca3ad410235`,
`b9c61ed60b9e6538855b560768436850174e8c52`, and
`caadf3f813be63b06bf9c65662dda397a68a8f37`

This test-first slice implements the bounded ordered conditional selector
`case(predicate, value, ...)`:

1. The parser accepts case-insensitive `case` with one through 16 alternating
   condition/value pairs. It preserves pair order and source ranges, requires
   even arity, leaves a bare field named `case` ordinary, and rejects malformed
   separators, missing values, implicit predicate adjacency, excess pairs, and
   excess nesting with source-located diagnostics.
2. Conditions use the current eval/where grammar and precedence: comparisons,
   direct `isnull`/`isnotnull`, parentheses, and explicit `NOT`, `AND`, and
   `OR`. Their leaves share the query-wide 32-predicate budget with earlier
   `where`, `if`, `case`, and `count(eval(...))` expressions.
3. Dedicated ordered AST and logical-plan branches carry predicates separately
   from scalar values. Both trust boundaries reject zero/over-limit branches,
   nil and typed-nil conditions or values, invalid predicate structure,
   excessive depth/nodes, and cycles before recursive conversion or SQL
   generation.
4. Conditions are considered from first to last. The value paired with the
   first Boolean true condition is selected; false or null continues. No true
   condition publishes a present typed null. The current grammar uses an
   explicit always-true comparison such as `1=1` for a default because
   `true()` is not yet a supported eval function.
5. Values use the stable fixed-scalar tier shared with `if` and `coalesce`:
   `String`, `Bool`, `UInt8`, `Int64`, `UInt64`, and `Float64`. Non-null values
   must have one exact type, including numeric width/sign. Statically null
   values and the implicit default adopt it; all-null values become nullable
   String.
6. Dynamic values, fixed multivalues, canonical time, mixed kinds, differing
   numeric types, and incompatible String provenance fail with the
   source-located `SPL_UNSUPPORTED_CASE_VALUE_TYPE` diagnostic. The compiler
   never delegates these cases to ClickHouse `Variant` or common-type
   inference.
7. Boolean recognition is consumer-aware. A case whose non-null values are
   syntactically Boolean may feed `where`, another conditional, or
   `count(eval(...))`. Plain Bool literals remain assignable, while a Boolean
   null-predicate result cannot escape through a value into direct `eval`,
   `tonumber`, or `replace` consumption.
8. The compiler emits one parameterized
   `multiIf(ifNull(condition, 0), value, ..., typed_null_default)`, preserving
   alternating condition/value bind order and calculated-field
   materialization. It performs no ordinary `ARRAY JOIN` or event-row
   expansion.
9. Each case has an incremental 64 KiB generated-SQL ceiling in addition to
   the 256 KiB whole-query ceiling. The compiler validates branch count,
   structural nodes, depth, and active-node cycles before materialization
   discovery or recursive lowering.
10. Unit coverage pins ordered pairs and bindings, predicate precedence,
    fixed/nullable values, implicit and all-null defaults, Boolean consumers,
    projected values, raw provenance, calculated-field materialization,
    incompatible types, malformed/over-limit pairs, typed nils, cycles, and
    variadic SQL growth. Adversarial review additionally fixed preservation of
    bindings inside all-null conditional values for both case and coalesce.
11. Pinned ClickHouse `26.3.17.4` coverage runs with common-Variant inference
    disabled and executes first-true ordering, null-condition fallthrough,
    implicit null, explicit default, Boolean filtering, `Int64`, `UInt8`,
    `UInt64`, `Float64`, sequential assignments, calculated-field
    materialization, and `EXPLAIN actions=1` proof of no row expansion.
12. The compatibility contract cites Splunk's official conditional-function
    reference and documents exact arity, ordering, default, type, provenance,
    Boolean, unsupported Dynamic/container, execution, and resource behavior.
    Editor completion and highlighting advertise case only in function
    position, leaving a field named `case` ordinary.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --max-issues-per-linter=0 --max-same-issues=0
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1
git diff --check
```

All gates pass. The unrestricted repository-wide lint run reports zero issues,
down from 1,376 at the start of the requested cleanup; no blanket G115
exclusion was added. The frontend corpus contains 115 application tests and 47
release/build tests. The production static export generated all 11 pages, and
the final pinned Store/compiler run passed in 43.264 seconds.

The lint cleanup's independent reuse, quality, and efficiency reviews traced
the checked-conversion invariants through their producers and consumers. They
drove a shared precomputed log-generator/pacer ordinal schedule, one validated
ingest batch count per response, and removal of a redundant outbox length
conversion. The quality rereview found no remaining concrete correctness or
maintainability issue.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `1ee3b7e`, `ae24b1c`, `f1f2e83`,
   and the zero-lint cleanup through `caadf3f`. Preserve any unexpected local
   changes.
2. Select the next bounded eval slice from `lower`/`upper`/`len`/`substr` or
   `round`/`ceil`/`floor` only after writing its executable type, null,
   Dynamic, multivalue, precision, Unicode, and resource contract.
3. Keep Dynamic/container `coalesce` and `case` as explicit future typed-union
   slices. They require either bounded object reconstruction or durable
   selected-parent metadata.
4. Keep `tostring`, heterogeneous conditionals, wildcard count, broader
   conditional count names, and `eventstats` as separate reviewed contracts.

## Previous checkpoint: typed SPL `coalesce`

Date: 2026-07-27

Parser and logical-plan checkpoint (committed and pushed):
`a9e0ffe618593230695c9f9150af082ec6867275`

ClickHouse compiler and pinned execution checkpoint (committed and pushed):
`a26106da5ff5bb4c61cabed19c261c8ac138eb94`

This test-first slice implements the bounded conditional value selector
`coalesce(value, ...)`:

1. The parser accepts case-insensitive `coalesce` with one through 32 scalar
   arguments, preserves source ranges and argument order, and shares the
   existing 32-level scalar and query-wide predicate budgets. The planner
   carries the ordered call through the typed scalar IR and independently
   rejects zero/over-limit arity, invalid enums, typed-nil arguments, cycles,
   excessive depth, and oversized shared DAGs.
2. Selection is left to right and stops at the first non-null value. Explicit
   null, failed nullable conversion, and statically missing fixed-schema values
   are skipped. Empty String, numeric zero, and Boolean false are retained.
   One argument is an identity; all-null input publishes a present nullable
   String null.
3. Version 0.1 admits stable fixed `String`, `Bool`, `UInt8`, `Int64`,
   `UInt64`, and `Float64` values. Every non-null argument must have the same
   exact type, including numeric width/sign. Statically null arguments adopt
   that type without erasing bindings from a live null-producing expression.
4. Dynamic values, fixed multivalues, canonical time, mixed kinds, differing
   numeric types, and incompatible String provenance fail with the
   source-located `SPL_UNSUPPORTED_COALESCE_VALUE_TYPE` diagnostic. This
   prevents accidental ClickHouse `Variant` inference, numeric widening,
   binary `_raw` parsing, or incomplete flattened-object reconstruction.
5. Boolean recognition is consumer-aware. A coalesce whose non-null arguments
   are syntactically Boolean can be consumed by `where`, an `if` condition, or
   `count(eval(...))`. Plain Bool literals remain assignable, while
   `isnull`/`isnotnull` results cannot escape through coalesce into direct
   `eval`, `tonumber`, or `replace` consumption.
6. The compiler emits one parameterized scalar `coalesce(...)`, preserves
   source occurrence order for bindings, carries calculated-field
   materialization and matching text provenance, and performs no ordinary
   `ARRAY JOIN` or event-row expansion.
7. Each call has an incremental 64 KiB generated-SQL ceiling in addition to
   the 256 KiB whole-query ceiling. The regression corpus distinguishes this
   variadic bound from the existing per-`if` bound and proves a bounded form
   remains under the whole-query ceiling.
8. Compiler trust-boundary validation now runs before filter materialization
   discovery as well as before eval compilation. Forged cyclic scalar graphs
   therefore fail before any recursive field walk, Boolean classification, or
   SQL expansion.
9. Unit coverage pins ordered binds, fixed and nullable types, Boolean
   consumers, all-null and projected-away values, raw-text provenance,
   calculated-field materialization, incompatible types, zero/over-limit
   arity, typed nils, cycles, and variadic SQL growth.
10. Pinned ClickHouse `26.3.17.4` coverage runs with common-Variant inference
    disabled and executes missing/null selection, empty String, zero, false,
    `Int64`, `UInt8`, `UInt64`, `Float64`, all-null, one-argument identity,
    sequential assignments, projected values, Boolean filtering,
    calculated-field materialization, and `EXPLAIN actions=1` proof of no row
    expansion.
11. The compatibility contract cites Splunk's official conditional-function
    reference and documents exact syntax, null/type/provenance behavior,
    Boolean boundaries, unsupported Dynamic/container inputs, and resource
    limits. Editor completion and highlighting advertise coalesce only in
    function position, leaving a field named `coalesce` ordinary.
12. Adversarial correctness/performance review drove the pre-materialization
    cycle guard, preservation of bindings inside typed all-null expressions,
    the independent variadic SQL-growth regression, and pinned nullable and
    integer-width execution probes.

Validation completed on the current implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
git diff --check
```

All gates pass. The frontend corpus contains 114 application tests and 47
release/build tests. The final pinned Store/compiler run passed in 45.83
seconds.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `a26106d` plus the
   compatibility/editor/checkpoint commit that follows it. Preserve any
   unexpected local changes.
2. The next bounded eval slice should be selected from `case`,
   `lower`/`upper`/`len`/`substr`, or `round`/`ceil`/`floor` only after writing
   its executable type, null, Dynamic, multivalue, precision, Unicode, and
   resource contract. `case` can reuse the fixed conditional type library but
   must separately pin missing default, first-true selection, alternating
   arity, and Boolean consumer behavior.
3. Keep Dynamic/container coalesce as an explicit future typed-union slice.
   It requires either bounded object reconstruction or durable selected-parent
   metadata; do not silently treat a flattened object parent as null.
4. Keep `tostring`, heterogeneous `if`, wildcard count, broader conditional
   count names, and `eventstats` as separate reviewed contracts.
5. The broader backend/product backlog and safe-resume procedure remain at the
   end of this document.

## Previous checkpoint: typed SPL `stats count(eval(...))`

Date: 2026-07-27

Implementation, tests, compatibility, and editor checkpoint (committed and
pushed):
`66b2b16c88f1e7a41bb5486dffe5942629969097`

This test-first slice implements the bounded conditional aggregate
`stats count(eval(<predicate>)) AS <field>`:

1. The parser accepts case-insensitive outer `count` and inner `eval`, requires
   one current eval/where predicate and an explicit unquoted `AS` output, and
   rejects `c(eval(...))`, inferred expression names, implicit predicate
   adjacency, multiple arguments, and malformed parentheses with source-
   located diagnostics.
2. The predicate uses the existing typed grammar and precedence: comparisons,
   direct `isnull`/`isnotnull`, nested Boolean `if`, parentheses, `NOT`, `AND`,
   and `OR`. Conditional measures share the query-wide 32-predicate ceiling
   with earlier `where`/`if` expressions and the ordinary 16-measure stats
   ceiling.
3. Dedicated AST and logical-plan variants carry a mutually exclusive
   predicate rather than overloading the aggregate input field. Planner and
   compiler defenses reject nil/typed-nil predicates, field-plus-predicate or
   percentile metadata, non-predicate functions, invalid operators, bad
   scalar arity/metadata, and open-event use of the reserved `fields` payload.
   A closed transforming schema may still expose an ordinary field named
   `fields`.
4. Count semantics are true-only: Boolean true contributes one; false, null,
   and a comparison involving a missing value contribute zero. `isnull` and
   `isnotnull` retain their documented missing/null behavior. A global
   aggregation over no rows returns non-null `UInt64(0)`.
5. The predicate is a measure, never an aggregate-wide prefilter. Sibling
   counts and other measures see every scoped row, and grouped results retain
   groups whose conditional contribution is zero.
6. Each distinct compiled `{SQL, arguments}` predicate is lowered once to a
   non-null per-row `UInt64` contribution. Exact repeated predicates share
   that contribution; predicates with identical SQL shape but different
   arguments do not. Arguments remain in first-use, left-to-right occurrence
   order.
7. Aggregation sums the contribution through `UInt128` and publishes
   `UInt64`; the production source-row ceiling bounds the final conversion.
   Ordinary conditional counts add neither `WHERE` nor `ARRAY JOIN`.
8. Predicates over calculated Dynamic fields reuse one materialized input
   fence and singleton binding set for the aggregate stage. Pinned server
   execution caught and fixed a ClickHouse alias-scope issue by projecting the
   bound aliases explicitly from that fence; bindings remain private and die
   at the immediately following transforming aggregate.
9. Parser/planner/compiler unit tests cover casing, precedence, exact and
   over-limit budgets, bind order, repeated physical sharing, siblings,
   grouping, projection, calculated fields, reserved fields, output metadata,
   missing/null behavior, empty input, and forged invariants.
10. Pinned ClickHouse `26.3.17.4` coverage executes canonical and Dynamic
    comparison matches/misses, the true/false/null/missing truth table,
    Boolean composition, nested and nullable `if`, grouped zero contributions,
    sibling row counts, projection, calculated `spath`, global empty input,
    and `EXPLAIN actions=1` proof that the ordinary path has no row expansion.
11. Both AST-to-plan and plan-to-SQL trust boundaries now revalidate bounded
    predicate occurrence count and depth, track active nodes to reject cycles,
    and charge compact shared DAGs by occurrence. Aggregate cardinality is
    rejected before predicate preprocessing. These guards prevent forged plans
    from reaching unbounded recursive validation, materialization discovery,
    or SQL expansion.
12. Compatibility documentation and editor completion advertise the exact
    explicit-alias form and true-only behavior. Nested `eval(` inside stats is
    highlighted as a function without relabeling the pipeline `eval` command.

Validation completed on the committed implementation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./...
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run typecheck
npm run lint
npm run test:frontend
npm run build
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -v
git diff --check
```

All gates pass. The frontend corpus contains 113 application tests and 47
release/build tests. The final pinned Store/compiler run passed in 43.34
seconds. Independent contract/correctness, compiler/performance, test/security,
and post-fix code-quality reviews drove fixes for invalid planner enums and
function arity, real field-comparison coverage, query-wide predicate-budget
coverage, early aggregate-cardinality enforcement, and cycle/depth/shared-DAG
resource bypasses at both trust boundaries. Their final current-tree reviews
are clean.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `66b2b16` plus the checkpoint-doc
   commit that follows it. Preserve any unexpected local changes.
2. The recommended next bounded expression slice is `coalesce(value, ...)`,
   which is in the product plan and builds directly on the now-tested
   missing/null and fixed-branch machinery. Before implementation, write the
   official-source contract for arity, first-non-null selection, empty/false/
   zero behavior, type and multivalue compatibility, evaluation order, and
   resource ceilings.
3. Start with red parser/planner tests, then compiler tests, then pinned
   ClickHouse execution. Keep the accepted arity and scalar nesting within the
   existing query budgets, and reject unstable Dynamic/container unions rather
   than inheriting ClickHouse type inference accidentally.
4. Keep `case`, `tostring`, numeric/string eval functions, wildcard count,
   inferred conditional-count output names, `c(eval(...))`, and `eventstats`
   as separate reviewed contracts. `eventstats` should wait for a stable
   reusable aggregate library.
5. Heterogeneous/Dynamic/container `if` branches remain a separate follow-on
   contract. Flattened object parents still require a durable hidden
   selected-descendant representation or explicit bounded reconstruction.
6. The broader backend/product backlog and safe-resume procedure remain at the
   end of this document.

## Previous checkpoint: typed SPL `if` end to end

Date: 2026-07-27

Parser/planner checkpoint (committed and pushed):
`cfaa75bf3b6aa520dde94c6ca209f72cc1a800db`

ClickHouse compiler and pinned integration checkpoint (committed and pushed):
`c1ad25b93223204df10f3fbb4ed37061f9842f3f`

Compatibility/editor checkpoint (committed and pushed):
`fed32762ed3bf8e22994383ea5f4aa401f375b5a`

This test-first slice implements and advertises the bounded Boolean consumer
`if(condition, true_value, false_value)`:

1. The parser has a dedicated `ScalarIfExpr`; the first argument is the
   existing typed `WhereExpr`, not an ordinary scalar argument. It therefore
   supports comparisons, direct `isnull`/`isnotnull`, parentheses, `NOT`,
   `AND`, and `OR` with the same eval/where precedence and no invented scalar
   truthiness. Function names remain case-insensitive.
2. The logical plan mirrors that separation with `ScalarIfExpression` and
   lowers its condition through the existing backend-neutral predicate IR.
   The true and false branches remain typed scalar expressions.
3. `if` requires exactly three arguments and supports nested conditionals.
   Predicates inside `if` and later `where` commands share one global
   32-predicate ceiling; scalar nesting retains the 32-level ceiling. Tests
   pin exact-limit, over-limit, and cross-command behavior.
4. Boolean validation is consumer-aware. A null predicate consumed as the
   condition of an `if` no longer poisons an otherwise scalar result, so
   `tonumber(if(isnull(x), "0", "1"))` is valid. A null-predicate result that
   can escape through a result branch is still rejected by search-mode
   `eval` and by current non-Boolean consumers. Existing plain Bool literal
   scalar behavior is preserved; a statically Bool-valued `if` may be used as
   a direct predicate, while bare `where true` remains outside this tier.
5. Parser diagnostics cover malformed commas, implicit predicate adjacency,
   unsupported Boolean operators such as `XOR`, missing arguments, and
   non-predicate first arguments. Planner defense-in-depth rejects nil and
   typed-nil conditions/branches plus forged invalid Boolean operators rather
   than panicking or silently treating them as `AND`.
6. The ClickHouse compiler lowers one scalar
   `if(ifNull(condition, 0), true_value, false_value)`. Only Boolean true
   selects the true branch; false or null selects the false branch. Bind
   arguments remain in condition/true/false occurrence order, and `if` does
   not expand multivalue members or multiply rows.
7. Version 0.1 admits stable fixed unions only: String/String, Bool/Bool, and
   identical `UInt8`, `Int64`, `UInt64`, or `Float64` numbers. A literal,
   calculated, projected-away, declared-missing, renamed, or grouped value
   proven always null adopts the other fixed branch type; null/null is
   nullable String until an enclosing conditional supplies a fixed type.
   Dynamic, fixed multivalue, time, mixed-kind, and differing-number branches
   fail with `SPL_UNSUPPORTED_IF_BRANCH_TYPE` rather than inheriting
   ClickHouse `Variant` or common-supertype inference.
8. String branch metadata is fail closed. Identical text eligibility
   propagates, and a null branch adopts the other branch's eligibility.
   Mixing binary-sensitive `_raw` with an ordinary String is rejected so a
   later `spath` or other text consumer cannot parse bytes as UTF-8.
9. Predicate materialization traverses conditions and both branches.
   Calculated Dynamic fields retain the existing materialized fence, while
   the conditional itself adds no ordinary `ARRAY JOIN`. Production execution
   pins ClickHouse short-circuit evaluation, subject to the documented static
   type-analysis/constant-folding caveat.
10. Every compiled conditional scalar has an incremental 64 KiB generated-SQL
    ceiling in addition to the 256 KiB whole-query ceiling. This stops nested
    conditionals and Dynamic comparisons from producing geometric compiler
    allocations before final query validation.
11. Pinned ClickHouse `26.3.17.4` coverage executes missing/null/empty/zero/
    false/container conditions, String labels, Boolean predicates, nullable
    String and Bool, Int64/UInt8/UInt64/Float64, nesting, sequential eval,
    projection, rename, known-missing fields, calculated-field fences, and
    no-row-expansion plans. A separate binary `_raw`/`spath` regression pins
    text eligibility through an identical-branch conditional.
12. Editor completion advertises the exact three-argument form and true-only
    selection. Highlighting recognizes case-insensitive `if` only when
    followed by `(`, so a field named `if` remains ordinary text. The
    compatibility contract records all admitted and rejected forms.

Validation completed across these checkpoints:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go test ./... -count=1
go vet ./internal/spl ./internal/plan ./internal/clickhouse
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run typecheck
npm run lint
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -v
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestSpathAgainstClickHouse$' -count=1 -v
git diff --check
```

All local gates pass; the frontend corpus contains 111 application tests.
Independent parser/planner, compiler correctness, compiler performance,
test-quality, and compatibility/editor reviews drove fixes for typed-nil and
invalid operators, consumer-aware Boolean classification, mixed predicate
budgets, raw-text provenance, geometric SQL growth, all-null type adoption
through eval/projection/rename/stats, compiler-known missing values, missing
fixed-type runtime cases, calculated-field execution, and non-vacuous
assertions. Their final current-tree findings are clean.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `fed3276` plus the checkpoint-doc
   commit that follows it. Preserve any unexpected local changes.
2. The next bounded aggregate slice is
   `stats count(eval(<predicate>)) AS <field>`. Start with red parser/planner
   tests and require explicit `AS` in this first form. Accept only the current
   eval/where predicate grammar; defer `c(eval(...))`, wildcard count,
   arbitrary scalar truthiness, default expression-derived names, and
   `eventstats`.
3. Count exactly Boolean true. False or null contributes zero. Conditional
   count must be a measure, never an aggregate pre-filter: sibling measures
   and grouping must still see every scoped row. Reuse the query-wide
   32-predicate and 16-measure ceilings.
4. Carry a mutually exclusive predicate input through the aggregate AST and
   plan, reject nil/typed-nil/field-plus-predicate forged states, and compile
   the row predicate once into a non-null `UInt64` count contribution.
   Preserve condition bind order and calculated-field materialization.
5. Add unit and pinned ClickHouse tests for true/false/null/missing,
   `NOT`/`AND`/`OR`, nested Boolean `if`, grouping, sibling row count,
   multiple/repeated measures, projected and calculated fields, exact
   argument order, no accidental `WHERE`, no ordinary row expansion, physical
   sharing where safe, and adversarial complexity/forged-plan paths.
6. Treat heterogeneous/Dynamic/container `if` branches as a separate follow-on
   contract. Pinned ClickHouse has `use_variant_as_common_type=true`, so
   implicit branch inference can produce `Variant` and violate compiler type
   metadata. Flattened object parents also need a durable hidden selected-
   descendant column or explicit reconstruction; do not propagate a
   conditional descendant expression across projections or use expensive
   JSON `.^` sub-object reads casually.
7. Keep broader `count` forms, `case`, `coalesce`, `tostring`, `eventstats`,
   and aggregate-library refactoring as distinct reviewed contracts rather
   than widening conditional count implicitly.

## Previous checkpoint: native SPL `isnull` / `isnotnull` predicates

Date: 2026-07-27

Implementation/test/compatibility/editor checkpoint (committed and pushed):
`2d35c6699c2bb5bb48a6d40d1e0795b2792c38bf`

This test-first slice implements the product-plan informational null functions
without inventing general scalar truthiness or search-mode Boolean assignment:

1. The parser accepts case-insensitive `isnull(value)` and
   `isnotnull(value)` with exactly one scalar argument. They can be used
   directly in `where`, composed with parentheses/`NOT`/`AND`/`OR`, or
   compared explicitly with a Boolean literal. Direct predicates and
   comparisons share the existing 32-predicate complexity ceiling.
2. `isnull` is true for a missing field or a scalar result that is null;
   `isnotnull` is its exact complement. Empty String, false, and numeric zero
   are present and non-null. Projected-away fields are missing, and failed
   `tonumber` results are null.
3. Empty fixed multivalue results use the existing logical-absence contract.
   At the explicit Open Splunk typed boundary, an exact Dynamic array is
   present even when it is empty or contains only null members. A flattened
   object parent is present when bounded descendant metadata exists. Null
   predicates never traverse members or expand event rows.
4. The logical plan carries a statically Boolean scalar predicate. The
   ClickHouse compiler lowers presence to exact-field existence plus
   `isNotNull(value)`, with the bounded descendant probe for flattened object
   parents. Presence placeholders remain attached to the scalar value in SQL
   occurrence order, and calculated fields retain the existing materialized
   predicate fence.
5. Search-mode SPL does not allow `eval flag=isnull(field)` to publish a raw
   Boolean. The parser and planner reject direct or nested assignment, and
   current `tonumber`/`replace` consumers reject nested null Booleans rather
   than inventing Bool-to-String coercion. Ordinary Bool literals and fields
   retain their preceding scalar-conversion behavior.
6. Compiler defense-in-depth rejects forged non-Boolean predicates, bad
   arities, forbidden Boolean consumers/assignments, typed-nil field/literal/
   call nodes, and typed-nil `replace` pattern/replacement nodes with errors
   instead of panics.
7. Editor completion advertises direct null predicates. Highlighting marks
   `isnull`/`isnotnull` only when followed by `(`, so identically named fields
   remain ordinary text.

Validation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go vet ./internal/spl ./internal/plan ./internal/clickhouse
go test ./... -count=1
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run typecheck
npm run lint
npm run test:frontend
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
    -run '^TestStoreAgainstClickHouse$' -count=1 -v
git diff --check
```

All gates pass. The frontend corpus now has 109 application tests. The pinned
ClickHouse `26.3.17.4` store/compiler suite passes with a dedicated nine-event
null matrix covering missing, explicit null, empty String, false, zero, empty/
null-only/nonempty Dynamic arrays, flattened objects, projection, nullable
conversion, fixed multivalues, complement laws, and an `EXPLAIN actions=1`
no-`ArrayJoin` assertion.

Three independent adversarial correctness, compiler/performance, and
test/quality reviews found and drove fixes for undocumented Bool coercion,
typed-nil forged-IR panics, a stale predicate-limit description, and three lint
ratchet failures. Their final current-tree verdicts are clean.

Immediate resume steps:

1. Confirm `main` and `origin/main` contain `2d35c66` plus the checkpoint-doc
   commit that follows it, then inspect the corresponding GitHub workflows.
2. Write the bounded executable contract and red tests for `if(condition,
   true_value, false_value)`, the product-plan Boolean consumer needed for
   documented forms such as
   `eval flag=if(isnull(field), "missing", "present")`. Pin condition grammar,
   branch type/null unification, missing values, nesting/complexity, bind
   order, calculated-field materialization, Dynamic/multivalue boundaries,
   forged IR, and pinned ClickHouse behavior before implementation.
3. Keep direct `eval flag=isnull(field)` rejected. Conditional
   `count(eval(predicate))`, wildcard count, and `eventstats` remain separate
   contracts; take them only after the Boolean-expression/consumer model is
   stable.
4. Preserve the no-row-expansion and immutable-scope invariants, and run the
   focused package, full Go/frontend, exact lint-ratchet, and pinned
   ClickHouse gates before the next push.

## Previous checkpoint: bounded SPL `c(field)` count abbreviation

Date: 2026-07-27

Implementation/test/docs/editor checkpoint:
`070d24fe6de1da8bc69b34ab4d8cb49341027f49`

This deliberately small test-first slice closes the documented SPL1 count
abbreviation without widening count semantics:

1. The parser accepts case-insensitive `c(field)` with one exact unquoted
   field and maps it to the existing `count(field)` AST and logical-plan
   function. Explicit `AS` is preserved; the default output is the documented
   Open Splunk canonical choice `count(field)`.
2. Bare `c`, `c()`, `count()`, `c(eval(...))`, wildcard `c(*)`/`c(prefix*)`,
   quoted inputs, and malformed arities remain explicit failures. Bare
   argument-free `count` remains the supported row-count form.
3. Compiler-equivalence tests prove that `c(field)` and `count(field)` emit
   identical SQL, arguments, schema, and runtime metadata. No new ClickHouse
   execution path or aggregate state was added.
4. Wildcard count remains deferred because SPL expands matching fields; it is
   never reinterpreted as row count. Open event schemas would require bounded
   runtime field discovery, while closed schemas need stable plan-time
   expansion, no-match, alias-rewrite, collision, and output-cap contracts.
5. Editor completion now advertises `c(field)`. Highlighting recognizes the
   one-letter abbreviation only when followed by `(`, so an ordinary field
   named `c` is not mislabeled as a function.

Validation:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse -count=1
go vet ./internal/spl ./internal/plan ./internal/clickhouse
go test ./... -count=1
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
npm run typecheck
npm run lint
npm run test:frontend
git diff --check
```

All gates pass. The frontend corpus now has 108 application tests, including
the focused `c` highlighter regression. Two independent primary-source SPL
contract and adversarial implementation reviews are clean.

Follow-on status:

The `isnull`/`isnotnull` work that was next here is complete at the newer
checkpoint above. Conditional `count(eval(predicate))`, wildcard count, and
`eventstats` remain separate bounded contracts.

## Previous checkpoint: bounded percentile family

Date: 2026-07-27

Parser/planner checkpoint (committed and pushed):
`efe41992ddec1f4c58e169a7b15772524e229ced`

Implementation/test/compatibility checkpoint:
`371f8815b54fe777dc574d39077150c9ca573a05`

CI lint repair:
`209f753746c2f653381496e97b349d27e081a359`

This slice implements the product-plan percentile family:

1. The parser and logical plan accept case-insensitive `pN(field)` and
   `percN(field)` for integer suffixes 1 through 99. Leading zeroes are
   accepted and canonicalized. The default output is always
   `percN(field)`, matching documented Splunk naming; explicit `AS` is
   preserved. Zero, 100, decimal suffixes, two-argument `perc(field, N)`,
   expressions, wildcards, and malformed arities fail explicitly.
2. The AST and plan carry a validated `uint8` suffix rather than a
   floating-point fraction. Builder and compiler defense-in-depth reject
   forged out-of-range levels and percentile metadata on other aggregates.
3. Every finite immediate numeric value participates: integers, floats,
   numeric Strings, tagged decimals, canonical timestamps, and immediate
   multivalue members. Duplicates remain separate observations. Missing,
   null, empty, Boolean, bytes, object, nonnumeric, nonfinite, and nested
   container values are ignored. Runtime Dynamic arrays and fixed
   multivalues are supported without row expansion.
4. Multiple levels over one exact input share one
   `quantilesGKOrNull(100, levels...)` or
   `quantilesGKOrNullArray(100, levels...)` state. Repeated synonyms such as
   `p50` and `perc50` share the same component. Inputs also share one numeric
   array normalization with `sum`/`avg`.
5. Statically scalar percentile-only inputs use the scalar multi-level GK
   path and avoid singleton-array/filter/map work. Dynamic, fixed
   multivalue, and same-input `sum`/`avg` consumers use the array path.
   Existing field-existence predicates and bind arguments are preserved.
6. `arrayElementOrNull` publishes nullable `Float64`; zero-row global input
   and retained all-ineligible groups are null, while grouped zero-row input
   emits no groups. Projected-away inputs remain absent.
7. Accuracy is fixed at 100 (approximately 1% rank error). Splunk uses
   different exact behavior for smaller distinct sets and a proprietary
   approximation for larger inputs, so exact values are a documented
   compatibility divergence. The 16-measure, group, row, thread, query-byte,
   and 1 GiB query-memory ceilings remain authoritative.
8. Compiler tests pin trusted level literals, scalar/array specialization,
   one state per input, separate states across inputs, synonym deduplication,
   source-order positions, `sum`/`avg` sharing in either measure order,
   forged plans, fixed and Dynamic multivalues, nulls, maximum measures, no
   `ARRAY JOIN`, and the compiled query ceiling.
9. The pinned store fixture covers p1/p50/p90/p95/p99/perc50 rank envelopes,
   grouped and global nulls, duplicate multivalue participation, ignored
   nested/nonnumeric members, fixed multivalue re-aggregation, and physical
   `EXPLAIN actions=1` state sharing for both scalar and array lowering. The
   manager fixture covers the whole family as nullable doubles.
10. The compatibility contract and editor completion/highlighting now expose
    generic bounded `pN`/`percN` rather than p95 alone.

Validation completed on the published implementation and lint repair:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse ./internal/queryexec -count=1
go vet ./internal/spl ./internal/plan ./internal/clickhouse ./internal/queryexec
go test ./... -count=1
npm run typecheck
npm run lint
npm run test:frontend
npm run build
golangci-lint run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1 -timeout=6m
git diff --check
```

The final pinned ClickHouse store suite, including scalar and Dynamic physical
`EXPLAIN actions=1` assertions, passed in 45.266 seconds before the lint repair
and in 46.839 seconds after it. The pinned manager/executor suite passed in
13.385 seconds. Full `go test ./... -count=1`, frontend typecheck, lint, tests,
and production build all passed. The exact lint ratchet reports zero issues.

GitHub run `30272617065` for `371f881` passed the GradeThis compatibility
corpus, frontend, vulnerability, backend-vertical, and protobuf jobs. Its only
failure was five static-analysis findings in the new integration test: a
checked fixture-length conversion, three shadowed error bindings, and helper
argument order. Commit `209f753` repairs those findings without changing
runtime semantics. Replacement run `30273254671` was in progress when this
handoff was first written; it subsequently completed successfully with every
job green, including lint, race/coverage, GradeThis, frontend, vulnerability,
backend-vertical, protobuf, and release checks.

Independent SPL-contract, code-inventory, correctness, efficiency,
performance, and reuse reviews drove the scalar specialization,
existence/bind preservation, physical-state regressions, and duplicate-test
removal. Final implementation correctness and reuse reviews are clean. A
separate adversarial review of the lint repair is also clean and reran both
the exact lint ratchet and the pinned store fixture.

Continuation after this checkpoint:

1. Replacement workflow `30273254671` is the fully green validation record.
2. The exact-field `c(field)` count abbreviation is complete at the newer
   checkpoint recorded above.
3. Keep `eventstats` behind the stable aggregate library. Decimal percentile
   suffixes, SPL2 two-argument `perc`, `upperperc`, and `exactperc` remain
   separate future contracts.

## Latest checkpoint: bounded chronological `stats earliest/latest`

Date: 2026-07-27

Planning commit:
`932f4036e2967d5304a95b27b7109e15ffcbf601`

Initial compiler implementation:
`ac721fb1d84f746d8783b02a8e1b5ac13fef14f3`

Pinned semantic corpus:
`e6acd1d01ef5ff13608f6cd551e0d3d817debfaf`

Atomic execution hardening:
`9714c795ab93c334120e7391057f72297748164c`

Bounded multivalue, terminal-output, and bind-order hardening:
`f9985a1184b43a78a5ae5ef8761c9ff649ec1836`

This checkpoint completes the first chronological aggregate slice:

1. SPL parsing and planning accept case-insensitive `earliest(field)` and
   `latest(field)` with one exact unquoted field, canonical default aliases,
   explicit `AS`, and source-located diagnostics. `first`, `last`,
   `earliest_time`, `latest_time`, quoted/wildcard/expression inputs, and
   malformed arities remain unsupported.
2. Chronology uses the original event `_time`, then event ID, immutable
   visibility sequence, private
   `(index, collector, batch sequence, batch ID)` source identity, and
   multivalue member ordinal. Filters, `head`, `tail`, and `dedup` determine
   the survivor set; a pipeline `sort` does not replace event chronology.
   Planning and compiler defense-in-depth both require an unmodified visible
   canonical `_time`.
3. Missing fields, explicit nulls, empty multivalues, and null multivalue
   members contribute nothing; an empty String is eligible. `earliest`
   selects the first eligible member of its winning event and `latest` the
   last. Global empty input emits one null row, retained all-null groups emit
   null, and grouped empty input emits no rows.
4. Supported scalar values retain the shared canonical string/bytes
   representation in nullable `Dynamic` output. Nested arrays, objects,
   generic containers, flattened object parents, and unsupported immediate
   multivalue members fail the complete scoped aggregate atomically even when
   they are not the winner or a downstream command hides every result row.
5. Dynamic multivalues are reduced per event to a constant-size tuple: first
   eligible value, last eligible value, eligible-member count, and one invalid
   bit. Bounded `arrayFirst`, `arrayLast`, `arrayCount`, and `arrayExists`
   passes avoid a normalized member copy, per-member row keys, row expansion,
   `groupArray`, windows, or Array aggregate combinators. The scalar
   `argMinOrNullIf`/`argMaxOrNullIf` states remain constant-size.
6. Repeated aliases share candidate normalization and one winner state per
   input/direction. A 2,000,000-member pinned regression consumes all four
   candidate components under an exact 1 GiB ClickHouse query-memory limit.
   Sixteen distinct Dynamic inputs compile below the 256 KiB SQL ceiling.
7. Runtime validation inputs and the completed downstream result are
   materialized once. A validation envelope joins the whole-result check
   before publishing ordinary, `chart`, or defensive `timechart` output and
   retains a zero-row validation branch, preventing ClickHouse from optimizing
   poison away behind a false filter or empty terminal result.
8. Every detached validation barrier owns exactly its SQL bind arguments.
   Final arguments are rebuilt in emitted CTE-definition order before the
   downstream relation's arguments. Runtime `eval` binding and a synthetic
   two-barrier unit regression pin this invariant.
9. Parser, planner, compiler, transport, and pinned ClickHouse tests cover
   chronology versus value/sort order, every tie-breaker at one and four
   threads, multivalue/null/empty behavior, all supported scalar types,
   invalid UTF-8 bytes, scope and incomplete-`BY` poison, downstream
   projection/filtering, terminal `chart`, nested list/object poison, shared
   states, global/grouped empties, and high-cardinality memory behavior.
10. Independent correctness, performance, and code-quality reviews found and
    drove fixes for optimizer-pruned validation, terminal `chart` bypass,
    unbounded `arrayFold` memory, materialized-CTE bind ordering, and missing
    live nested-member coverage. Their final current-tree verdicts report no
    remaining P1/P2 blocker.

The exact local validation record is under **Latest validation evidence**.
The documented `c(field)` abbreviation and native `isnull`/`isnotnull`
predicates are complete at the newer checkpoints above. The next product-plan
SPL work is a bounded compatible Boolean consumer, starting with an explicit
`if` contract; conditional and wildcard count remain later separate contracts,
followed by `eventstats` only after the aggregate and Boolean-expression
libraries are stable.
The optional direction-aware Dynamic selector optimization remains nonblocking
cleanup. The inherited lint inventory was subsequently eliminated by
`53a4dcc`, `b9c61ed`, and `caadf3f`. The overall backend goal remains active.

## Previous checkpoint: cumulative Go lint ratchet and boundary hardening

Date: 2026-07-27

Auth/visibility hardening commit:
`b0c00f370323221f4bce50457caf11db3f3b939c`

Collector hardening commit:
`fbb89976271f96026e84964b036e7094d932f2cd`

CI ratchet and remaining changed-code repairs:
`4e0042805c9c1a7481c4310c3f6780907231a12a`

This checkpoint turns the previously unusable repository-wide Go lint job into
a cumulative changed-code gate without pretending the inherited inventory is
already clean:

1. The Go lint job checks out complete Git history and runs pinned
   `golangci-lint v2.12.2` with
   `--new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102`.
   This fixed adoption revision makes every later main commit cumulative; a
   concurrency-cancelled intermediate workflow cannot let a new finding
   disappear on the following push.
2. The uncapped adoption inventory contained 1,468 findings, rather than the
   default report's capped 227. The reviewed first repair wave reduced the
   current uncapped inventory to 1,365 while the cumulative ratchet reports
   zero findings. The remaining inventory stays explicit cleanup debt; do not
   advance the baseline merely to hide an issue.
3. Visibility sequencing now validates every persisted signed sequence before
   publishing it as `uint64`, rejects out-of-range public sequence inputs before
   SQLite conversion, validates corrupted cutoff state, handles retention
   horizons above `math.MaxInt64` without wraparound, and bounds
   `RowsAffected` before committing a prune.
4. Collector-token revocation rejects invalid persisted versions. The token
   metadata query name no longer resembles a hard-coded credential to static
   analysis, and transaction/query error paths no longer hide outer errors.
5. Collector conversions are range-checked, file-identity handling is split by
   Darwin/Linux types, and Darwin retains the historical signed-to-unsigned
   checkpoint encoding. A Darwin-only regression pins the high-bit case so an
   upgrade cannot orphan a checkpoint and duplicate or skip input.
6. Command, server-listener, integration, and ClickHouse migration helpers use
   context-aware APIs and context-first signatures. Test fixture permissions
   are tightened, log-generator output defaults to owner-only, and only narrow,
   justified security annotations remain.
7. The full Go suite, focused race suites, collector cross-builds, exact lint
   ratchet, uncapped inventory, workflow YAML parse, and diff checks passed.
   Three independent adversarial reviews drove public nil-context coverage,
   nonempty retention-overflow coverage, Darwin persisted-key compatibility,
   and removal of six unnecessary command-taint suppressions. They reported no
   remaining blocker.

This historical checkpoint's 1,365-item inventory was later reduced to zero by
the repository-wide cleanup through `caadf3f`; the adoption baseline remained
fixed.

The workflow triggered by `4e00428`, GitHub Actions run `30255910487`,
completed successfully. It passed the cumulative lint ratchet, full
race-enabled Go suite, frontend, protobuf, backend vertical, pinned GradeThis
ClickHouse corpus, vulnerability scan, Linux and macOS production builds, and
the cross-platform embedded-asset comparison. The chronological aggregate
slice described in the latest checkpoint above now follows this work.

The exact validation record is under **Latest validation evidence**. The
overall backend goal remains active.

## Previous checkpoint: bounded ordered `stats list(field)`

Date: 2026-07-27

Implementation commit: `4e2ddb43ddb60ecd790c6ad3783fd7d83ecfda72`

CI repair commit: `05c1eaff6a373d220762c06838677e5db3fd6ee6`

This slice implements the first bounded order-sensitive SPL aggregate and
repairs the two actionable failures from the preceding remote CI run:

1. SPL parsing and planning accept case-insensitive `list(field)` with one
   exact unquoted field and the default output name `list(field)`. Unsupported
   quoted, wildcard, expression, missing-input, and extra-argument forms retain
   source-located diagnostics.
2. `list` preserves duplicates and current pipeline order. One event can
   contribute each immediate non-null top-level multivalue member in stored
   member order. Missing fields, explicit nulls, null members, and empty
   multivalues contribute nothing; an empty String remains a value.
3. Event order defaults to `_time DESC, event_id DESC`, then the immutable
   visibility sequence and private
   `(index, collector, batch sequence, batch ID)` source identity. The final
   source key keeps pre-visibility migrated rows deterministic when their
   sequence is zero. Explicit `sort`, `head`, `tail`, and `dedup` order is
   preserved.
4. Scalar conversion is shared with `dc` and `values`. Objects and nested
   containers fail the complete aggregate atomically, including poison after
   visible occurrence 100; rows excluded by tenant/index/time/visibility,
   filters, or an incomplete `BY` tuple cannot trigger that error.
5. Only the first 100 eligible strings per group are returned. Prefix windows
   select and byte-account that ordered prefix before grouping; the retained
   physical state is bounded to 100 tuples and 512 KiB per unique group/input.
   The compiler never expands event rows and never uses an unordered
   `groupArray`.
6. Repeated aliases over one input share one ordered aggregate state while
   counting independently toward public budgets. `list` and `values` share a
   10,000-element/512-KiB row budget and a 100,000-element/8-MiB complete-result
   budget, validated before a later filter, sort, projection, or `LIMIT` can
   hide overflow.
7. A statically projected-away input uses one bounded zero-state aggregate
   instead of sorting the full input through three ordered windows merely to
   publish `[]`. Global-empty and retained-group semantics and the fixed
   multivalue type remain intact.
8. The executor publishes `Array(String)` as typed multivalue results and
   preserves invalid UTF-8 members as Bytes. Stable list-specific markers map
   unsupported and resource failures to sanitized public execution errors.
9. The pinned ephemeral ClickHouse corpus covers explicit/default order,
   multivalue member order, duplicates, null/missing/empty values, tail and
   dedup input, the 100/101 boundary, exact/+1 byte boundaries, poison after
   100, incomplete `BY`, same-visibility source ties at one and four threads,
   downstream re-aggregation, shared physical state, projected-away input,
   combined `values`/`list` budgets, duplicate-alias accounting, and
   whole-result overflow before downstream `LIMIT`.
10. `05c1eaf` updates `golang.org/x/text` from `v0.36.0` to `v0.39.0`, removing
    the reachable `GO-2026-5970` path, and repairs a race/shuffle-sensitive
    search-history retry fixture that had accidentally rebuilt nanosecond
    search bounds from microsecond-normalized creation time.
11. Full Go, frontend, pinned ClickHouse, vulnerability, race/shuffle, vet,
    typecheck, frontend-lint, and diff checks passed locally. Two
    correctness/performance reviewers and the simplify
    reuse/quality/efficiency pass drove the legacy source tie, missing-input
    hot path, executable resource-barrier tests, shared constants/helpers, and
    frontend function-table consolidation. Their final current-tree reviews
    reported no remaining blocker.

The remote workflow for `327a162` subsequently passed frontend, protobuf,
backend vertical, the pinned GradeThis ClickHouse corpus, full race-enabled Go
tests, and vulnerability scanning. Its only failure was the accumulated Go
lint inventory. The cumulative lint checkpoint above installs the ratchet and
repairs the first reviewed wave; the newer chronological checkpoint now
completes `earliest(field)` / `latest(field)`.

The exact validation record is under **Latest validation evidence**. The
overall backend goal remains active.

## Previous checkpoint: deterministic committed release identity and artifacts

Date: 2026-07-27

Implementation commit: `5ecd99957bf4801da8b39e9bfabd274e11d5e208`

Cleanup/proof fix: `f68630a5b4fc213a379bda1f2c163b4c96b42fac`

This slice implements committed release-revision and embedded-asset
consistency and proves repeatability on the checkpoint host. The independent
Linux/macOS CI comparison was subsequently confirmed by run `30255910487`:

1. Go `1.26.5`, Node `24.18.0`, npm `11.16.0`, Buf, and both Go protobuf
   generators are pinned. Protobuf generation is complete-tree,
   transactional, serialized, and preserves only explicitly handwritten
   files; concurrent generators cannot publish mixed Go/TypeScript contracts.
2. `make release` accepts only a clean committed `HEAD`, bootstraps both the
   launcher and snapshot materializer from raw Git object bytes, builds in an
   isolated environment with fresh caches and a fixed umask, disables ambient
   VCS stamping, and refuses a stale slow build if `HEAD` changes before
   publication.
3. Snapshot extraction uses one streaming `git cat-file --batch` process. File
   count, individual and aggregate blob bytes, relative path bytes and depth,
   component bytes, distinct directories, aggregate directory-prefix bytes,
   Git output, and stderr are bounded. Invalid UTF-8, links, submodules,
   administrative directories, and cross-platform case/Unicode collisions
   fail closed.
4. Temporary launcher, build, and extraction roots are resolved physically
   before paths are derived. Active `TMPDIR` symlink-retarget regressions prove
   that neither an attacker launcher nor an attacker source tree can replace
   the committed inputs after materialization.
5. The canonical JSON asset manifest binds application version, full source
   revision, deterministic UI build ID, every UI file, protobuf schema, SQLite
   migrations, and ClickHouse migrations. The server verifies the embedded
   manifest and bytes; server, collector, log generator, protobuf bootstrap,
   HTML, and browser UI expose the same structured identity.
6. Publication is serialized and transactional. Private artifacts and all
   three binary identities are verified before the canonical `build/` rename;
   post-publication verification and rollback remain under the same lock.
   Concurrent publication, stale-source publication, occupied destinations,
   tampering, failed rollback, and prior-build restoration have deterministic
   regressions.
7. Cleanup handles Go's deliberately read-only module-cache directories and
   every repository-side transaction tree. Removal or residual-data failures
   force a nonzero release status while lock release is still attempted;
   incomplete rollback evidence is deliberately preserved.
8. CI now defines independent committed-release builds on Linux amd64 and
   macOS, uploads the canonical proof files, and compares the asset manifest,
   binary identities, and embedded verification output byte for byte. The
   Linux package uses a commit timestamp, canonical ownership/modes, sorted
   paths, and timestamp-free gzip output. Run `30255910487` passed both builds
   and the byte-for-byte comparison.
9. The final local frontend gate contains 47 release/proto/materialization
   transaction tests plus 106 application tests. Full Go and race suites,
   vet, Go build, typecheck, lint, protobuf regeneration, a production
   server/embed verification, and the Docker-backed collector-to-browser
   vertical all passed. The final vertical completed in 22.35 seconds, stored
   four distinct events with zero replay, and passed all six current GradeThis
   SPL cases.
10. The exact pinned-toolchain matrix at `f68630a` passed twice. The cold run
    took 37.85 seconds with 861,011,968-byte peak RSS; the prior-artifact
    replacement run took 36.32 seconds with 856,113,152-byte peak RSS. Both
    produced exactly six byte-identical files with identical modes and hashes:
    binaries are `0755`, proof files are `0644`, the UI contains 119 files,
    and its SHA-256 is
    `20520d9edbf374ae647ee293a68d966efa23e431920f06f9b787fc8bfe83caa4`.
    Fresh internal dependency caches intentionally make the second pass only
    4.0% faster; this is isolation cost, not an incremental-build benchmark.
11. Both exact runs kept `HEAD` and the worktree unchanged; each emitted only
    6,554 bytes of stdout and 601 bytes of stderr. Neither left a work root,
    launcher, publication, prior-build, failed-build, or lock residue. The
    earlier 682 MiB cleanup reproducer was removed after its permanent
    regressions passed.
12. Correctness/concurrency and performance/security reviewers drove the
    locking, stale-release, TOCTOU, streaming, resource-bound, portable-path,
    physical-root, and cleanup fixes. They reported final implementation
    staged SHA-256
    `cb29202d24c47dc46fecc0bd88702865b2a2a33da7707678eca0a74f84b8ce0e`
    and cleanup staged SHA-256
    `8a9cf57c8d5a96b256937b16a32e5433941ed5094b482464358f03ad69f76503`
    clean.

The exact validation record is under **Latest validation evidence**. Local
release-revision consistency and deterministic embedded frontend assets are
complete at this checkpoint. The subsequent remote run and ordered
`list(field)` slice are recorded in the latest checkpoint above. CI artifact
reuse and dependency-audit follow-ups remain separate operational work. The
overall backend goal remains active.

## Previous checkpoint: result-kind-bounded browser adaptation

Date: 2026-07-26

Implementation/proof commit: `c20204b667c5711bc9c4484ba43d046e3a9f65d4`

This slice finishes result-kind specialization and removes redundant
date-time formatter construction plus duplicate typed-value decoding and
allocation from browser result adaptation:

1. The authoritative result kind now bounds every projection:
   `RESULT_SET_KIND_EVENTS` builds only events,
   `RESULT_SET_KIND_TIME_SERIES` builds only timeline points, and
   `RESULT_SET_KIND_STATISTICS` builds only its categorical rows and generic
   table. Event field profiles and authoritative event histograms continue to
   come from their dedicated server routes; backend exports remain server-side.
2. Event and time-series date-time formatters are lazy and local to one
   adaptation. A nonempty valid projection constructs exactly one formatter,
   while statistics, empty input, and all-invalid input construct none. A new
   adaptation observes the then-current local timezone instead of retaining
   import-time or previous-call state.
3. The authoritative server-timeline adapter likewise constructs at most one
   formatter per nonempty response. Repeated-response tests prevent a
   module-level singleton, and empty or invalid responses remain allocation
   free for this formatter.
4. Event rows now decode each ordinary typed cell or flattened `fields` child
   once. The previous path decoded every ordinary cell twice and recursively
   rebuilt list/object values twice. Source-Date counters pin one serialization
   for ordinary timestamps, flattened object children, and timestamps nested
   in lists.
5. Time-series adaptation no longer decodes complete rows. It reads the time
   cell and candidate numeric cells only, retaining split-series order, exact
   unsafe-integer export text, bucket-width behavior, and an
   O(rows × columns) bound under the enforced 64-column browser limit.
6. Controlled red tests observed 1,048 formatter constructions for 1,000
   event rows and 1,000 constructions for 1,000 authoritative timeline
   buckets. Permanent tests assert construction counts rather than timing:
   one formatter for 1,000 events, one per time-series adaptation/response,
   zero on skipped paths, and refreshed timezone labels. The frontend suite
   now contains 104 passing tests.
7. The complete Go and Go race suites, vet, Go build, frontend tests,
   typecheck, lint, and production Next.js build passed. The exact
   Docker-backed release-path suite passed after the final single-decode fix:
   the vertical stored four distinct events with zero replay and passed all
   six current GradeThis searches; fixed-result rendering, expiration/gap
   recovery variants, and cancellation also passed. No Open Splunk test
   container remained afterward.
8. Independent consumer-correctness, allocation/performance, and
   lifetime/test-design reviewers drove full Events/Time Series
   specialization, the single-decode regression, repeated-call lifetime
   proofs, and invalid-input coverage. All three reported staged SHA-256
   `b1cd789777fe9336a33f4c6b5d856bb2befec8a3b30b8fe65200d21e73748253`
   clean.

The exact validation record is under **Latest validation evidence**.
Result adaptation specialization and formatter reuse are complete at this
checkpoint. Release-revision consistency and byte-identical embedded frontend
assets were subsequently completed at `5ecd999` with the cleanup proof at
`f68630a` on the checkpoint host. The subsequent remote run, CI repairs, and
ordered `list(field)` slice are recorded in the latest checkpoint above. The
overall backend goal remains active.

## Previous checkpoint: statistics-only result projections

Date: 2026-07-26

Implementation/proof commit: `e647dd2e5ae3b422ec98ee16b758d15fc87a4aa5`

This slice made the browser adapter honor the backend result kind as an
authoritative projection boundary instead of materializing event-only state
for transforming statistics:

1. `RESULT_SET_KIND_STATISTICS` produces only the categorical chart rows and
   generic statistics table that its Statistics and Visualization views
   consume. Its `events`, `fields`, and `timeline` projections are empty.
2. Statistics adaptation no longer decodes every row into a fabricated event,
   formats event timestamps, derives and sorts field profiles, or constructs
   an event histogram. For schemas that produce a generic table, the remaining
   asymptotic work is Θ(columns + rows × columns + decoded nested-value nodes),
   matching the table's structural output size even when it contains zero
   rows.
3. The historical `_raw` rule is preserved: a transforming schema that still
   contains an event raw column does not invent a generic statistics table.
   A guarded typed-object regression proves that this raw payload is not even
   decoded on the statistics path while the usable metric projection remains
   intact.
4. The redundant caller-provided `timechart` boolean was removed. An
   exhaustive result-kind switch selects Events, Statistics, or Time Series
   directly from the authoritative schema, and unspecified, unrecognized, or
   future unknown numeric kinds fail closed instead of falling through as
   event data.
5. Red tests first reproduced seven unnecessary `Intl.DateTimeFormat`
   constructions for two statistics rows. Permanent coverage pins empty
   event-only projections, exact statistics rows/table columns, skipped raw
   decoding, unsupported-kind rejection, and retained event/time-series
   behavior. The frontend suite contained 97 passing tests.
6. The complete Go and Go race suites, vet, Go build, frontend tests,
   typecheck, lint, production Next.js build, and Docker-backed release-path
   suite passed.
7. Independent correctness, efficiency, and maintainability reviewers
   reported staged SHA-256
   `ce22fc3ec685b30864e215891bb9ac3ebe0e8d49ef2f179fd0ce483e4f16b609`
   clean.

Statistics-only result specialization is complete at `e647dd2`; the formatter
and remaining Events/Time Series specialization follow-up is complete at
`c20204b`. The overall backend goal remains active.

## Previous checkpoint: per-surface ordered configured-redaction replay

Date: 2026-07-26

Implementation/proof commit: `1b8939775efcf053d0d11ec870cf075dc5a22178`

This slice removes the construction-time performance cliff for configured
redaction policies that require historical ordered semantics, without
weakening fail-closed behavior or changing observable sequential output:

1. Composite configured redactors always retain the exact ordered validators,
   but hazardous configurations no longer run that chain across every surface
   of every event. One bounded composite pass first detects a policy-dependent
   change; only the affected typed field, raw payload, or message replays the
   historical chain. Safe sibling surfaces remain on the one-pass path.
2. Valid UTF-8 text, JSON, invalid binary raw bytes, embedded JSON strings,
   ambiguous boundaries, and binary-to-UTF-8 transitions retain their prior
   behavior. Detection suppresses disposable output construction, and replay
   starts from the original surface so generated quotes, marker cascades,
   specialized authorization/cookie/PEM extents, and depth-limit fail-close
   selection remain exact.
3. Duplicate-key-only JSON changes carry canonicalization metadata separately
   from a policy match. Safe duplicates are decoded and canonicalized once
   even in an ordered-on-change configuration; duplicate JSON combined with a
   sensitive match still replays. Duplicate JSON inside malformed prose keeps
   the historical whole-boundary fail-close behavior.
4. A directly named typed field starts at its last matching policy because
   that whole-value assignment discards every earlier result, then runs the
   remaining suffix so a later policy can still reinterpret the generated
   marker. The sensitive typed-field primitive also clears protobuf unknown
   bytes from both the field and value boundary, including an input already
   equal to its marker, so forward-compatible wire data cannot bypass
   redaction.
5. Permanent differential regressions cover every event surface, safe and
   sensitive duplicate JSON, nested and malformed encoded JSON, middle and
   final direct-policy matches, marker cascades, binary input, depth bounds,
   concurrent reuse, and serialized unknown-byte secrets. Independent golden
   assertions pin the canonical and fail-closed outputs rather than relying
   only on the sequential oracle.
6. The hazardous-policy fuzz target freezes syntax markers, repeated fields,
   marker-to-later-field cascades, specialized private-key extents, structured
   hits, declared UTF-8, messages, binary input, and combined surfaces. The
   final 30-second hazardous campaigns completed 744,099 supplemental and
   274,443 alias executions; the ordinary supplemental campaign added 209,756
   executions. No reproducer survived, and every earlier counterexample is a
   named regression or explicit seed.
7. A permanent allocation regression compares opaque and syntax-bearing
   32-policy composites for event, text, key/value, and duplicate-JSON safe
   misses. On the checkpoint M4 Max, one 4-KiB safe payload used for both raw
   and message (8 KiB combined) fell from roughly 2.23–2.30 ms, 326,656 bytes,
   and 608 allocations on the sequential chain to 60.7–62.7 µs, 10,208 bytes,
   and 19 allocations on the composite path.
8. Duplicate-only JSON remains policy-count independent: at 32 policies its
   raw-plus-message benchmark took roughly 3.04–3.06 µs, 5,749 bytes, and 101
   allocations versus 63.7–64.2 µs, 162,628–162,631 bytes, and 2,116 allocations
   sequentially. A final-policy direct hit with a 1-MiB value avoids 31
   discarded full-value scans; the focused benchmark dropped from roughly
   273–276 ms and 62 MiB allocated to about 374–380 ns and 1.4 KiB.
9. Hit-only raw, message, and valid-JSON benchmarks at 2, 8, and 32 policies
   make the detection-pass tradeoff explicit. For tiny two-policy fixtures,
   detection added roughly 0.15–0.82 µs depending on the surface. At 32
   policies raw and JSON remained modestly slower in the short samples while
   the message fixture was faster because replay avoids unrelated event
   surfaces. These are observational measurements, not release thresholds.
10. The complete ordinary Go suite, a full-repository race pass followed by
    final affected-package race gates, vet, build, three differential fuzz
    campaigns, focused repeated allocation/confidentiality regressions, and
    the exact Docker-backed vertical passed. The vertical stored four distinct
    events with zero replay, passed all six current GradeThis searches, and
    left no test container running.
11. Correctness/confidentiality, efficiency/concurrency, and
    maintainability/test reviewers repeatedly drove fixes for nested detection
    metadata, duplicate-only replay, direct-field prefix scans, fuzz seed
    coverage, independent goldens, hit-only measurement, and protobuf unknown
    bytes. All three independently verified the final staged SHA-256
    `023c792c59b45711bf5cd01c0019ad49f7d203ca874fcbf6587206a8c94abf9a`
    and reported it clean.

The exact validation record is under **Latest validation evidence**. The
syntax-bearing configured-redaction performance follow-up is complete at this
checkpoint. Its next priority was statistics-only result specialization, now
complete at `e647dd2`; formatter reuse and the remaining result-kind
specialization are now complete at `c20204b`. Release-revision implementation
and local repeatability are complete at `5ecd999` and `f68630a`; only the
independent Linux/macOS CI confirmation remains under **Remaining work**. The
overall backend goal remains active.

## Previous checkpoint: bounded integration/browser harness resources

Date: 2026-07-26

Implementation/proof commit: `3f8922972ab5258a0f0658c714b5ba36971dcf71`

This slice puts explicit memory and listener bounds around the integration
harness itself so a failing build, hostile browser diagnostic, or unexpected
socket fan-out cannot turn the correctness proof into a resource-amplification
path:

1. Go builds, isolated frontend builds, and Playwright specifications now share
   a concurrency-safe 1-MiB process-output buffer. Failure output retains an
   in-budget truncation marker; successful commands that exceed the budget
   fail explicitly rather than silently discarding evidence.
2. Self-exec tests pin normal success and failure, the exact byte boundary,
   interleaved stdout/stderr overflow, missing executables, and environment
   replacement without duplicate inherited `CGO_ENABLED` settings.
3. Browser text diagnostics are capped at 4 KiB of UTF-8 per entry and 32
   entries per history, followed by one fixed overflow marker. Boundary
   scanning does not encode the entire attacker-sized input, handles
   multibyte and lone-surrogate input, and returns isolated snapshots.
4. The same diagnostic runtime is installed in the page realm before
   application code. Fresh bounded errors discard the original error's
   message, stack, and cause so truncation does not retain the oversized
   source through object references.
5. Page errors, API failures, external URLs, preview statuses, job metrics,
   and stale job-strip snapshots now use bounded histories. Compact boolean
   latches continue to observe finalization errors, stale one-row metrics, and
   stale loading/results DOM states even after the corresponding history is
   full.
6. A real-page worker self-test deliberately fills and overflows the preview,
   metrics, and stale-DOM histories, then injects forbidden late values and
   proves that all latches still fire. Its temporary fixtures are removed in a
   `finally` block and the preview recorder is reset before each product test.
7. Stale-DOM observation no longer materializes or normalizes the full
   `root.textContent`; it scans only the preview rows and targeted phase/count
   elements, with bounded intermediate concatenation.
8. Matching search-protocol WebSockets have explicit ownership. Browser
   cancellation observes exactly one socket, treats duplicate attachment as
   idempotent, removes closed-socket listeners immediately, and keeps the page
   observer alive to reject a forbidden reconnect. Sequence-gap routes cap
   accepted connections before connecting upstream, and the page-frame
   recorder caps matching sockets at two and detaches listeners on close.
9. Exact frame evidence is separately bounded to 64 frames of at most 16 KiB.
   ArrayBuffer byte lengths and Blob sizes are checked before copying; pending
   Blob conversions reserve their frame slots, and expected byte payloads are
   checked before base64 allocation.
10. The exact Docker-backed vertical and all affected compiled-browser
    sequence-gap and cancellation cases passed against Google Chrome, in
    addition to the complete Go/frontend, race, vet, build, lint, and type
    gates.
11. Independent Go/process-reuse, WebSocket/concurrency/code-quality, and
    diagnostic/performance reviewers found no remaining issue in frozen staged
    patch SHA-256
    `f3f6ac27e6b4fdd109a7d993047dcc2dbbb7da65c4925ad1ee3d91c915de3e09`.

The exact validation record is under **Latest validation evidence**. Harness
resource hardening is complete at this checkpoint. Its next priority was the
red benchmark/test and per-event optimization for the syntax-bearing
configured-redaction marker cliff, now complete at `1b89397`; the statistics
adapter is complete at `e647dd2`, formatter reuse and the remaining
result-kind specialization are complete at `c20204b`, and release-revision
implementation and local repeatability are complete at `5ecd999` and
`f68630a`; only the independent Linux/macOS CI confirmation remains. The
overall backend goal remains active.

## Previous checkpoint: composite configured pre-WAL redaction

Date: 2026-07-26

Implementation/proof commit: `34f3a9b291ff7ea327869cf4e635f5c496f13563`

This slice compiles distinct configured replacement groups into one immutable
resolver while retaining the exact output and fail-closed behavior of the
historical ordered `NewSupplementalRedactor` chain:

1. `buildDurableRedactor` now constructs zero or one composite configured
   resolver after last-rule-wins grouping. Mandatory redaction remains the
   first trust-boundary pass, configured overrides apply when their field
   boundary survives, and alias lineage remains the final pre-WAL pass.
2. Exact fields carry their replacement marker and specialized raw-value kind
   in one immutable lookup. Structured fields and valid JSON resolve distinct
   markers in one traversal; unchanged free-form text is independent of the
   configured marker count.
3. Compatibility is deliberately conservative. Repeated fields, pre-final
   syntax-bearing markers, marker-to-later-field cascades, embedded encoded
   payloads, ambiguous boundaries, hidden earlier assignments, and any
   non-final free-text hit replay the ordered validators. This preserves legacy
   quote generation, policy precedence, specialized authorization/cookie/PEM
   extents, and fail-closed marker selection.
4. Binary events re-evaluate UTF-8 validity between replayed policies, matching
   the historical transition from byte scanning to full text scanning when an
   earlier replacement removes the last invalid byte.
5. Embedded JSON depth behavior is pinned for both ordinary strings and direct
   sensitive-key matches: at the bound, later configured policies retain their
   historical final marker while an enclosing fail-closed boundary retains the
   earliest changing marker.
6. Active rename aliases share the same composite text resolver, but valid
   root JSON without a message avoids constructing it. Literal empty or invalid
   alias replacements retain the old unchecked ordered path, and
   `StructuredOnly` aliases still never affect raw/message data.
7. A frozen sequential corpus plus permanent differential fuzz targets cover
   duplicate JSON keys, exact/case-sensitive names, ambiguous encoded
   keys/values, prose-wrapped JSON, invalid UTF-8, depth bounds, generated
   boundaries, syntax/marker cascades, authorization, cookie, private-key PEM
   extents, aliases, and concurrent resolver reuse.
8. Collector unit coverage reopens the offline WAL and proves two neutral
   configured secrets with different markers are absent from deterministic
   wire bytes while safe data survives. The Docker vertical sends the same two
   markers through the real collector, offline WAL/restart, server,
   ClickHouse, SPL search, WebSocket, HTTP protobuf, and JSONL export.
9. The final 20-second fuzz runs completed 133,454 supplemental executions and
   408,956 alias executions. Full ordinary tests, affected race tests, vet,
   build, and the Docker-backed vertical all passed after the final runtime
   change.
10. Observational M4 Max benchmarks show the intended scaling where exact
    compatibility permits it. At 32 policies and 64 KiB, safe text improved
    from roughly 17.6 ms to 0.49 ms and valid JSON hits from roughly 22.8 ms to
    0.70 ms. Hit-heavy 4-KiB free text that requires ordered replay is now at
    parity with the legacy chain while allocating fewer bytes; both
    policy-order and reverse-text-order fixtures remain pinned so this
    tradeoff cannot be hidden.
11. Multiple correctness, confidentiality, performance, concurrency, reuse,
    and code-quality review rounds found and drove fixes for policy/text-order
    inversions, generated-quote and PEM leaks, binary-to-UTF-8 transitions,
    direct-key depth semantics, alias-oracle gaps, redundant normalization, and
    discarded fallback buffers. The final implementation staged diff was
    `90448fbafc9bc552e09ce509e1af3d93c9d01fa3b4f8e4fab3d5cf49c3a20520`.

The exact validation record is under **Latest validation evidence**.
Composite configured pre-WAL redaction is complete at this checkpoint. The
next priority was the harness-output bound, now complete at `3f89229`.
The overall backend goal remains active.

## Previous checkpoint: high-source-count collector polling

Date: 2026-07-26

Implementation/proof commit: `f41720e0f868354fafd535022b445b12ddaff99b`

This slice removes the material per-source steady-poll allocation costs and
pins collector behavior at the races most likely to lose or delay data:

1. Each tailer now owns and safely reuses one `time.Timer` for all three poll
   waits. Polling remains fixed-delay-after-work, cancellation is prompt, stale
   ticks are consumed before reset, and the steady wait path allocates zero
   bytes.
2. A clean-EOF fast path avoids rebuilding a framer and its 4-KiB source
   scratch buffer when the file size still equals the tailer's current read
   offset. Drain requests deliberately bypass that shortcut so an append racing
   with the preceding `Stat` is reframed and cannot be skipped.
3. Guard fingerprints now reuse per-tailer scratch and a raw SHA-256 digest.
   Persisted file-identity fingerprints remain hexadecimal and unchanged,
   while the in-memory guard's steady fingerprint path allocates zero bytes.
4. Multi-glob discovery now sorts the deduplicated result with the
   standard-library `slices.Sort` rather than quadratic insertion sort. A
   direct 10,000-path comparison improved from roughly 50 ms to 0.52 ms on the
   checkpoint machine.
5. A same-size copy-truncate rewrite now resets the multiline inactivity
   clock. Without that reset, a newly rewritten partial multiline event could
   inherit the old file's inactivity age and flush too early.
6. Manager construction rejects multiline input without a line-start pattern.
   This validates the configuration before a clean-EOF shortcut could defer
   the error until data later arrived.
7. Controlled regressions pin timer rearming and cancellation, zero-allocation
   guard reuse, clean EOF, append-after-`Stat` drain, same-size copy-truncate,
   repeated append waves and source coordinates, sorted/deduplicated globs,
   and multiline configuration validation. The pre-existing `start_at=end`
   test now waits for its exact discovery checkpoint instead of a weaker
   discovered-source count, removing a test synchronization race.
8. In an actual 1,000-empty-file, 10-ms-poll, approximately 1.5-second profile,
   allocations fell from 2,054,808 mallocs and 760,255,248 bytes to 1,357,072
   mallocs and 149,706,432 bytes: 34.0% fewer allocations and 80.3% fewer
   allocated bytes. A corresponding nonempty-file run converged to essentially
   the same result. Remaining polling cost is dominated by filesystem
   discovery and `Stat` work across the manager and tailers; the tailer timer,
   framer, and guard hot paths no longer allocate in steady state.
9. The complete ordinary and race-enabled Go suites, vet, build, the exact
   Docker-backed vertical, and the 30,000-event sustained-load/outage/restart
   proof all passed. The load proof stored exactly 30,000 distinct event,
   request, and timestamp identities and drained a 5,700-event durable backlog
   after the outage.
10. Three independent final reviewers recomputed staged patch SHA-256
    `944048bc19bb28af667fd59dcce49f6c73cebcd2b6ee9eb2eea31d27d9f3d7a3`.
    After adversarial review of timer lifecycle, drain races, copy-truncate,
    multiline and oversized records, guard ownership, allocation evidence,
    and test synchronization, all three reported the frozen patch clean.

The exact validation record is under **Latest validation evidence**.
High-source-count collector polling is complete at this checkpoint. The next
priority is differential coverage for a composite configured pre-WAL
redaction resolver, as ordered under **Remaining work**. The overall backend
goal remains active.

## Previous checkpoint: bounded browser statistics rendering

Date: 2026-07-26

Implementation/proof commit: `9d6acc11f2626f92d5ddd2b4e608a1268cc0c9e3`

This slice makes large statistics results bounded, usable, and measurable in
the real compiled browser application:

1. The production statistics table now virtualizes when either row count
   exceeds 100 or materialized data cells would exceed 2,048. It renders a
   six-row overscan around the viewport, uses native table spacer rows, and
   exposes accurate ARIA row counts and indexes.
2. Generic and timechart result tables use fixed layout, bounded column
   geometry, clipped cells, a sticky header, and a named keyboard-scrollable
   region. The table remains horizontally scrollable rather than expanding the
   page, including at the supported 64-column browser boundary.
3. Virtual scroll state resets on result generation, page, sort, and density
   changes; it deliberately survives live-preview growth. Shrink, threshold,
   column-count, and viewport transitions clamp both the logical window and
   the physical scroll offset, including transitions back to an unvirtualized
   table.
4. Browser result boundaries now reject schemas outside 1–64 columns and REST
   pages larger than the requested page size. The same width check applies to
   zero-row live previews and again inside the frontend adapter as a defensive
   boundary.
5. A deterministic real-browser fixture loads the production protobuf HTTP
   handler, search manager, compiler, and compiled backend-mode UI. Its
   executor returns exactly 1,000 rows by 64 columns with a deterministic
   schema, checks its width and key names plus every ordinal, first/last
   sentinels, unique row IDs, and terminal metadata, and records payload hash
   and size. It does not substitute a mock page or bypass the application
   adapter.
6. The browser proof checks the top and bottom sentinels, keyboard `End`,
   sticky-header behavior, ascending and descending sort, standard and compact
   density, and a 1024×768 viewport. The current initial DOM contains 18
   materialized rows, one spacer, 19 total body rows, and 1,216 materialized
   cells for a 10,240-pixel-wide table.
7. Independent mutation tracking spans initial render and the complete
   scroll/sort/density/resize interaction flow. It requires peaks of at most 32
   materialized rows and 34 total body rows; checkpoint assertions additionally
   cap spacers at two and materialized cells at 2,112. The final green run
   observed combined peaks of 25 materialized and 27 total body rows. Self-tests
   inject both 1,000 ordinary rows and 1,000 spacer rows on a disposable page,
   proving synchronous transient explosions cannot evade the observer or
   contaminate measured target-page metrics.
8. Navigation/resource timing uses the actual result-response end, stable DOM
   plus two animation frames, a cleared and enlarged resource-timing buffer,
   drained performance observers, and interval filtering. Render timings and
   long-task/layout-shift values are explicitly observational rather than
   release thresholds.
9. Browser process orchestration now shares one runner, bounds retained child
   output, cleans process groups, validates finite required metrics, and writes
   screenshots, metrics, and bounded logs for CI artifacts. The required
   backend-vertical CI job includes the fixed-rendering proof and has explicit
   job/package timeouts.
10. Three independent final reviewers recomputed staged patch SHA-256
    `3af2a8373ff9900d862415851c84ccbd35b69462bda8325c939054c99792a732`.
    After adversarial iterations covering row/cell bounds, transient DOM
    mutation escape hatches, schema/page limits, scroll transitions, observer
    lifetime, resource timing, isolation, and process cleanup, all three
    reported the frozen patch clean.

The exact validation record is under **Latest validation evidence**. Browser
rendering for the fixed first-release payload is complete at this checkpoint.
The next priority is high-source-count collector profiling and the
pre-existing per-poll timer allocation described under **Remaining work**.
The overall backend goal remains active.

## Previous checkpoint: bounded WebSocket consumers and replay recovery

Date: 2026-07-26

Implementation/proof commit: `4c4003f`

This slice makes slow WebSocket consumers deterministic, bounded, isolated,
and recoverable on both sides of the transport:

1. A real-server integration fixture wraps exactly one accepted TCP
   connection with a test-only write gate. With the production minimum
   seven-frame queue, the first progress frame is held in flight, the next six
   remain queued, and the eighth new progress frame closes only that slow
   connection. The test does not depend on kernel-buffer timing or impose a
   performance threshold.
2. A healthy sibling receives every progress sequence contiguously while the
   slow writer is blocked. Authoritative REST state advances throughout and
   the search executor is admitted exactly once, proving that transport
   backpressure cannot stall or repeat search execution.
3. The abandoned checkpoint deliberately falls outside the retained replay
   suffix. Reconnection receives exact `SEQUENCE_EXPIRED` bounds and recovery
   path, performs authoritative REST recovery, reconnects at the server's
   latest sequence, receives subsequent live progress, and converges with the
   healthy sibling on identical terminal state. The executor still runs once
   and the deterministic clock finishes with no poll waiters.
4. Server preview pressure no longer reports an undelivered disposable
   preview as success. The pressured connection is closed for replay, the
   canonical event remains retained, and healthy siblings continue. Both live
   publication and initial subscribe/replay tailoring use a dedicated,
   cancellation-aware bounded-work gate rather than charging temporary
   canonical copies to the connection queue budget; only final stamped frames
   consume queue capacity.
5. The browser client now owns a client-wide inbound bound, including the
   active listener and every pending generation: 64 frames and 4 MiB by
   default, with validated positive safe-integer overrides. Direct
   `ArrayBuffer` values and all views are copied into exact-size owned storage,
   preventing caller mutation and oversized backing-buffer retention; immutable
   `Blob` values are charged by size.
6. Inbound overflow closes the originating socket with application code 4000
   and requests replay without advancing the checkpoint. Listener work is
   serialized globally across socket generations, so a stale callback
   completes before replacement-generation callbacks. Automatic reconnect is
   deferred while that callback is active, while intentional disconnect
   cancels the deferred reconnect.
7. Every callback-triggered restart is fenced to its originating socket and,
   after awaited callbacks, to the same subscription object. Delayed close,
   sequence-gap, send-error, disconnect/reconnect, and unsubscribe/resubscribe
   races therefore cannot close a replacement socket or commit a stale
   checkpoint.
8. The browser queue uses a logical head and periodically compacts only after
   at least 64 consumed entries and proportional progress. This removes
   repeated `Array.shift()` work and bounds physical slot retention with
   amortized linear processing under a sustained nonempty backlog.
9. Controlled reds reproduced the exact server defect: with unrelated queued
   data leaving room for the final acknowledgment plus one-row preview, the
   old initial replay path closed the healthy subscriber because it charged
   the larger two-row canonical scratch copy. Browser regressions likewise
   pinned active-plus-pending frame/byte bounds, mutable buffer ownership,
   stale-listener ordering, replacement-socket fencing, delayed close, and
   queue compaction.
10. Three independent final reviewers recomputed staged patch SHA-256
    `887f03b62962c1aacb45b992f0481b1a7c5f04b6cc408f1d81fa5dc648c9670c`
    and reported no remaining correctness, replay/sequence, concurrency,
    accounting, leak, deadlock, efficiency, performance, determinism, reuse,
    or code-quality finding.

The exact validation record is under **Latest validation evidence**. No Docker
fixture was launched for this slice. The next priority is a separate browser
rendering measurement for a fixed 1,000-result payload with stable-DOM and
animation-frame gates. The overall backend goal remains active.

## Previous checkpoint: concurrent SPL searches during live recovery

Date: 2026-07-26

Implementation/proof commit: `9898b41`

This slice extends the real-process sustained-load proof with deterministic
public SPL searches while the collector is actively recovering:

1. Each concurrent cohort contains eight ready-gated goroutines. All workers
   reach the gate, signal readiness, and wait on one closed channel before
   constructing and admitting their protobuf HTTP searches. Admission errors
   are returned through a fully buffered channel and drained by the parent;
   worker goroutines never call `testing.T` methods.
2. The harness runs rolling cohorts rather than one isolated burst. Cheap
   source and physical-row samples gate up to three waves, and the proof
   requires at least two. Source records and publicly visible results must
   both advance strictly while the generator and collector remain alive.
   Every result in a later cohort must expose at least the maximum visible
   prefix from the preceding cohort, preventing a single high result from
   hiding regressions in its peers.
3. Each result proves an exact contiguous source prefix through public SPL:
   `count`, distinct event/request/user/timestamp counts, and minimum/maximum
   timestamps. The event, request, and timestamp cardinalities must agree;
   the first timestamp is the fixture start and the last is exactly
   `start + (events - 1)ms`. Schema, nullability, public job metadata,
   completion state, truncation state, scan counters, and unique job IDs are
   pinned as well.
4. Raw ClickHouse row counts are deliberately only a later observational
   upper bound. The collector calls `prepared.Send()` before its SQLite
   visibility transaction commits, so physical storage cannot safely be used
   as a lower-bound proxy for the public searchable prefix.
5. A liveness timer is derived from the remaining paced source window with
   one-second/flush headroom. It bounds only the cheap progress-poll phase,
   is created after a fresh sample, and is canceled before a search cohort.
   Search requests retain their separate 60-second deadline, and cohort count
   is independently capped at three.
6. Timing remains observational. The harness records server-internal
   `[CreatedAt, FinishedAt)` lifecycle duration, client monotonic whole-window
   wall time, scan rows/bytes, queue wait, and half-open
   `[StartedAt, FinishedAt)` maximum active overlap. It imposes no latency or
   overlap threshold and does not compare concurrent results with a final
   search over a different data set.
7. The protobuf HTTP harness now has a shared error-returning request helper;
   the existing fatal test wrapper delegates to it. Pure tests pin rejection
   of gapped timestamp prefixes, every-member cohort regression, half-open
   overlap arithmetic, and the sustained-load plan's derived liveness window.
8. Controlled reds first captured the missing validator, then showed that a
   plausible count/unique-ID result with a gapped timestamp range was wrongly
   accepted. Review iterations removed unsafe physical-row lower bounds,
   mixed-clock comparisons, weak cohort advancement, hidden latency
   thresholds, retry amplification, misleading overlap claims, and a timer
   that could outlive polling.
9. The exact final Docker run completed in 46.14 seconds. Its two cohorts
   admitted 16 public jobs while source records advanced from 11,200 to
   11,400, physical rows from 6,500 to 7,700, and exact searchable prefixes
   from 6,500 to 7,100. Client wall time was 208.172 ms, lifecycle span
   202.901 ms, and observed maximum active overlap four. The searches scanned
   108,800 rows and 61,881,298 bytes; lifecycle
   min/median/p95/max was 20.653/48.652/103.556/103.556 ms, maximum queue wait
   was 84.823 ms, and all final 30,000-event outage/restart assertions
   remained green.
10. Three independent final reviewers recomputed staged patch SHA-256
    `71d19acf2990b12db60e53cc50ecb0dc6c2abec063ceb5cdf69c7fb8845bc4fb`
    and reported no remaining correctness, determinism, concurrency,
    efficiency, performance, metrics, or code-quality finding.

The exact validation record is under **Latest validation evidence**. The next
load slice is a deterministic bounded-queue slow WebSocket consumer, followed
by separate browser rendering measurements for a fixed 1,000-result payload.
The overall backend goal remains active.

## Previous checkpoint: sustained-load outage and restart correctness

Date: 2026-07-26

Implementation/proof commit: `59b8f7c`

This slice adds the first measured real-process backend load proof and fixes
the collector restart defect that proof exposed:

1. Opt-in `TestBackendSustainedLoad` builds the real server, collector, and
   log-generator binaries, starts pinned ephemeral ClickHouse, and writes
   30,000 high-cardinality NDJSON events at a target 1,000 events/second. The
   schedule is fixed at 5,000 warm events, 6,000 server-offline events
   (5,000 required plus 1,000 source headroom), and 19,000 recovery events.
   It uses 10,000 possible users, one unique request ID per event, 1 ms
   timestamps, and 100-event output flushes.
2. The source proof independently scans the complete file, pins the three-phase
   timestamp and ordinal schedule, requires exact request-ID cardinality and a
   bounded high user cardinality, and builds a SHA-256 raw-record multiset.
   Stored rows must reproduce every raw record and its extracted request ID,
   user ID, ordinal, and event timestamp exactly.
3. After the warm prefix is stored, checkpointed, and acknowledged, the test
   kills the server, generates the bounded offline phase, and proves storage
   and the durable checkpoint cannot advance. The collector runs at debug
   level only for this proof; each counted append diagnostic is emitted after
   the `SyncAlways` WAL append returns. The test waits for diagnostics covering
   at least 5,000 durably appended events before crash-stopping the collector,
   then cold-reopens the WAL and independently requires a nonempty,
   unquarantined durable backlog covering the same offline window.
4. The first stronger controlled red found only 4,800 durable events when the
   source stopped exactly at the 5,000-event target. Adding 1,000 events of
   source headroom and synchronizing on post-append evidence made the gate
   deterministic without assuming the collector tracks source generation
   instantaneously. The final run crash-retained 5,700 events in 19 WAL
   batches from the exact 6,000-event offline source phase.
5. Restarting from that intact WAL originally produced 35,700 physical rows
   but only 29,700 distinct event IDs. The collector replayed the original
   durable batches while rereading their source bytes from the older terminal
   checkpoint; stable event IDs survived, but the reread minted new batch IDs
   and bypassed server batch-idempotency.
6. Collector startup now reconstructs the highest source coordinate owned by
   intact unacknowledged WAL batches and overlays it only on the file manager's
   resume view. The acknowledged checkpoint remains unchanged until terminal
   delivery. Manager-derived legacy cursor enrichment at or behind a pending
   coordinate is suppressed; terminal or newer-generation writes pass through,
   and acknowledged/superseded overlay entries are pruned. Resume lookup holds
   the overlay read lock across the durable lookup so a concurrent terminal
   commit cannot remove the pending coordinate after an old durable cursor was
   read. Once the overlay drains, it releases the map and switches manager and
   terminal paths to an atomic inactive fast path.
7. WAL recovery exposes pending source marks through a compile-time
   `ResumeQueue` capability. Aggregation is shared with acknowledgment
   planning, fails closed on metadata/cursor conflicts, stops at the intact
   prefix before a corrupt gap, and avoids a transient slice header per queued
   batch. The file manager receives a narrow manager-specific checkpoint
   interface rather than a proxy pretending to implement durable lifecycle
   operations.
8. Deterministic tests reproduce the restart defect as seven queued events
   where only four should exist, then pin the fixed count, unchanged terminal
   checkpoint, legacy cursor ephemerality, durable-equal preference,
   copy-truncate generation fencing, corrupt-gap fallback, and conflicting
   cursor/identity rejection. A blocking-store concurrency test also pins the
   exact old-durable-read versus terminal-prune race found by adversarial
   review.
9. The final Docker run stored exactly 30,000 rows with 30,000 distinct event
   IDs and exact raw/extracted values. It generated 12,531,099 source bytes in
   30.255 seconds, recovered 264 ms after backend health while the separately
   bounded recovery phase was actively generating, drained after generation
   in 346 ms, and left one EOF checkpoint, an empty acknowledged WAL, no
   quarantines, and an empty owner-only dead-letter file. ClickHouse reported
   three active parts, 7,299,197 compressed bytes, 34,495,468 uncompressed
   bytes, and 11,496,867 bytes on disk. Public SPL `stats count`/`dc` searches
   completed in 42.3 ms and 41.5 ms on this checkpoint machine.
10. Load polling now uses cheap row counts and runs exact distinct counting
    only at convergence, so the harness does not repeatedly allocate exact
    distinct state while measuring ingestion. Timings are observational,
    cache-affected checkpoint evidence, not universal acceptance thresholds.

The exact validation record is under **Latest validation evidence**. The
delivery contract remains at-least-once: identical durable batch retries are
server-deduplicated, while unavoidable crash-boundary source rereads retain
stable event IDs for explicit logical `dedup event_id`. Concurrent search is
now complete in the latest checkpoint above. The next load slice is a
deterministic slow WebSocket consumer, followed by separate browser rendering
measurements. The overall backend goal remains active.

## Previous checkpoint: load-generator pacing and live-output foundation

Date: 2026-07-26

Implementation/proof commit:
`860acac` (`harden log generator for sustained load`)

This slice makes `open-splunk-loggen` a trustworthy source for the next
real-process load proof:

1. Rate limiting now uses a reusable timer and absolute ordinal deadlines, so
   event generation, encoding, and ordinary writes do not accumulate as extra
   inter-event sleep. The schedule begins on the first event rather than
   before output setup.
2. A long scheduler or I/O stall cannot create an unbounded replay burst.
   Catch-up debt is capped at the smaller of 100 events or 100 milliseconds;
   the CLI describes `-rate` as a target rate with bounded catch-up rather than
   claiming a strict instantaneous maximum.
3. Invalid floating-point rates, sub-nanosecond and unrepresentably slow
   intervals, and ordinal/deadline overflow fail before output is opened.
   Context cancellation works both before a wait and while the reused timer is
   armed.
4. `-flush-events` gives a live tailer bounded user-space visibility without
   imposing a flush syscall on every unpaced fixture. `-append` uses
   `O_APPEND`, preserves complete existing records, and rejects a nonempty
   file whose trailing record lacks a newline.
5. Every post-open exit now flushes buffered bytes, syncs file output, and
   closes it while preserving all error causes. The command suppresses only
   error trees composed entirely of cancellation, so a shutdown-time
   flush/sync/close failure cannot be hidden as an ordinary interrupt.
6. A pre-canceled run returns before creating or truncating its output. Tests
   also force cancellation with a delimiter still buffered and an injected
   final-flush failure, proving the durability failure remains visible.
7. The controlled red suite reproduced relative-pacing drift, unbounded
   catch-up, cancellation-masked finalization failure, pre-cancellation
   truncation, and incomplete-record append corruption. Three independent
   adversarial reviewers then traced the timing, timer, overflow, I/O,
   cancellation, and file-lifecycle paths and reported the final patch clean.

The exact validation record is under **Latest validation evidence**. At this
checkpoint the next priority was the opt-in, pinned-ClickHouse sustained-load
integration harness; that core proof is now complete in the latest checkpoint
above. This commit remains the load-source foundation, not itself an
ingestion-capacity claim. The overall backend goal remains active.

## Previous checkpoint: shutdown-safe export artifact removal

Date: 2026-07-26

Implementation/proof commit:
`961cba2` (`serialize export artifact removal with shutdown`)

This slice closes the concrete export finding from the remaining
preview-to-final resource-release audit:

1. Export `Get` and `Cleanup` no longer hold the manager-wide lock while
   unlinking expired artifacts or partials. A blocked filesystem therefore
   cannot prevent `Close` from setting `closed`, canceling the manager, and
   rejecting new admission.
2. Both paths enroll in the existing admission barrier while `manager.mu`
   still excludes shutdown, then release the lock before filesystem I/O.
   `Close` sets `closed` under that same lock before waiting, so no positive
   `WaitGroup.Add` can race a zero-count `Wait`, and artifact storage cannot be
   torn down under an admitted remover.
3. Closing the final download lease had a separate shutdown race: it removed
   itself from manager accounting before performing a deferred unlink, so
   `Close` could miss the lease and return while that unlink was still
   blocked. A pre-shutdown final lease now admits its removal under
   `manager.mu`; once shutdown has begun it skips lease-side deletion and
   leaves the artifact to `Manager.Close`.
4. Worker-side temporary/artifact removal remains owned by `workers.Wait`,
   and secure redemption-time artifact opening remains covered by the
   preexisting admission. Per-entry removal flags continue to serialize
   concurrent cleanup, lookup, and lease-close attempts.
5. Three deterministic tests block the injected filesystem remover for
   `Cleanup`, request-triggered expiration through `Get`, and final-download
   lease close. They prove shutdown begins promptly, cannot return before the
   admitted remover, and performs exactly one artifact unlink. Bounded
   completion checks prevent a future deadlock from falling through to a
   package-wide timeout.
6. Controlled red runs reproduced both defects: `Get`/`Cleanup` blocked
   shutdown admission, and final-lease unlink allowed `Manager.Close` to
   return early. The focused cases then passed 100 ordinary repetitions and
   20 race-enabled repetitions.
7. Independent search-job/executor/snapshot review found the existing
   preview, final-result lease, cancellation, failure, expiration, and
   shutdown ownership paths clean. Two final export reviewers confirmed the
   exact staged patch SHA-256
   `8a63a57ce80fa863be19a296a66c1f1159edd1bd185a0afe0241e1ed8a8bc03c`
   and reported no remaining correctness, efficiency, deadlock, teardown, or
   test-determinism finding.

The exact validation record is under **Latest validation evidence**. This
completes the current release-path resource-release audit pass; the next
priority is the measured sustained-load proof. Known longer-term lifecycle
items remain explicitly listed under **Remaining work, in priority order**.
The overall backend goal remains active.

## Previous checkpoint: sanitized current GradeThis Open Splunk path proof

Date: 2026-07-26

Implementation/proof commit:
`c576e85` (historical commit subject: `prove current GradeThis collector migration`)

Retention foundation:
`458c8b4` (`enforce logical event retention`)

This slice proves a replacement path for the current GradeThis/go-common log
source through the real collector and public backend search path without
changing the exact product-plan v0.1 corpus. It does not modify the target
GradeThis repository's active Compose deployment:

1. `configs/examples/collector.yaml` is now a runnable environment-substituted
   GradeThis profile with gzip transport, an explicit application host,
   trusted `gradethis` index/source/sourcetype/service/environment metadata,
   compressed-file exclusion, a durable state directory, and no OpenTelemetry
   component in the path.
2. A separate sanitized current-source manifest generates 20 deterministic
   NDJSON events with `Request summary statistics`, INFO/WARN/ERROR selection,
   Go `µs`/`ms`/`s` durations, UTC and `-07:00` timestamps, sparse root values,
   a three-layer trace, relative callers, and TEST-NET addresses. The pinned
   default fixture SHA-256 is
   `41f8f92f9170192810bdb741ed79fcf7f9f28c7966bb7e5dd9c54925c0f38f88`.
   Collector-owned metadata is absent from raw JSON.
3. Six current-source investigations are defined once with their expected row
   counts: trace following, severity counts, failed 5xx requests, path/status
   counts, duration-unit extraction with `rex`, and top messages. Ordinary
   tests keep all six progressing through parse, plan, and compile.
4. The real backend vertical creates the `gradethis` index and an index-scoped
   token, validates the committed collector config against exactly one empty
   source file, starts the actual collector binary, observes its zero
   checkpoint, appends and fsyncs the generated fixture, and requires exactly
   20 stored/distinct IDs.
5. That fixture must reach EOF checkpoint, acknowledged/drained WAL, and clean
   collector shutdown. ClickHouse assertions require every trusted metadata
   value and prove those metadata keys never enter raw. The six searches run
   through the public HTTP protobuf contract with three-row signed-cursor
   pages, stable schemas/totals/ordinals/row IDs, exact types and ordering,
   complete scope/range metadata, and no truncation.
6. The collector's last confidentiality boundary now sanitizes mandatory and
   configured policies before WAL or dead-letter persistence. Its ordered
   allow/deny/rename lineage derives from the exact compiled processor
   instances, is event-specific, preserves collisions and constant-field
   provenance, fails closed for unknown processor types, and avoids lineage
   allocation when no ordinary rename source is present.
7. Direct policies redact structured fields, raw bytes, and canonical messages
   in place. Same-replacement rules are grouped, final default policies already
   covered by mandatory names are eliminated, and the shipped GradeThis
   profile therefore performs exactly one direct pre-WAL sanitizer pass.
   Batched alias redaction canonicalizes duplicate JSON members even when the
   last member already equals the replacement, so shadowed secret bytes cannot
   cross the WAL boundary.
8. Decode-failure diagnostics no longer include decoder errors because JSON
   field names are attacker-controlled. Tests cover offline WAL contents,
   rename declassification, allow/deny order, punctuation and normalized
   lookalikes, conflicting replacements, constants, duplicate fields, and
   secret-free logs.
9. The exact product-plan GradeThis v0.1 corpus remains independent and all ten
   searches still pass through the decoder, production ClickHouse Store,
   compiler, executor, manager, and signed-cursor result paging.
10. Three final read-only adversarial reviews found no correctness,
    confidentiality, SPL-fidelity, reuse, or checkpoint-blocking performance
    issue in the frozen artifact. General configurations with multiple
    distinct custom replacement markers still require one raw/message scan per
    marker; a composite resolver is deferred until it can be differential-
    tested against every existing fail-closed encoded/duplicate/depth case.

The exact validation record is under **Latest validation evidence**. At this
checkpoint, the next release-path priority was the remaining preview-to-final
resource-release audit; that pass is now complete at `961cba2`. The overall
backend goal remains active.

At checkpoint creation, unrelated frontend changes were present in the shared
worktree and were deliberately neither staged nor committed by this backend
slice. Inventory and preserve unexpected local changes before continuing.

## Previous checkpoint: logical event retention before physical TTL cleanup

Date: 2026-07-26

Implementation/proof commit:
`458c8b4` (`enforce logical event retention`)

Lifecycle foundation:
`b2b2839` (`prove clock-driven expiration cleanup`)

This slice makes per-index retention an immediate query-visibility boundary
instead of relying on asynchronous ClickHouse TTL merges:

1. The common compiled ClickHouse scan now requires
   `expires_at > IndexTimeCutoff` alongside tenant, index, event-time,
   index-time, and visibility predicates. The cutoff is the immutable search
   snapshot, not ClickHouse `now()`, so preview, final results, retries,
   timelines, field APIs, and export re-execution see one retention boundary.
2. Compiler tests pin the SQL and argument order. Exact millisecond equality
   is expired, one millisecond after the cutoff is visible, and a
   sub-millisecond search cutoff is canonicalized once to DateTime64(3)
   precision and bound identically to the index-time and expiry predicates.
3. One pinned ClickHouse fixture inserts three still-physical rows around the
   artificial cutoff. Raw queries prove all three remain stored and that two
   are logically expired before and after the assertions. Only the
   just-after row can appear through direct execution, a real manager's
   preview/results, export re-execution, timeline, field catalog, or field
   summary; expired-only fields cannot leak through discovery.
4. The runtime default and resolved Store retention must be positive whole
   milliseconds; persisted control-plane/admin policies may also use zero as
   the deployment-default sentinel. This matches the native DateTime64(3)
   storage boundary and prevents positive retention from being silently
   shortened by driver truncation.
5. Event expiration is bounded to the timestamp interval that the pinned
   native Go driver can encode safely. A real-driver integration test writes
   and reads the upper supported boundary, while unit tests reject timestamps
   just outside either bound.
6. Reservation metadata is version 4 for new batches and records the strict
   millisecond contract. Version 3 remains decodable and preserves its legacy
   pre-driver nanosecond calculation so an ambiguous old batch can be retried,
   deduplicated, and advance contiguous visibility rather than wedge
   ingestion. The compatibility path is proven through the real native
   connection and storage.
7. SQLite migration `0009` canonicalizes preexisting retention values:
   positive sub-millisecond values become 1 ms, larger unaligned values are
   floored to the old native-storage result, and zero retains its default
   sentinel meaning. Durable insert and update triggers reject future
   unaligned values.
8. Controlled red tests first exposed the missing scan predicate, accepted
   sub-millisecond policies, unsafe driver range, incompatible legacy replay,
   and absent migration. Final correctness, architecture/performance, and
   code-quality reviewers found no remaining issue after fixes; the final
   reviewer separately verified real-driver v3 replay, both SQLite triggers,
   and every named event-row position.

The exact validation record is under **Latest validation evidence**. The next
release-path priority is a sanitized real GradeThis collector/config
migration, followed by resource-release coverage and measured load work as
ordered under **Remaining work, in priority order**. The overall backend goal
remains active.

## Previous checkpoint: clock-driven expiration and tombstone cleanup

Date: 2026-07-26

Implementation/proof commit:
`b2b2839` (`prove clock-driven expiration cleanup`)

Recovery foundation:
`b80bf0a` (`prove recovered socket stale-frame fencing`)

This slice closes the remaining first-release lifecycle proof:

1. Search WebSocket polling now accepts a context-aware wait function. The
   production default remains a stoppable real timer; tests can drive every
   refresh from one deterministic clock without sleeping or creating a
   goroutine per wait.
2. A real-manager HTTP/WebSocket fixture uses the production search manager,
   export manager, handler, upgraded sockets, controlled executor, active
   result lease, active artifact-download lease, and an unredeemed grant. It
   proves running preview, successful search completion, a real CSV artifact,
   and an invalid-SPL diagnostic failure before advancing time.
3. At exactly one nanosecond before expiry, completed/failed jobs and the
   export artifact remain available and all three sockets answer application
   pings. At the exact expiry boundary, the search/result, diagnostic, export,
   download-grant, and new-lease manager calls all switch to their expired
   contracts, while subscribed WebSockets publish the corresponding state.
4. The subscribed retained-result projection is required to be exactly four
   ordered frames: EXPIRED state, progress, one empty non-truncated RESET, and
   EXPIRED terminal. Every additional or row-bearing preview is rejected.
   Existing result and download leases remain readable while preventing any
   new lease or grant.
5. Expiring an export purges every retained completed terminal containing
   artifact metadata before installing the artifact-free EXPIRED terminal.
   Reconnecting from the old completed checkpoint must therefore receive
   `SEQUENCE_EXPIRED`, never a replay that can briefly restore a stale
   download-capable artifact.
6. Export tombstone retention is measured from the actual transition to
   EXPIRED, matching search behavior. A deliberately late first cleanup can no
   longer expire and immediately delete the tombstone in one pass.
7. The unpinned diagnostic tombstone retires first and closes its socket.
   Result/download leases pin the expired search/export tombstones and backing
   resources. Closing the final result lease synchronously removes the overdue
   search tombstone and rows. Closing the final download lease removes the
   artifact; the ensuing cleanup removes the export tombstone. All remaining
   sockets then close without another application frame.
8. The fixture proves all manual waiters are released, WebSocket poll reads
   quiesce, and the executor is invoked/exits exactly once. Its cleanup
   choreography advances the clock without waking registered pollers, mutates
   both managers, and only then releases those pollers, eliminating a
   stress-reproduced lost wakeup.
9. Controlled red tests exposed both production defects: stale completed
   export replay originally acknowledged `replay_will_follow=true`, and a late
   first export cleanup originally returned `ErrNotFound` immediately.
   Adversarial reviews also found and drove fixes for an assertion in the
   wrong lifecycle table, a socket-timeout false pass, duplicated fixture
   machinery, unordered stale-preview acceptance, completion/poller ordering,
   and the tombstone lost-wakeup race. Final frozen-diff correctness,
   architecture, and efficiency rereviews reported no findings.

The exact validation record is under **Latest validation evidence**. At that
checkpoint, logically authoritative per-index event retention was next; it is
now complete at `458c8b4`.

## Previous checkpoint: recovered-socket stale-duplicate fencing

Date: 2026-07-26

Implementation/proof commit:
`b80bf0a` (`prove recovered socket stale-frame fencing`)

Recovery/progress foundation:
`522b0ac` (`fence search progress by state revision`)

This slice closes the stale-duplicate release boundary without changing the
already-correct production WebSocket client:

1. The direct client test now establishes a gap at sequence 502, resumes the
   retained subscription from 501, applies contiguous replay through 503,
   sends an equal 503 duplicate on the recovered socket, and then applies 504.
   The final checkpoint and event list are exact; no additional gap, error, or
   reconnect is allowed.
2. The ordinary real-Chrome sequence-gap fixture captures a byte-identical
   nonempty preview frame at checkpoint `K` and the original RUNNING state
   frame before inducing the existing real `K+1`/`K+2` gap, reconnect, and
   retained replay.
3. After replay has advanced the production client to `K+2`, the proxy sends
   exact old preview frame `K` through the recovered Playwright
   route-to-page WebSocket boundary. A scoped, byte-exact, bounded page-side
   recorder proves receipt. The subsequent real `K+3` and `K+4` frames apply,
   while always-updated DOM latches prove the stale text, preview table, or a
   non-`resyncing`/missing preview status never appears.
4. The same recovered socket then accepts the real completed state, final
   progress, and terminal event. Before the deliberately stale GET2 response
   is released, the proxy sends the exact old RUNNING state frame again. The
   fixture requires its sequence to be below the current transport checkpoint
   and its state revision below the completed revision.
5. Always-updated DOM latches prove that the job strip never leaves Completed,
   never returns to Queued/Preparing/Running/Canceling, and that the preview
   status stays present as `finalizing` with no stale preview rows. Bounded
   histories remain diagnostics only, so their cap cannot make the correctness
   oracle vacuously pass.
6. Both injections leave the recovered connection count at two and do not
   trigger an extra authoritative GET. The existing stale-GET, final GET,
   result publication, one terminal close, final table, executor-invocation,
   and safety assertions still complete normally.
7. A controlled mutation disabling
   `event.sequence <= previous` made the Chrome case fail exactly at
   `stale preview text never resurrected`. Restoring the production fence made
   the focused, race-enabled, and exact CI-equivalent gates pass.
8. Reviewers drove replacement of direct synthetic dispatch with the supported
   recovered `WebSocketRoute.send`, exact receipt evidence at the routed
   browser boundary, strict sequence/revision staleness checks, scoped and
   bounded instrumentation, reusable preview-status diagnostics, unbounded
   regression latches, and explicit no-gap/no-error unit assertions. Final
   correctness, efficiency, and fixture-quality rereviews reported no
   remaining concrete finding.

The exact validation record is under **Latest validation evidence**. At that
checkpoint, clock-driven terminal job/result/export expiration and tombstone
removal was next; it is now complete at `b2b2839`.

## Previous checkpoint: authoritative browser cancellation

Date: 2026-07-26

Implementation/proof commit:
`787a7f9` (`prove authoritative browser cancellation`)

Progress/recovery foundation:
`522b0ac` (`fence search progress by state revision`)

This slice closes the honest browser-cancellation boundary without changing
the already-correct production lifecycle:

1. A dedicated Chrome case reuses the real in-process SQLite control database,
   SPL parser/planner/compiler, controlled executor, search manager,
   production HTTP/WebSocket handler, and compiled backend UI. It waits for a
   real sequenced progress event and visible preview row before cancellation,
   proving the executor and subscription are active.
2. The cancel route is held before its request reaches the backend. The UI
   must synchronously dispose its one search WebSocket while the job remains
   running and the preview remains visible. Playwright then advances the page
   clock two seconds—past the client's 750 ms first reconnect delay—and
   requires exactly one connection and one close with no socket error.
3. A second discrete Cancel click is issued while the first request remains
   held. This exercises the application's synchronous pending-request guard,
   rather than the DOM double-click filter. Browser-route and server-middleware
   counters independently require exactly one cancel POST; create is also
   exactly once.
4. After the request gate opens, the test decodes the real protobuf
   `CancelSearchJobResponse` and requires the exact job ID, `CANCELED` state,
   positive job revision, matching progress revision, complete phase, and no
   failure. A second gate withholds those response bytes from the application,
   which must still show the running state and preview. This proves canceled
   presentation is authoritative rather than optimistic.
5. Releasing the same response bytes must make the job strip non-busy and
   canceled, restore the Run button, remove preview status and provisional
   rows, and issue no results POST. Browser errors, failed same-origin
   requests, external resources, and external WebSockets remain empty.
6. The Go fixture independently requires one executor invocation, one exit
   whose returned error is `context.Canceled`, zero recovery-control commands,
   one server create, one server cancel, and a retained manager snapshot in
   canceled state with a positive version, finish time, and no failure.
7. CI now includes `TestBrowserSearchCancellation` in the exact
   Docker/ClickHouse plus Chrome selector and retains
   `test-results/browser-search-cancellation` on failure.

The exact validation and adversarial review record is under **Latest validation
evidence**. Reviewers drove the pre-server request gate, second discrete click,
virtual-time reconnect proof, server-side request counter, explicit canceled
executor-exit counter, shared WebSocket URL matcher, and guarded response
waiter. Final correctness, efficiency, reuse, and fixture-quality reviews
reported no remaining blocker.

At that checkpoint, stale-duplicate injection and clock-driven expiration were
next. They are now complete at `b80bf0a` and `b2b2839`, respectively.

## Previous checkpoint: versioned progress and coalesced browser recovery

Date: 2026-07-26

Browser/recovery commit:
`522b0ac` (`fence search progress by state revision`)

Protocol/producer foundation:
`b5502a3` (`version search progress projections`)

Prior terminal-recovery foundation:
`d1286a4` (`prove REST-only terminal gap recovery`)

This slice closes the two progress/recovery boundaries left by the preceding
sequence-gap checkpoint:

1. `SearchProgress.state_version` is additive field 12 in the protobuf
   contract. The search manager populates it from the same job-state revision
   used by `SearchJob`, state-change events, and terminal envelopes. Generated
   Go and TypeScript bindings, producer tests, protocol fingerprints, replay,
   cancel, REST, WebSocket, and vertical assertions pin the field.
2. The browser owns one job-scoped progress revision. Lower progress revisions
   are acknowledged but never rendered; higher revisions apply even if an
   estimated counter decreases. Equal live or terminal projections are
   idempotent when only `elapsed`/`updated_at` drift, while an equal stable
   conflict triggers authoritative recovery. Invalid revisions and envelope
   mismatches also recover without mutating accepted state.
3. A legacy nested zero revision is inherited only from a positive
   authoritative REST or terminal envelope. A standalone unversioned live
   progress frame is not rendered and requests REST convergence. Equal-version
   REST replaces the projection because REST is the convergence authority;
   retrying the same authority on a stable conflict would otherwise loop.
4. Job phase presentation is separate from progress-metric acceptance.
   Completed/canceled/failed/expired state can advance terminal presentation
   without allowing stale or mismatched final metrics to overwrite the current
   counters. Job and progress revision state reset together at job/application
   boundaries.
5. The whole semantic REST recovery cycle is single-flight, not only its HTTP
   request. Concurrent triggers share one continuation, mutate retry/backoff
   state once, and collapse urgent traffic into at most one immediate
   follow-up after settlement. Ordinary live traffic cannot postpone an
   already-queued urgent reconciliation.
6. The new real-browser REST-first fixture drops `K+1`, forwards `K+2`, and
   captures the byte-identical retained replay. REST revision `K+2` is applied
   before replay with deliberately different projection timing. The test then
   releases replay `K+1` alone, confirms receipt on the second browser
   WebSocket, crosses a render barrier, and proves a bounded DOM observer never
   sees one scanned row. It repeats the receipt/render barrier for equal
   revision `K+2`, then uses visible live `K+3` as the ordered message-chain
   acknowledgement. No conflict notice or extra GET is allowed.
7. Existing accepted-WebSocket-terminal and REST-only-terminal companions
   retain their exact causal barriers and request/frame counts. The
   WebSocket-terminal paths now capture GET2 while the job is still running so
   the terminal revision, rather than a broad arrival epoch, deterministically
   makes it stale and requires GET3.
8. Frontend tests now include a pure revision reducer suite and total 42.
   CI runs the Docker/ClickHouse vertical, sequence expiration, ordinary gap,
   REST-only terminal gap, and REST-first progress gap under the exact selector
   below, retaining a separate REST-first Playwright artifact.

The exact validation and adversarial review record is under **Latest validation
evidence**. Reviewers found and drove fixes for duplicate recovery
continuations, repeated metric dispatches, no-op notice allocations, scenario
flag ambiguity, a potentially batched false-pass, an incorrect DOM regex, and
observer/listener cleanup. Final semantics, efficiency, reuse, and
browser-fixture reviews reported no remaining blocker.

At that checkpoint, browser cancellation, stale-duplicate injection, and
clock-driven expiration were next. They are now complete at `787a7f9`,
`b80bf0a`, and `b2b2839`, respectively.

## Previous checkpoint: browser sequence-expiration and transient recovery

Date: 2026-07-25

Transient-failure proof:
`ad4a77b` (`prove transient browser recovery failure`)

Sequence-expiration implementation:
`8f6d569` (`prove browser sequence expiration recovery`)

Preceding real-recovery checkpoint:
`a2af9e0` (`record real websocket recovery checkpoint`)

This slice proves `SEQUENCE_EXPIRED` recovery through the production React
workspace, generated protobuf HTTP/WebSocket transports, and a real search
manager:

1. A dedicated in-process fixture uses a real SQLite control database, SPL
   parser/planner/compiler, controlled executor, search manager, bounded
   one-event replay ring, production HTTP handler, and Chrome. It creates one
   browser-owned job and forwards its initial preview at checkpoint `K`.
2. The fault proxy withholds real progress `K+1` and `K+2`, closes the first
   socket, and requires the browser to reconnect the exact same job,
   subscription, preview options, and `after_sequence=K`. The real server
   responds with `SEQUENCE_EXPIRED`; the fixture validates the routed
   subscription, target, checkpoint, earliest/latest bounds, and
   acknowledgement before recovery may proceed.
3. The first authoritative recovery GET is held while real progress `K+3`
   arrives, then returns an actual HTTP 503. No snapshot or queued progress is
   applied, the UI remains `resyncing` at zero rows, and the next connection
   again subscribes from exact `K` with the original identity and options.
4. A deliberately stale second response with `state_version=0` is also
   rejected without advancing the checkpoint. The fourth connection again
   resumes from exact `K`. Its fresh authoritative snapshot is captured at
   three rows and held while real `K+4` arrives; the UI remains at zero until
   the three-row response is successfully applied and acknowledged, after
   which the queued WebSocket event is required to establish four rows.
5. A watchdog race captures an authoritative GET at four rows, delivers live
   WebSocket progress `K+5`, and only then releases the older response. The
   production live-update epoch fence rejects that in-flight snapshot, so the
   visible five-row state cannot regress even when its numeric state version
   is otherwise acceptable.
6. The same recovered connection accepts a fresh preview and terminal result.
   The final authoritative two-row projection replaces preview state. The
   executor is invoked once and exits once; job creation, sockets, recovery
   GETs, frames, commands, diagnostics, and waits are all bounded.
7. The client now rejects a resynchronization payload whose subscription does
   not match its routed envelope and rejects inverted replay bounds. Recovery
   acknowledgement remains provisional until the asynchronous recovery
   handler resolves; queued events and the checkpoint stay suspended in the
   meantime. That commit-protocol property is pinned directly by the unit
   suite, while the browser fixture proves the application handler does not
   acknowledge a failed authoritative request.
8. Integration builds use a unique staged repository per Go test process,
   with shared immutable output only inside that process. Raw frontend builds
   and parallel integration processes no longer collide through `.next` or
   `out`. Cleanup validates its exact temporary target. CI runs both real
   browser cases, preserves both artifact directories, and gives the combined
   Go package a 12-minute timeout inside the 15-minute job budget.

Final validation on the exact committed implementation passed:

```sh
go test ./... -count=1
go test -race ./integration ./internal/server ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequenceExpiredRecovery)$' \
  -count=1 -timeout=12m -v
git diff --check
```

The frontend suite contains 32 passing protocol tests. The latest combined
Docker/ClickHouse and Chrome gate completed in about 34 seconds. The real
staged recovery case also passed under the race detector, and repeated harness
race checks left no staged directory or Docker container behind.

Independent correctness, concurrency/performance, and quality rereviews
reported no remaining concrete finding. A follow-up transient-failure review
found that the first draft fetched its fresh snapshot after `K+4`, which could
have hidden a dropped queued frame; capturing the snapshot at three rows
before sending `K+4` closed that false-positive path.

At that checkpoint, the next recommended slice was explicit browser
sequence-gap injection: forward `K+2` while dropping `K+1`, require the client
to reconnect from `K`, and prove contiguous replay.

## Previous checkpoint: resumable WebSocket recovery

Date: 2026-07-25

Implementation commit:
`86d4dd8` (`complete resumable websocket recovery`)

Starting checkpoint:
`510c2e0` (`harden websocket resume recovery`)

This slice completes the in-memory protocol and first-party client contract
needed before a deterministic real-browser disruption test:

1. A connection establishing a new server epoch may resume from any retained
   sequence between the epoch start and the latest published frame. The server
   verifies that the entire retained suffix is continuous before replaying it.
   An out-of-range checkpoint still reports `SERVER_RESTARTED`; an incomplete,
   overdue, or gapped suffix reports `SEQUENCE_EXPIRED`.
2. Resynchronization is a commit protocol. A recovery listener must call the
   supplied acknowledgement before the client advances its checkpoint.
   Rejected and resolved-but-unacknowledged handlers retain the old checkpoint
   and reconnect. A same-epoch checkpoint regression is rejected, while an
   explicit `SERVER_RESTARTED` response may establish a lower new-epoch
   boundary.
3. Successful sequenced work establishes connection stability. Subscription
   acknowledgement alone arms only a delayed stability timer, and that timer
   is not armed until all generic event listeners finish successfully. This
   prevents repeated recovery failures or a slow/rejecting acknowledgement
   listener from collapsing exponential reconnect backoff.
4. Every subscription identifier is single-use for the lifetime of a
   connection, including after unsubscribe. The exact 256-identifier ceiling
   is bounded. A 257th unique identifier receives
   `TOO_MANY_SUBSCRIPTIONS` with `connectionWillClose=true`; the server closes
   with retry semantics and the first-party client preserves and retries the
   rejected local subscription on a fresh connection.
5. Positive unknown future protobuf event variants are target-fenced, exposed
   only to generic listeners, and allowed to advance the sequence checkpoint.
   Malformed or mismatched frames cannot advance it.
6. An authoritative zero-row preview or invalidating terminal projection
   removes all retained preview state and exact replay accounting. Expiration
   can therefore never resurrect older row-bearing preview frames.
7. Expired terminal targets with active subscribers continue bounded
   tombstone polling. A missing first read at or after expiry retires the
   target; later tombstone deletion does the same. Retirement removes the
   exact cached target and closes existing sockets so reconnect observes
   `JOB_NOT_FOUND` rather than remaining stranded on stale state.

The implementation adds focused Go and frontend tests for every boundary
above, including a fresh-epoch lagging replay, zero-row preview expiration,
first-expiry tombstone deletion, active-subscriber retirement, identifier
reuse and exact exhaustion, unacknowledged resync, epoch regression, lower
restart epochs, unknown future variants, repeated recovery failure backoff,
and slow acknowledgement listeners.

Final validation passed:

```sh
go test ./... -count=1
go test -race ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

The frontend suite contains 29 passing protocol tests. Independent backend,
frontend, correctness, performance, and reuse/quality reviews found no
remaining checkpoint blocker after their findings were fixed. The last
focused reviewers reran the first-expiry and identifier-exhaustion boundaries
20 times and rechecked the delayed-listener race and reconnect retry contract.

The real-browser retained-replay path and real-manager expiration/cancellation
fixture were subsequently completed in `7136f29` and `72bd6ca`. The remaining
fault cases are recorded under **Remaining work, in priority order**. The
overall backend goal remains active.

## Previous checkpoint: crash-safe collector and server process restarts

Date: 2026-07-25

Starting checkpoint:
`21d21cb` (`prove exact GradeThis compatibility corpus`)

Use the current `main` HEAD for the completed restart checkpoint. The commit
containing this section is created after all gates below pass and is pushed
directly to `origin/main`.

This slice closes the first release's explicit cross-process durability proof:

1. The backend vertical starts with an empty discovered `app.log`, appends one
   primer, and waits for its ClickHouse row and durable source checkpoint.
2. It hard-kills the collector at that transaction boundary, accepts either a
   drained WAL or the one valid checkpoint-before-local-WAL-ack crash state,
   reopens the WAL for inspection, and restarts against the same WAL and
   checkpoint directory.
3. After two more acknowledged events, it waits for the collector's WAL
   acknowledgment high-water to settle, hard-kills the server, appends and
   fsyncs a final sentinel, waits for an actual `segment-*.wal` append, and
   fsyncs the observed segment from the harness before the deliberate kill.
4. It hard-kills the collector, reopens its WAL, and requires exactly one
   pending event with the sentinel's exact byte and physical-line cursor.
5. The same server SQLite database/master key and collector WAL/checkpoints are
   reused. Four distinct stable event IDs must become queryable with no loss.
   The documented at-least-once boundary permits one physical sentinel replay
   in a second batch, so the acceptance SPL uses `dedup event_id` and still
   requires exactly four logical results.
6. A clean final collector shutdown must leave the exact EOF checkpoint and a
   drained WAL with a positive acknowledged sequence before the existing
   protobuf HTTP, binary WebSocket, browser, timeline, paging, and export
   assertions continue.

The acceptance test exposed a real collector bug: each poll constructed a
one-shot framer whose line counter restarted at one. Byte offsets and stable
event IDs were safe, but origins and resumed checkpoints regressed their
physical-line metadata. The fix carries an explicit cursor end to end:

- protobuf `EventOrigin` has additive optional `next_line_number`;
- every line, multiline, flush, max-lines, and oversized frame reports its
  half-open physical-line interval;
- the tailer owns the cursor across polls and resets it only for a new
  copy-truncate generation;
- decoder origins, compact WAL marks, recovery, acknowledgment aggregation,
  local terminal delivery, checkpoint persistence, and server ingestion
  validation retain and check it;
- equal or increasing byte offsets cannot carry conflicting or regressing
  present line cursors, and the reserved `uint64` maximum cannot enter a batch
  that the server would be unable to acknowledge; and
- an oversized record that reaches the current EOF retains bounded
  discard-through-delimiter state, so a later suffix is never published as a
  separate event. A crash safely reconstructs that state from the older
  durable byte checkpoint.

`start_at=end` deliberately numbers the newly monitored stream from line one;
it does not scan skipped historical bytes. Checkpoints written before
`next_line_number` are handled in O(1). Ordinary line-framing checkpoints can
derive the exact next line from the prior event and are upgraded in one atomic
batch per discovery pass. A legacy multiline checkpoint—or a legacy nonzero
discovery offset with no line anchor—cannot recover its exact ending physical
line without an unbounded prefix scan. Those sources therefore omit both
optional line fields instead of publishing an approximation; byte offsets and
stable event IDs remain exact. A new file generation restores exact line
metadata. This compatibility boundary avoids both false origin data and
stranding large existing checkpoints.

Focused collector/input/WAL/ingest/integration tests and the full collector
race suite passed after these fixes. The ordinary, frontend, build,
browser/Docker vertical, generated-protobuf, and cleanliness gates are recorded
under **Latest validation evidence**.

The next recommended release-path slice at that checkpoint was deterministic
browser disconnect/resume and replay-window-expiration coverage. The overall
backend goal remains active; it was a safe pause, not completion of the
product plan.

## Previous checkpoint: exact GradeThis v0.1 compatibility corpus

Date: 2026-07-25

Starting checkpoint:
`221b4ab` (`record GradeThis corpus pause checkpoint`)

Frontend result adaptation:
`430e6ce` (`fix top result visualization adaptation`)

Use the current `main` HEAD for the exact corpus implementation commit. It is
committed after this document is updated and must be pushed directly to
`origin/main`.

This slice completed the product plan's named ten-search GradeThis
compatibility criterion:

- one shared `v0.1` manifest owns the exact ten SPL templates used by ordinary
  compiler coverage, the legacy executor smoke, and the exact acceptance test;
- one deterministic 20-event synthetic profile generates byte-pinned NDJSON
  with a strict secret/PII/non-documentation-data scanner;
- an ordinary test decodes every event with the production collector and pins
  canonical timestamps, messages, trace IDs, and typed request fields;
- a focused integration test writes that decoded batch through the production
  ClickHouse store, then executes all ten searches through parser, planner,
  compiler, executor, search-job manager, and owner/tenant-scoped signed paging
  against ClickHouse `26.3.17.4`;
- every public schema flag, ordered row ordinal, typed cell, total, completion
  state, and terminal cursor is exact; result sets larger than three rows must
  cross opaque cursor pages; and
- required CI runs the focused pinned-ClickHouse corpus before production
  packaging. The `top message` frontend adapter now treats `percent` as a
  measure and has a real protobuf-shaped categorical-visualization regression.

The acceptance pass exposed and fixed a real sparse-result transport defect:
ClickHouse JSON columns can report paths absent from an individual row as
null when another row in the part has that path. Ordinary raw-event searches
now carry a bounded private `field_names` presence column through SQL, validate
its exact transport contract, and reconstruct only the fields present on that
row. The private column never appears in the public schema. The fixture
contains one explicit-null field beside rows where that field is missing, so
the pinned test proves the distinction across cursor pages.

The logical/physical path codec and allocation-free normalized-size accounting
for dots, backslashes, and percent escapes are shared by ingestion, storage,
and result conversion. Canonical path count, depth, segment,
aggregate-name-byte, ordering, collision, and reserved-root bounds are
enforced before ingestion acknowledgement after redaction, again at storage,
and again while converting search results. Path parsing preallocates only the
hard 16-segment maximum even when a segment contains many escaped dots.

Adversarial review also hardened the corpus scanner against fused and
multiword credential/PII keys, credential assignments and bearer tokens in
values, embedded Unix/Windows paths, production URLs, duplicate JSON keys,
and non-documentation IPv4/IPv6 addresses including bracketed IPv6 ports.
Scanner diagnostics use ordinal key locations and never echo rejected key
text. Failed owner/tenant access and tampered signed cursors are direct
acceptance assertions.

Independent correctness, security/maintainability, and performance reviewers
reported no remaining checkpoint blocker after those fixes. A final simplify
pass then removed redundant sparse-column state and allocations, centralized
raw-fields removal, and routed path encoding, byte accounting, typed-nil
handling, String comparison, and the broad executor image default through
shared utilities.

The exact corpus and the full existing query-executor ClickHouse suite both
passed at this checkpoint. No ephemeral `open-splunk-*` container remained.

## Previous pause checkpoint: collector-to-browser gate and GradeThis corpus audit

Date: 2026-07-25

Branch: `main`

Starting checkpoint:
`d0949e8` (`record scalar extrema backend checkpoint`)

Durable-ingestion commit:
`5b99c83` (`prove durable backend vertical ingestion`)

Collector-to-browser gate:
`cf37bd7` (`add collector to browser acceptance gate`)

This document is committed immediately after the acceptance-gate commit. Use
the current `main` HEAD as the document commit rather than copying an older
hash from this file.

The earlier branch repair is complete: the work is on `main`, all current
checkpoint commits were pushed directly to `origin/main`, and subsequent work
must continue on `main`.

The overall backend objective remains active. Work was intentionally paused at
this earlier green checkpoint and later resumed. The product architecture plan
is not complete.

Immediately before this pause, three read-only agents audited the product
plan's ten GradeThis searches, the current fixtures, and the compile and
ClickHouse integration coverage. Their conclusions and the exact next
test-driven slice are recorded below so a new conversation does not need to
repeat that investigation.

## Earlier pause slice: collector-to-browser completion

### Deterministic self-contained server build

- `make build` and `make build-server` now compile the static frontend in
  backend mode by default.
- The release-path integration test explicitly clears any ambient API base URL
  and rebuilds the frontend before compiling the Go server.
- The compiled server starts from an empty temporary working directory with a
  deliberately nonexistent `PATH`, proving it does not need Node.js,
  repository files, or another executable runtime.
- The test checks the embedded HTML is the backend workspace, exercises the
  protobuf bootstrap route, and then uses that same compiled server for every
  protocol and browser assertion.

### Live collector durability proof

- The vertical starts the collector against an empty `app.log` and waits for
  the exact zero-offset discovery checkpoint before writing data.
- It appends and fsyncs one generated primer event, waits for the ClickHouse row
  and a checkpoint at that file boundary, then appends and fsyncs the remaining
  fixture. This proves real tailing after discovery and after a durable
  acknowledgment rather than ingesting a file that was already complete.
- The final checkpoint must reach the exact synced EOF. After a clean collector
  shutdown, the WAL must be drained and retain a positive acknowledged
  sequence high-water mark.
- The server still provisions the index and scoped ingestion token over
  protobuf HTTP, and secret values are checked against collector/server logs,
  typed result pages, and downloaded export bytes.

### Protocol, paging, and real-browser proof

- The Go protocol client creates a search, observes an acknowledged binary
  protobuf WebSocket subscription with monotonic state/progress/terminal
  sequences, and fetches the four authoritative results over exactly two
  opaque cursor pages.
- Cursor tokens and row IDs cannot repeat, ordinals must be globally
  contiguous, schemas must remain stable, total size must be exact, and the
  retained snapshot must be complete.
- A pinned Playwright test launches Chromium against the UI embedded in the
  compiled Go server. The browser creates its own search through same-origin
  protobuf HTTP, receives binary WebSocket traffic, renders the timeline, and
  ends with four authoritative non-preview rows containing the fixture
  sentinel.
- The browser observer uses the repository's generated TypeScript protobuf
  codecs. It correlates the create response, outgoing subscription, positive
  sequence `search_progress` frame, results response, and rendered UI to the
  same browser-created job.
- A recorded `live` or `finalizing` preview transition proves the application
  applied live preview traffic before replacing it with authoritative results;
  paused, resynchronizing, and finalization-error states fail the test.
- Browser errors, failed same-origin API requests, external HTTP(S) resources,
  and foreign WebSockets are bounded and rejected. Playwright output is capped,
  failure screenshots/traces are retained, and timeout cancellation gives
  Playwright a bounded graceful shutdown before a hard-kill fallback.
- The existing timeline, typed redaction, one-time JSONL export, and 10,001-row
  bounded export re-execution checks remain in the same release-path test.

### Required CI gate

- CI installs the pinned Playwright dependency and Chromium build, runs the
  opt-in Docker collector-to-browser test as a required `backend-vertical`
  job, and uploads browser failure artifacts.
- Production binary packaging now depends on that gate.
- Local setup and the exact opt-in command are documented in the root and
  integration READMEs.

## Previous checkpoint reference: optimized scalar-String `stats min` and `stats max`

Open Splunk already implemented typed `stats min(field)` and
`stats max(field)` from SPL parsing through ClickHouse execution, downstream
calculated-field use, search-job transport, documentation, and editor support.
This slice removed the known singleton-Array overhead for fields proven
statically scalar String while retaining the guarded multivalue path.

### SPL, AST, and plan contract

- `min` and `max` are case-insensitive and accept exactly one unquoted, exact
  field.
- The default names are canonical `min(field)` and `max(field)`; use `AS` when
  the current downstream field grammar must reference the result.
- Empty calls, expressions, quoted fields, wildcards, multiple arguments, and
  forged aggregate metadata fail explicitly rather than being approximated.
- Separate AST and plan enum values, canonical-name mapping, required-input
  validation, reserved open-schema `fields` handling, output uniqueness, and
  relational-depth checks are covered by tests.
- A closed schema may legitimately contain a calculated field named `fields`;
  the planner accepts extrema over that field.
- The search editor and compatibility contract advertise the supported forms.

### Runtime ordering and typed results

- Missing, explicit null, an empty multivalue, and null multivalue members do
  not participate.
- Every immediate scalar member of a top-level multivalue participates without
  expanding event rows. Nested arrays and objects, generic objects, and
  flattened object parents fail atomically with the existing sanitized stats
  unsupported-value marker.
- Runtime String/Dynamic candidates use a deterministic two-class order:
  finite numeric candidates sort before lexical candidates; numeric candidates
  compare numerically and lexical candidates compare by raw bytes. Therefore
  `min` selects a numeric candidate whenever one exists, while `max` selects a
  lexical candidate whenever one exists.
- Numeric text must be valid UTF-8, at most 4 KiB, match the complete decimal
  grammar, and convert to finite `Float64`. Whitespace, `NaN`, infinity,
  overflowing exponents, invalid UTF-8, and longer values remain lexical.
- Numeric zero is normalized to positive zero. Equivalent spellings such as
  `01`, `1.0`, and `1e0` publish the same `Double(1)` result.
- Runtime winners are nullable `Mixed`: numeric winners are `Double`, lexical
  winners retain their String/Bytes representation, and no winner is null.
- Statically typed numeric, Boolean, and timestamp scalars use native
  `minIfOrNull` / `maxIfOrNull`, preserving physical type and timestamp
  nanosecond precision rather than converting through `Float64`.
- Global aggregation over no rows emits one null result row. Retained groups
  with no eligible value contain null; grouped aggregation over no rows emits
  no groups. Projected-away inputs remain absent.

Punctuation placement is deliberately documented as an Open Splunk v0.1
boundary. Public Splunk documentation warns that symbol ordering is not
standard. The pinned fixture now includes numeric text with `!` and `~` so the
chosen numeric-before-lexical/raw-byte behavior cannot drift silently.

### Efficient ClickHouse lowering

- A proven scalar String field now materializes one nullable String alias, one
  bounded finite-`Float64` classifier alias, and one
  `(class, numeric-key, lexical-key)` tuple candidate.
- Scalar extrema use conditional `minIfOrNull` / `maxIfOrNull` tuple
  aggregation. Dynamic and `StringArray` inputs retain the guarded Array path,
  including unsupported-container validation.
- One stable per-field descriptor coordinates `min`/`max`, `values`, and
  `dc`, including interleaved fields and either measure order.
- Repeated identical extrema share one physical aggregate key and one stored
  type. An immediate post-aggregate projection reconstructs every public
  `Mixed` alias and drops the tuple key before any downstream
  `SELECT *`, filter, sort, or limit can widen rows.
- `min`/`max` and `values` over the same scalar input share the original String
  materialization while values alone retains exact, bounded Array semantics.
- Both paths use the same numeric normalization, tuple ordering, and
  Null/Double/String/Bytes stored-type mapping helpers, preventing subtle
  command or lowering drift.
- The lowering performs no `ARRAY JOIN`, row expansion, sorting, or unbounded
  list aggregation.
- The numeric text grammar is shared with `bin`, preventing command semantics
  from drifting.
- Runtime result type metadata is private and survives `rename`, `eval`,
  downstream `bin`, and re-aggregation.
- A transforming aggregate no longer causes downstream `bin` to reference the
  event-only `__os_field_metadata_version` column. Calculated semantic type
  metadata is authoritative after event rows have been transformed.
- Generated placeholder count/order and the existing generated-SQL and
  relational-depth limits remain exact.

## Adversarial review record

Three independent agents reviewed the collector-to-browser change for
correctness, protocol semantics, reliability/performance, and reuse/code
quality. A second read-only pass reviewed the hardened result. Findings and
resolutions:

1. **A completed file was not a sufficient tailing proof.** The test now starts
   with an empty discovered file, appends a primer, waits for its durable
   checkpoint, and only then appends the remainder.
2. **Binary traffic alone could belong to another job.** The Playwright
   observer now decodes generated protobuf messages and requires an outgoing
   subscription plus a positive-sequence `search_progress` event for the exact
   job ID decoded from the browser's create response.
3. **Wire arrival alone did not prove UI application.** A mutation recorder
   requires the preview UI to enter `live` or `finalizing`, rejects recovery
   error states/notices, and then requires final non-preview rows.
4. **Manual field-number parsing duplicated generated code.** The browser spec
   is strict TypeScript and uses the checked-in generated codecs for create,
   results, WebSocket command, and WebSocket event messages.
5. **Failure-path promises could reject before their normal await.** Response
   waiters and the protocol completion are marked handled immediately, the
   original promises remain authoritative, and protocol listeners/timers are
   disposed in `finally`.
6. **Resource and cleanup paths were unbounded.** Browser recorders, child
   output, subscriptions, and progress events are capped. Context cancellation
   first signals the Playwright process group so it can close Chromium, then
   uses the standard bounded hard-kill fallback.
7. **Paging checks had assembled synthetic response metadata.** Each real
   protobuf page is retained and inspected directly; only stable schema and
   cloned rows are collected for cross-page assertions.
8. **Opt-in CI could silently skip without Docker.** Setting the integration
   flag now makes a missing Docker CLI fatal.

The final correctness and quality re-audits reported no remaining concrete
blocker. The performance review reported no checkpoint blocker; its remaining
harness and CI optimizations are recorded below as follow-up work rather than
weakening the release-path proof.

### Previous scalar-extrema reviews

Three independent post-implementation agents reviewed correctness and SPL
semantics, ClickHouse execution/performance, and reuse/maintainability. Their
concrete findings were handled as follows:

1. **Stop-ship downstream failure fixed.** `stats min(metric) AS low | bin low`
   originally referenced the event-only metadata-version column after `stats`
   had dropped it, causing ClickHouse Code 47. The compiler now distinguishes
   event metadata from authoritative calculated metadata. A unit assertion and
   executed pinned-ClickHouse regression cover this path.
2. **Shared logic consolidated.** Numeric-bin and extrema decimal grammar now
   use one constant. Extrema, `dc`, and `values` share one String-input
   resolution/cache helper.
3. **Documentation corrected.** “No row materialization” was too broad; the
   true invariant is no row expansion. Symbol ordering is now labeled as an
   explicit compatibility boundary.
4. **Coverage strengthened.** Native Boolean extrema execute for both values.
   Native timestamp extrema now compare distinct timestamps one nanosecond
   apart, proving ordering and precision. Punctuation, closed-schema `fields`,
   and min-plus-values input sharing gained regressions.
5. **Test-only parser mistake corrected.** An attempted Bool integration probe
   used an unsupported comparison inside `eval`; it was replaced with the
   supported Bool-literal path.

The final reviewer verdict reported no remaining stop-ship correctness,
security, argument-ordering, relational-depth, optimizer-sharing, or
unbounded-state issue.

The scalar optimization received a second three-agent adversarial pass. It
found and resolved:

1. **Per-field/result cache drift.** Independent ordinals and repeated
   expressions were replaced with stable field descriptors and one cached
   result per `(input, function)`. Tests pin interleaved `host`/`service`
   wiring, exact calculated-argument order, and the 64-measure ceiling.
2. **Private tuple-key retention.** The first cache revision could carry a
   winning String twice through downstream `SELECT *` stages. Reconstruction
   now happens in an immediate cleanup projection that excludes every tuple
   key, and the added relational level is explicitly pinned.
3. **Coverage and fixture cost.** Eight filtered scalar queries became one
   grouped result comparison. Repeated aliases are executed and scanned as
   well as explained. A 4-KiB-plus-one String pins the lexical length boundary
   through both scalar and Dynamic paths and downstream re-aggregation.
4. **Reproducible performance evidence.** An opt-in benchmark now uses the
   production classifier/ordering helpers, a generated two-million-row corpus,
   the pinned server, one thread, disabled query cache, fixed samples, and
   `system.query_log` memory/duration. Its documentation explicitly limits the
   claim to helper-level lowering; compiler shape is covered by assertions and
   pinned `EXPLAIN`.

All three reviewers reported no remaining concrete correctness, efficiency,
or code-quality finding after those fixes.

## Latest validation evidence

### Bounded chronological `stats earliest/latest`

The reviewed chronological sequence
`932f4036e2967d5304a95b27b7109e15ffcbf601`,
`ac721fb1d84f746d8783b02a8e1b5ac13fef14f3`,
`e6acd1d01ef5ff13608f6cd551e0d3d817debfaf`,
`9714c795ab93c334120e7391057f72297748164c`, and
`f9985a1184b43a78a5ae5ef8761c9ff649ec1836` passed:

```sh
go test ./internal/clickhouse \
  -run '^(TestCompileStatsChronological|TestWrapChronologicalValidation)' \
  -count=1
go test ./... -count=1
go vet ./internal/clickhouse
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
git diff --check
```

The strengthened pinned ClickHouse run completed in 40.726 seconds after the
final argument-ownership and nested-member regressions were added. It executed
the 2,000,000-member Dynamic fixture with all four tuple components consumed
under `max_memory_usage=1073741824`, valid and poisoned terminal `chart`
outputs, downstream literal binding, nonwinner and hidden poison, null/empty
groups, supported scalar types, and deterministic source ties. The
query-executor integration completed in 12.561 seconds; it is rerun after any
compiler argument-order change.

The complete Go suite passed. The exact cumulative lint gate returned
`0 issues`; focused vet and diff checks also passed. The unchanged frontend
baseline passed 47 release/script tests, 107 application tests, type checking,
lint, and a production build earlier in the same chronological slice.

The performance review reproduced why the discarded `arrayFold` design was
unsafe: one 250,000-member row approached the exact 1 GiB limit, while the
selector design processed eight such stored-Dynamic rows at about 10.3 MiB.
The fixed `Array(String)` path used about 7.4 MiB. A 5,000,000-row validation
probe found one additional logical final-result pass but no measured peak
memory or wall-time regression. This extra pass is a nonblocking P3
optimization opportunity, not a correctness exception.

Correctness, performance, and code-quality reviewers all finished read-only
current-tree reviews. The final code-quality pass caught the materialized-CTE
bind-order defect and missing live nested-member case before commit; both now
have permanent red-then-green regressions. Final reviews reported no remaining
P1/P2 correctness, efficiency, performance, or maintainability finding.

### Cumulative Go lint ratchet and boundary hardening

The reviewed commits `b0c00f370323221f4bce50457caf11db3f3b939c`,
`fbb89976271f96026e84964b036e7094d932f2cd`, and
`4e0042805c9c1a7481c4310c3f6780907231a12a` passed:

```sh
go test ./... -count=1
go test ./internal/collector/... -count=1
go test ./internal/auth ./internal/visibility -count=1
go test -race ./internal/collector/... -count=1
go test -race ./internal/auth ./internal/visibility \
  -shuffle=on -count=10
go vet ./internal/collector/...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
  run --timeout=5m \
  --new-from-rev=327a1625b7a080c9c52a31b856da03633c4cb102
git diff --check
```

The exact repository ratchet returned `0 issues`. A separate uncapped
inventory run with both per-linter and duplicate caps disabled recorded 1,365
remaining inherited findings: 664 `govet`, 273 `gosec`, 169 `revive`, 104
`errorlint`, 71 `noctx`, 39 `staticcheck`, 26 `misspell`, and 19 across the
remaining enabled linters. The adoption inventory was 1,468, so this wave
removed 103 concrete findings without weakening the configured linter set.

The collector reviewer also ran Darwin/Linux amd64 and Linux 386 cross-builds,
framing and decoder fuzzing, a zero-allocation guard-fingerprint benchmark,
and the full collector race suite. The auth/visibility reviewer ran ten
race-enabled shuffled repetitions. The command/CI reviewer reran focused
command, integration, and migration tests and parsed the workflow YAML.

Adversarial review found and fixed two test-coverage regressions, a Darwin
checkpoint-key compatibility bug, and six unnecessary migration-test taint
suppressions. Final reviews reported no remaining correctness, security,
concurrency, performance, portability, or CI-ratchet blocker.

GitHub Actions run `30255910487` then passed every job: the cumulative Go lint
ratchet, race-enabled Go tests and coverage, vulnerability scan, frontend,
protobuf, backend vertical, pinned GradeThis ClickHouse corpus, Linux and
macOS production builds, Linux packaging, and the cross-platform canonical
embedded-asset comparison.

### Bounded ordered `stats list(field)`

The implementation at `4e2ddb43ddb60ecd790c6ad3783fd7d83ecfda72` and CI
repair at `05c1eaff6a373d220762c06838677e5db3fd6ee6` passed:

```sh
go test ./... -count=1
go test -race ./internal/searchhistory -shuffle=on -count=20
go vet ./...
npm run test:frontend
npm run typecheck
npm run lint
go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1 -v
git diff --check
```

The complete Go suite passed, including all parser, planner, compiler,
executor, migration, and transport packages. The repaired search-history
fixture passed 20 race-enabled randomized repetitions. The frontend gate
passed 47 release/protobuf/materialization tests plus 106 application tests;
typecheck and lint also passed.

`govulncheck` reported no reachable vulnerabilities after the `x/text`
upgrade. It still reported unreachable advisories in imported packages and
required modules; those are not presented as fixed or reachable.

The final pinned ClickHouse run completed in 36.17 seconds. An independent
performance/security reviewer reran it in 38.3 seconds and also ran the full Go
suite and vet. Both pinned runs passed the ordered list corpus and left no test
container. Controlled red tests first failed for the legacy source-identity
tie and for the projected-away input's unnecessary windows. The permanent
corpus also executes forged post-aggregate relations to prove that combined,
duplicate-alias, and whole-result limits cannot be hidden by a downstream
`LIMIT`.

The correctness reviewer found the legacy `visibility_seq = 0` ordering hole.
The performance reviewer found the known-empty input sort and missing runtime
whole-result proofs. The simplify reuse/quality/efficiency pass consolidated
the byte-accounting helper, shared list/values policy constants, and frontend
function-name source. After those changes, final adversarial review reported
no remaining correctness, performance, security, or maintainability blocker.

### Deterministic committed release identity and artifacts

The implementation at `5ecd99957bf4801da8b39e9bfabd274e11d5e208` and
cleanup/proof fix at `f68630a5b4fc213a379bda1f2c163b4c96b42fac` passed:

```sh
npm run test:frontend
npm run typecheck
npm run lint
make proto
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0-test \
OPEN_SPLUNK_SOURCE_REVISION=0123456789abcdef0123456789abcdef01234567 \
make build-server
build/open-splunk-server -verify-embedded-release
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release
git diff --cached --check
```

The final frontend gate passed 47 release, protobuf, source-materialization,
and publication-transaction tests plus 106 application tests. The complete Go
suite, race detector, vet, build, typecheck, lint, protobuf regeneration,
production embed verification, and Docker-backed vertical all passed. The
vertical completed in 22.35 seconds, stored four distinct events with zero
replay, survived collector and server crash restarts, and passed all six
current GradeThis searches.

The two `make release` runs used exactly Node `24.18.0`, npm `11.16.0`, and Go
`1.26.5`. The cold run took 37.85 seconds with 861,011,968-byte peak RSS; the
prior-artifact replacement run took 36.32 seconds with 856,113,152-byte peak
RSS. Their six published files were byte-for-byte identical with identical
modes and sizes. All three binaries reported version `0.1.0` and full revision
`f68630a5b4fc213a379bda1f2c163b4c96b42fac`; their embedded manifest and
verification output agreed. The UI contained 119 files with SHA-256
`20520d9edbf374ae647ee293a68d966efa23e431920f06f9b787fc8bfe83caa4`.
Both runs kept `HEAD` and the worktree unchanged and left no launcher, work,
publication, prior-build, failed-build, or lock residue.

The workflow now defines independent Linux amd64 and macOS release builds and
a byte-for-byte comparison of their canonical proof files. That cross-platform
gate will run from the pushed commit; the exact local two-run proof above was
performed on macOS and does not claim a completed remote CI run.

Correctness/concurrency and performance/security reviewers covered publication
serialization, stale-source rejection, rollback, cleanup failure propagation,
proto-generation transactions, Git-object streaming, resource and path
bounds, cross-platform collisions, physical temporary roots, and adversarial
symlink retargeting. They reported no remaining issue for implementation staged
SHA-256
`cb29202d24c47dc46fecc0bd88702865b2a2a33da7707678eca0a74f84b8ce0e`
or cleanup staged SHA-256
`8a9cf57c8d5a96b256937b16a32e5433941ed5094b482464358f03ad69f76503`.

### Result-kind-bounded browser adaptation

The exact implementation at `c20204b667c5711bc9c4484ba43d046e3a9f65d4`
passed:

```sh
npm run test:frontend
npm run typecheck
npm run lint
npm run build
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^(TestBackendVertical|TestBrowser(FixedResultRendering|SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The frontend suite passed all 104 tests. Controlled red tests observed 1,048
`Intl.DateTimeFormat` constructions for 1,000 event rows and 1,000
constructions for 1,000 server timeline buckets. The final deterministic
regressions require one formatter for 1,000 event rows, one formatter per
nonempty adaptation or response, zero for statistics/empty/all-invalid paths,
and one source-Date serialization per ordinary cell, flattened object child,
or nested-list timestamp. Timing observations were deliberately not made
release thresholds.

The final Docker-backed suite completed in 54.31 seconds. Its vertical
completed in 23.05 seconds, stored four distinct events with zero replay, and
passed all six current GradeThis searches. The fixed rendering fixture used a
424,238-byte 1,000-row by 64-column response, initially materialized 18 rows,
peaked at 25 materialized rows and 27 total DOM rows, and reported 61.9 ms
initial stable rendering on the checkpoint machine. Sequence expiration, all
three sequence-gap recovery variants, and cancellation passed. No
`open-splunk-*` test container remained.

Consumer-correctness, allocation/performance, and lifetime/test-design
reviewers covered authoritative and preview consumers, dedicated field and
timeline routes, backend export ownership, typed-value equivalence, nested
object/list handling, timezone changes, lazy allocation, repeated calls,
all-invalid inputs, asymptotic work, and the private module surface. Their
final review of staged SHA-256
`b1cd789777fe9336a33f4c6b5d856bb2befec8a3b30b8fe65200d21e73748253`
reported no remaining issue.

### Statistics-only result projections

The exact implementation at `e647dd2e5ae3b422ec98ee16b758d15fc87a4aa5`
passed:

```sh
npm run test:frontend
npm run typecheck
npm run lint
npm run build
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^(TestBackendVertical|TestBrowser(FixedResultRendering|SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The frontend suite passed all 97 tests. The controlled red for two ordinary
statistics rows observed seven `Intl.DateTimeFormat` constructions before the
result-kind specialization. The permanent accessor sentinel now ties the
regression to raw event decoding itself, so the formatter reuse completed at
`c20204b` cannot make the skipped-work proof vacuous.

The Docker-backed suite completed in 55.23 seconds. Its vertical completed in
23.72 seconds, stored four distinct events with zero replay, and passed all six
current GradeThis searches. The fixed rendering fixture used a 424,238-byte
1,000-row by 64-column response, initially materialized 18 rows, peaked at 25
materialized rows and 27 total DOM rows, and reported 65.7 ms initial stable
rendering on the checkpoint machine. Sequence-expiration, all three
sequence-gap recovery variants, and cancellation passed. No
`open-splunk-*` test container remained.

Correctness, efficiency, and maintainability reviewers covered every adapter
consumer, authoritative and live-preview paths, unsupported/future enum
values, call-site migration, retained event/time-series behavior, raw
sentinel reachability, asymptotic work, allocation lifetime, React state, and
test isolation. Their final review of staged SHA-256
`ce22fc3ec685b30864e215891bb9ac3ebe0e8d49ef2f179fd0ce483e4f16b609`
reported no remaining issue.

### Per-surface ordered configured-redaction replay

The exact implementation at `1b8939775efcf053d0d11ec870cf075dc5a22178`
passed:

```sh
go test ./... -count=1
go test -race ./internal/ingest ./internal/collector ./integration -count=1
go vet ./...
go build ./...
go test ./internal/ingest \
  -run 'Test(CompositeSupplementalRedactorDirectFieldDropsUnknownBytesLikeSequential|CompositeSupplementalRedactorDirectFieldReplaysFromMiddleMatch|TopLevelAliasRedactionDropsUnknownBytesFromSensitiveTypedField|CompositeSupplementalRedactorOrderedReplayMatchesSequentialAcrossSurfaces|CompositeSupplementalRedactorSyntaxSafeMissAllocationParity)$' \
  -count=50
go test ./internal/ingest -run '^$' \
  -bench 'BenchmarkCompositeSupplementalRedactorSyntaxMarker(DuplicateJSONSafeMiss|HitOnly)$' \
  -benchmem -benchtime=100ms -count=3
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=12m -v
git diff --cached --check
```

The complete `go test -race ./... -count=1` command also passed during final
hardening before the isolated unknown-byte change. The exact committed tree
then passed the affected race suites listed above. Its Docker vertical
completed in 24.33 seconds, stored four distinct events with zero replay,
passed all six current GradeThis searches, and left no `open-splunk-*` test
container running.

During final hardening, these three differential fuzz campaigns completed
1,228,298 executions without a saved reproducer:

```sh
go test ./internal/ingest -run '^$' \
  -fuzz '^FuzzCompositeSupplementalRedactorOrderedOnChangeMatchesSequentialPolicies$' \
  -fuzztime=30s
go test ./internal/ingest -run '^$' \
  -fuzz '^FuzzTopLevelAliasOrderedOnChangeMatchesSequentialTextGroups$' \
  -fuzztime=30s
go test ./internal/ingest -run '^$' \
  -fuzz '^FuzzCompositeSupplementalRedactorMatchesSequentialPolicies$' \
  -fuzztime=20s
```

The benchmark suite proved exact sequential output before timing safe misses,
duplicate-only canonicalization, tiny hit-only events at 2/8/32 policies,
sparse hits with large safe sibling surfaces, and a 1-MiB typed value matched
only by the final policy. Measurements are checkpoint-machine observations;
allocation regressions enforce relative one-pass parity without encoding
timing thresholds.

Three final reviewers independently recomputed staged SHA-256
`023c792c59b45711bf5cd01c0019ad49f7d203ca874fcbf6587206a8c94abf9a`.
Their final pass covered confidentiality/fail-close behavior, protobuf unknown
wire data, exact policy order, per-surface isolation, duplicate JSON,
concurrent immutable reuse, bounded scanning, allocation behavior, benchmark
honesty, golden-oracle independence, and fuzz dimensions. All reported the
frozen patch clean.

### Bounded integration/browser harness resources

The exact implementation at `3f8922972ab5258a0f0658c714b5ba36971dcf71`
passed:

```sh
go test ./... -count=1
go test -race ./integration -count=1
go vet ./...
go build ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=12m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
go test ./integration \
  -run '^TestBrowser(SequenceGapRecovery|SequenceGapRESTFirstProgressRecovery|SearchCancellation)$' \
  -count=1 -timeout=12m -v
git diff --cached --check
```

The frontend suite passed 93 tests. The Docker-backed vertical completed in
23.82 seconds, stored four distinct events with zero replay, and passed all six
current GradeThis searches. The affected real-browser sequence-gap,
REST-first-progress, and cancellation cases completed in 14.29, 4.99, and
1.77 seconds respectively. Focused harness race tests also passed ten
consecutive runs while the implementation was being hardened.

Adversarial review explicitly covered exact process-output boundaries,
concurrent stdout/stderr writes, truncation evidence, missing commands,
environment replacement, Unicode byte limits, large untrusted inputs, error
object retention, post-overflow safety latches, page-realm parity, DOM
materialization, socket ownership and cleanup, excess connection attempts,
Blob conversion races, and frame-slot accounting. All concrete findings were
fixed before the staged patch was frozen and independently re-reviewed.

### Composite configured pre-WAL redaction

The exact implementation at `34f3a9b291ff7ea327869cf4e635f5c496f13563`
passed:

```sh
go test ./... -count=1
go test -race ./internal/ingest ./internal/collector ./integration -count=1
go vet ./...
go build ./...
go test ./internal/ingest -run '^$' \
  -fuzz '^FuzzCompositeSupplementalRedactorMatchesSequentialPolicies$' \
  -fuzztime=20s -parallel=8
go test ./internal/ingest -run '^$' \
  -fuzz '^FuzzTopLevelAliasCompositeMatchesSequentialTextGroups$' \
  -fuzztime=20s -parallel=8
go test ./internal/ingest -run '^$' \
  -bench '^(BenchmarkCompositeSupplementalRedactor|BenchmarkTopLevelAliasRedaction)$' \
  -benchtime=50ms -count=1
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' -count=1 -timeout=15m -v
git diff --cached --check
```

The final supplemental and alias fuzz campaigns completed 133,454 and 408,956
executions respectively. Earlier adversarial campaigns also ran longer
supplemental and specialized-field variants; every promoted counterexample is
now a deterministic named regression or seed.

The Docker-backed vertical stored exactly four distinct events, replayed none,
passed all six current GradeThis searches, and proved exact
`[CREDENTIAL-MASKED]` and `[PIN-MASKED]` markers across public search and
export results. No configured sentinel survived the collector WAL, server
logs, ClickHouse result path, WebSocket frames, HTTP protobuf, or downloaded
JSONL artifact. No `open-splunk-*` container remained afterward.

The benchmark suite verifies output equivalence before timing. Safe text and
valid JSON scale independently of policy count on the composite path. Free
text that could change the interpretation of a later policy intentionally
replays the ordered chain. The final early-replay optimization stops before
building a disposable composite output, leaving 8- and 32-policy all-hit
4-KiB cases at approximately legacy latency with lower allocation volume.
Reverse text/policy order has a separate fixture because it can scan farther
before discovering that compatibility replay is required. These measurements
are observational, not release acceptance thresholds.

Independent review covered confidentiality, exact sequential equivalence,
generated-marker interactions, JSON and invalid UTF-8 boundaries, alias
semantics, concurrency, construction/runtime allocation, benchmark honesty,
reuse, and maintainability. All concrete findings were fixed or, for the
inherently ordered match-heavy text path, explicitly benchmarked and
documented.

### High-source-count collector polling

The exact implementation at `f41720e0f868354fafd535022b445b12ddaff99b`
passed:

```sh
go test ./internal/collector/input \
  -run '^(TestNewManagerRejectsMultilineWithoutLineStartPattern|TestTailerPollTimerRearmsWithoutAllocating|TestTailerPollTimerDropsConsumedTickAndHonorsCancellation|TestTailerGuardFingerprintReusesScratchWithoutAllocating|TestTailerSameSizeCopyTruncateResetsInactivityClock|TestTailerDrainReframesAfterAppendRacesWithEOFStat|TestManagerAppendWhileTailing|TestManagerMatchPathsSortsAndDeduplicatesIncludeGlobs)$' \
  -count=20
go test ./internal/collector/input -run '^TestManagerStartAtEnd$' -count=100
go test ./internal/collector/input -count=20
go test -race ./internal/collector/input -count=1
go test ./internal/collector/input -run '^$' \
  -bench '^(BenchmarkTailerPollTimer|BenchmarkTailerGuardFingerprint|BenchmarkTailerCleanEOFTracking|BenchmarkMatchPathsHighSourceCount)$' \
  -benchmem -count=3
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' -count=1 -timeout=15m -v
OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' -count=1 -timeout=15m -v
git diff --cached --check
```

The isolated reusable timer and guard-fingerprint benchmarks both reported
zero bytes and zero allocations per operation. Clean-EOF tracking reported
208 bytes and one allocation per poll, attributable to `File.Stat`.
Multi-glob matching over 1,000 sources took approximately 0.64–0.84 ms in the
committed benchmark. In the direct 10,000-path sort comparison, the previous
insertion sort took approximately 49.9–50.6 ms and `slices.Sort` took
approximately 0.518–0.526 ms.

The 1,000-empty-file profile reduced the baseline 2,054,808 mallocs and
760,255,248 allocated bytes to 1,357,072 mallocs and 149,706,432 bytes. A
1-KiB-per-file `start_at=end` profile measured 1,356,972 mallocs and
149,485,312 bytes, confirming that unchanged nonempty sources do not recreate
the old per-poll guard/framer costs.

The Docker-backed vertical stored exactly four distinct events and replayed
none, and all six current GradeThis SPL searches passed. The sustained-load
proof generated and stored exactly 30,000 events, including exact event,
request, and timestamp cardinalities; recovered a 5,700-event durable backlog
after a 6.17-second outage; and ran two eight-search concurrent cohorts. It
completed in 47.11 seconds on the checkpoint machine. Throughput, recovery
latency, query timing, and concurrency measurements are observational
checkpoint-machine evidence, not release acceptance thresholds.

Three independent reviewers verified the exact staged binary diff hash
`944048bc19bb28af667fd59dcce49f6c73cebcd2b6ee9eb2eea31d27d9f3d7a3`
and reported no remaining correctness, data-loss, race, timer-lifecycle,
allocation, performance, efficiency, determinism, reuse, or code-quality
finding.

### Bounded browser statistics rendering

The exact implementation at `9d6acc11f2626f92d5ddd2b4e608a1268cc0c9e3`
passed:

```sh
npm run test:frontend
npm run lint
npm run typecheck
npm run build -- --webpack
go test ./...
go test -race ./...
go vet ./...
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBrowser' -count=1 -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
git diff --cached --check
```

All 89 frontend tests passed. The production Next.js build compiled and
generated all 11 pages. The complete six-case browser matrix, including fixed
rendering, sequence expiration/gap recovery, REST terminal/first-progress
recovery, and cancellation, passed in 37.512 seconds. The final Docker-backed
vertical passed in 21.01 seconds and all six current GradeThis SPL searches
returned the expected results.

The final 1,000-by-64 response was 424,238 bytes. Initial stabilization took
74.4 ms and bottom stabilization 174.6 ms on Chromium 151.0.7922.34. The
initial table held 18 materialized rows, one spacer, 19 total body rows, and
1,216 materialized cells; interaction-wide mutation peaks were 25
materialized rows and 27 total body rows. The table was 10,240 pixels wide.
These timings are observational checkpoint-machine measurements, not
performance acceptance thresholds.

Screenshots of the top, bottom, and compact states were inspected and showed
distinct sentinel rows, a stable header, bounded table geometry, and no page
overflow. Three independent reviewers verified exact staged patch SHA-256
`3af2a8373ff9900d862415851c84ccbd35b69462bda8325c939054c99792a732`
and reported no remaining correctness, measurement-integrity, performance,
efficiency, process-lifecycle, determinism, reuse, or code-quality finding.

### Bounded WebSocket consumers and replay recovery

The exact implementation at `4c4003f` passed:

```sh
go test ./internal/searchws ./internal/server \
  -run '^Test(PreviewQueuePressureSkipsSubscriptionSpecificCopy|DisposablePreviewPressureClosesAndRetainsReplay|PreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|InitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|TerminalPreviewInvalidationRemainsAuthoritativeUnderPressure|BoundedInitialFrameStagingOwnsGlobalQueueReservation|SearchWebSocketSlowConsumerIsBoundedAndRecoversWithoutBlockingSearch)$' \
  -count=20
go test -race ./internal/searchws ./internal/server \
  -run '^Test(PreviewQueuePressureSkipsSubscriptionSpecificCopy|DisposablePreviewPressureClosesAndRetainsReplay|PreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|InitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|TerminalPreviewInvalidationRemainsAuthoritativeUnderPressure|BoundedInitialFrameStagingOwnsGlobalQueueReservation|SearchWebSocketSlowConsumerIsBoundedAndRecoversWithoutBlockingSearch)$' \
  -count=5
go test -race ./internal/searchws \
  -run '^TestInitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget$' \
  -count=100
go test ./... -count=1
go test -race ./internal/searchws ./internal/server -count=1
go vet ./...
go build ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --cached --check
```

The frontend suite passed all 80 tests. The production Next.js build compiled,
typechecked, and generated all 11 static pages. The focused preview-pressure,
initial-staging, and gated-writer cases passed 20 ordinary repetitions and
five race-enabled repetitions across `internal/searchws` and
`internal/server`; the deterministic initial subscribe/replay boundary also
passed 100 race-enabled repetitions. Final reviewers independently reran the
frontend suite and up to three complete race-enabled repetitions of both
affected Go packages.

The controlled server red failed at the intended boundary: the old initial
subscribe/replay path hard-closed the connection with zero queued bytes when
the exact final 342-byte batch fit. The fixed path queues that batch, preserves
the unrelated reservation exactly, emits a replay acknowledgment plus the
correct truncated one-row preview, and releases its tailoring permit. The
real-server fixture separately proves one blocked in-flight frame plus six
queued frames remain open and the eighth new frame triggers isolated closure.

Three independent reviewers verified the exact staged binary diff hash
`887f03b62962c1aacb45b992f0481b1a7c5f04b6cc408f1d81fa5dc648c9670c`.
They traced queue and in-flight ownership, tailoring permits, cancellation and
lock ordering, browser frame accounting and backing-buffer ownership,
amortized compaction, stale-generation fencing, sequence/replay behavior, and
the integration fixture's transport determinism. All three reported the
frozen patch clean. This slice launched no Docker fixture.

### Concurrent SPL searches during live recovery

The exact implementation at `9898b41` passed:

```sh
go test ./integration \
  -run '^(TestBackendLoadConcurrentSearchValidationRejectsInconsistentSnapshots|TestMaximumBackendLoadSearchActiveOverlapUsesHalfOpenIntervals|TestValidateBackendLoadCohortVisibilityRejectsAnyRegression|TestBackendLoadPlanPinsSustainedOutageWindow)$' \
  -count=100
go test -race ./integration \
  -run '^(TestBackendLoadConcurrentSearchValidationRejectsInconsistentSnapshots|TestMaximumBackendLoadSearchActiveOverlapUsesHalfOpenIntervals|TestValidateBackendLoadCohortVisibilityRejectsAnyRegression|TestBackendLoadPlanPinsSustainedOutageWindow)$' \
  -count=20
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./integration -count=1
OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' -count=1 -v
git diff --cached --check
```

The exact final Docker run completed in 46.14 seconds and the package in
46.527 seconds. It generated all 30,000 events and 12,531,099 source bytes in
30.253 seconds, or 991.6 generated events/second and 414,209.2 bytes/second.
The accepted window was 31.028 seconds, or 966.9 events/second. Its backend
outage lasted 6.170 seconds, the durable backlog health gate covered 6,000
events, recovery first advanced 264.803 ms after health, and post-generation
drain took 340.382 ms.

The concurrent recovery window ran two eight-job cohorts. While all processes
remained alive, source records advanced 11,200 to 11,400, physical rows 6,500
to 7,700, and every public result in the second cohort retained the first
cohort's maximum prefix while the exact public prefix advanced 6,500 to
7,100. The 16 jobs scanned 108,800 rows and 61,881,298 bytes. Their client
wall was 208.172 ms, lifecycle span 202.901 ms, and observational maximum
active overlap four. Lifecycle min/median/p95/max was
20.653/48.652/103.556/103.556 ms; elapsed
min/median/p95/max was 18.624/19.893/38.340/38.340 ms; maximum queue wait was
84.823 ms.

Final convergence still required exactly 30,000 physical and distinct rows,
exact source/storage/raw-field equality, one terminal checkpoint, an empty
acknowledged WAL, no quarantines, and an empty owner-only dead-letter file.
ClickHouse reported three active parts, 7,303,879 compressed bytes,
34,495,468 uncompressed bytes, and 11,501,962 bytes on disk. Final public SPL
search lifecycle measurements were 41.655 ms and 35.116 ms.

Controlled reds and successive adversarial reviews found and fixed an
insufficient prefix validator, unsafe physical-row lower bounds, zero-delta
progress, mixed client/server clocks, timing-dependent overlap requirements,
cross-dataset ratios, a hidden three-second SLA, retry amplification, weak
cohort-wide monotonicity, and a polling timer that could remain live during
queries. Three independent final reviewers recomputed frozen staged patch
SHA-256
`71d19acf2990b12db60e53cc50ecb0dc6c2abec063ceb5cdf69c7fb8845bc4fb`
and reported it clean. The implementation is committed and pushed on `main`
at `9898b41`.

### Sustained-load outage and restart correctness

The implementation/proof checkpoint passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./internal/collector/... ./integration -count=1
go test ./internal/collector ./internal/collector/wal ./integration \
  ./cmd/open-splunk-collector \
  -run '^(TestDaemonRestartDoesNotRequeuePendingWALSourcePrefix|TestCheckpointResumeView.*|TestPrepareAckCachesAndCoalescesSourceMarksAcrossRecovery|TestRecoveryQuarantinesEverySegmentAfterCorruptGap|TestSourceCheckpointsFromWALRejectsCursorConflictWithDurableCheckpoint|TestSourceCheckpointsFromWALRejectsConflictingPendingIdentities|TestBackendLoad.*|TestManagedProcess.*|TestLockedBuffer.*|TestParseLogLevel)$' \
  -count=20
go test -race ./internal/collector ./internal/collector/wal ./integration \
  ./cmd/open-splunk-collector \
  -run '^(TestDaemonRestartDoesNotRequeuePendingWALSourcePrefix|TestCheckpointResumeView.*|TestPrepareAckCachesAndCoalescesSourceMarksAcrossRecovery|TestRecoveryQuarantinesEverySegmentAfterCorruptGap|TestSourceCheckpointsFromWALRejectsCursorConflictWithDurableCheckpoint|TestSourceCheckpointsFromWALRejectsConflictingPendingIdentities|TestBackendLoad.*|TestManagedProcess.*|TestLockedBuffer.*|TestParseLogLevel)$' \
  -count=20
OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' \
  -count=1 -timeout=12m -v
git diff --cached --check
```

The exact frozen implementation passed the complete ordinary suite, build,
vet, collector/integration race suite, and both ordinary and race-enabled
20-repetition focused regression sets. An earlier candidate also passed a
separate 20-repetition full integration/collector/WAL stress run; its WAL
package completed in 294.421 seconds. Its command was
`go test ./integration ./internal/collector ./internal/collector/wal -count=20`.
Later adversarial review added the resume-prune production linearization,
direct durable-depth test synchronization, and reviewer cleanup. Every
affected path then passed the focused ordinary and race sets 20 times, plus
the complete ordinary and affected-package race suites. The final real Docker
run completed in 49.53 seconds, cold-recovered 5,700 durable events from the
6,000-event offline source phase, and finished with exactly 30,000 physical
and distinct stored events.

The shared 100,000-batch acknowledgment aggregation benchmarks reported:

```text
compact identity: 4.85 ms/op, 2,403,077 B/op, 7 allocs/op
100,000 identities: 27.86 ms/op, 37,681,632 B/op, 537 allocs/op
```

These are local checkpoint observations. Pending-mark startup aggregation
reuses the same validated merge logic but aggregates directly under the queue
lock, avoiding the 100,000 transient group headers; its production caller runs
before sender and input goroutines start.

Controlled reds first proved that source progress alone did not guarantee the
requested WAL depth, then exposed the production duplicate replay as
35,700/29,700 physical/distinct rows and as seven instead of four queued events
in a deterministic restart test. Neither assertion was weakened. Adversarial
review added legacy-cursor write suppression, durable-equal preference,
pending-vs-durable cursor validation, corrupt-gap coverage, acknowledged
overlay pruning, a manager-specific checkpoint interface, compile-time WAL
resume capability, immutable raw expectations, lower-cost polling, direct
durable-WAL-depth synchronization, a health-gated recovery generation phase,
and atomic lookup versus terminal pruning.

The final frozen staged patch SHA-256 is
`55f1eb83d0dd12be3660a41860ce30cd56a26f6055679dd0abda5a3fd3d08a64`.
Three independent reviewers recomputed that exact hash and reported the patch
clean for crash/restart correctness, WAL/checkpoint consistency, load-test
determinism, performance, and code quality. The implementation is committed
and pushed on `main` at `59b8f7c`.

### Load-generator pacing and live-output foundation

The exact implementation at `860acac` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test ./internal/loggen ./cmd/open-splunk-loggen -count=50
go test -race ./internal/loggen ./cmd/open-splunk-loggen -count=10
git diff --cached --check
```

The focused suites passed 50 ordinary repetitions and 10 race-enabled
repetitions after the final reviewer-requested assertion. Independent
reviewers additionally ran the focused suites 100 times and the race-enabled
suites 20 times, plus vet and diff checks.

A compiler-free scheduling observation used the built binary:

```sh
/usr/bin/time -p /private/tmp/open-splunk-loggen-rate \
  -count=10000 -format=cardinality-json \
  -rate=1000 -interval=1ms -flush-events=100 -output=- >/dev/null
```

It emitted 10,000 high-cardinality events in 10.00 seconds on the checkpoint
machine. This is a reproducibility observation, not a cross-hardware
acceptance threshold. Before the absolute scheduler, the same cardinality
workload had taken 11.88 seconds (about 842 events/second) because per-event
generation and write time accumulated on top of every relative sleep.

The exact staged patch had SHA-256
`94c9ad171e414118cfab3bcd337eceb4eae298059e6aeeabdb9087de53339b1f`.
The final timing reviewer found deadline arithmetic, overflow guards, bounded
rebasing, cancellation priority, and timer reuse sound. The I/O reviewer
found all Flush/Sync/Close, append, pre-cancellation, stdout, and error-tree
paths clean under the documented single-writer file contract. The test and
performance reviewer found the generator suitable for the next 1,000 EPS
live-tail harness when it explicitly uses `-interval=1ms` and a finite
`-flush-events` value such as 100.

### Shutdown-safe export artifact removal

The exact implementation at `961cba2` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./internal/export ./internal/server -count=1
go test ./internal/export \
  -run 'TestDownloadLeaseDeferredUnlinkIsAdmittedBeforeManagerClose|TestManager(Cleanup|Get)DeletionDoesNotBlockShutdownAdmission' \
  -count=100
go test -race ./internal/export \
  -run 'TestDownloadLeaseDeferredUnlinkIsAdmittedBeforeManagerClose|TestManager(Cleanup|Get)DeletionDoesNotBlockShutdownAdmission' \
  -count=20
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=1
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The full ordinary Go suite, build, vet, complete export/server race suites, and
the exact real-manager expiration lifecycle all passed. The three new
shutdown/removal cases passed 100 ordinary repetitions in 7.094 seconds and
20 race-enabled repetitions in 3.122 seconds.

The six-case Docker/ClickHouse plus browser release gate completed in
52.492 seconds: `TestBackendVertical` in 24.28 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.80 seconds,
`TestBrowserSequenceGapRecovery` in 5.18 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.14 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.28 seconds, and
`TestBrowserSearchCancellation` in 1.31 seconds. All six current GradeThis
investigations passed inside the vertical. Read-only Docker checks before and
after the gate showed no `open-splunk-*` test container.

The frozen staged implementation patch had SHA-256
`8a63a57ce80fa863be19a296a66c1f1159edd1bd185a0afe0241e1ed8a8bc03c`.
Independent final reviewers confirmed that checksum, traced every artifact
remover and shutdown interleaving, and reported no remaining finding.

### Sanitized current GradeThis Open Splunk path proof

The exact implementation at `c576e85` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race \
  ./internal/collector ./internal/ingest \
  ./internal/testsupport/gradethiscorpus -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
  -count=1 -timeout=10m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The final full Go suite completed in 18.594 seconds. The race-enabled collector,
ingestion, and migration-corpus packages passed, with the longest package at
39.558 seconds. The exact ten-search v0.1 ClickHouse corpus completed in
5.99 seconds.

The final six-case Docker/ClickHouse plus browser release gate completed in
50.946 seconds: `TestBackendVertical` in 22.63 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.78 seconds,
`TestBrowserSequenceGapRecovery` in 5.21 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.19 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.37 seconds, and
`TestBrowserSearchCancellation` in 1.36 seconds. All six current GradeThis
investigations passed inside the vertical. A read-only Docker check showed no
remaining `open-splunk-*` test container.

The final frozen staged patch had SHA-256
`c863964ed3a674b870dbe7861a1cd794b423ca1009422dd2df7200af87109ba3`.
Adversarial reviewers independently verified that exact checksum. Their
findings drove shared processor-derived lineage, a unified stored-event poller,
manifest-owned expected row counts, the no-source lineage fast path, removal
of redundant grouping state, and duplicate-key alias canonicalization. Final
correctness/SPL, confidentiality/performance, and reuse/quality rereviews were
clean. The only retained nonblocking performance backlog is the
differential-tested composite sanitizer for uncommon configurations with
multiple distinct custom replacement markers; the shipped profile uses one
direct pass.

### Logical event retention before physical TTL cleanup

The exact implementation at `458c8b4` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race \
  ./internal/clickhouse ./internal/queryexec ./internal/export \
  ./internal/searchjobs ./internal/control ./internal/server \
  ./cmd/open-splunk-server -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' \
  -count=1 -timeout=6m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --check
```

The final pinned Store suite completed in 35.32 seconds and included the real
native-driver upper timestamp round trip plus an ambiguous version-3
reservation retry through storage and visibility completion. The pinned
executor/manager suite completed in 11.323 seconds. The exact six-case
Docker/ClickHouse plus browser release gate completed in 47.729 seconds. A
read-only Docker check showed no Open Splunk test container afterward.

The retention fixture is deliberately non-vacuous: all three rows remain
physically present, two have expired relative to the immutable artificial
cutoff, and only the just-after-boundary row is visible through every
read path. Controlled red/green evidence covered the common predicate, native
timestamp range, policy precision, metadata compatibility, and migration.
The final frozen staged diff had SHA-256
`579dfcce5a4f8c9c179fd8bc7e2bd1936d45af1eccf1605309ab3ef3d465f5b9`;
independent correctness, architecture/performance, and quality reviews
reported no remaining finding.

### Clock-driven expiration and tombstone cleanup

The proof at `b2b2839` passed:

```sh
go test ./... -count=1
go vet ./...
go test -race ./internal/export ./internal/searchws ./internal/server -count=1
go test ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=200
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=50
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The exact six-case Docker/ClickHouse plus
Playwright gate completed in 49.937 seconds:
`TestBackendVertical` in 22.04 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.71 seconds,
`TestBrowserSequenceGapRecovery` in 5.11 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.07 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.23 seconds, and
`TestBrowserSearchCancellation` in 1.25 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The final lifecycle fixture passed 200 consecutive ordinary runs and 50
consecutive race-enabled runs locally. Independent reviewers additionally ran
up to 100 ordinary repetitions, up to 60 semantic repetitions, and repeated
race-enabled affected-package suites. Controlled red/green evidence covered
artifact-terminal replay and late-cleanup tombstone retention. Reviewers then
found two false-pass assertions and two deterministic-clock races; each was
reproduced, fixed, stress-tested, and rereviewed on the final frozen staged
diff. Final correctness, architecture/determinism, and efficiency/leak
rereviews reported no remaining finding.

### Recovered-socket stale-duplicate fencing

The proof at `b80bf0a` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGapRecovery$' -count=1 -timeout=8m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The final focused real-Chrome gap proof completed
in 14.074 seconds overall, with the browser case itself taking 13.54 seconds.
The final race-enabled run completed in 15.185 seconds overall, with the
browser case taking 13.56 seconds. The exact six-case CI-equivalent gate
completed in 49.920 seconds:
`TestBackendVertical` in 20.66 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.02 seconds,
`TestBrowserSequenceGapRecovery` in 5.43 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.33 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.48 seconds, and
`TestBrowserSearchCancellation` in 1.54 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The controlled-mutation run disabled the production
`event.sequence <= previous` return and failed at the exact stale-preview
resurrection assertion. The fence was restored before every green gate and
before commit. Simplify and adversarial reviewers found and drove fixes for a
custom synthetic injection path, redundant receipt waits, broad unbounded
socket/DOM instrumentation, duplicated status recording, insufficient
sequence/revision proof, bounded correctness histories, missing unit
gap/error assertions, ambiguous timeout diagnostics, and transient
preview-status removal. Final frozen-diff reviews reported no remaining
correctness, performance, determinism, or code-quality finding.

### Authoritative browser cancellation

The cancellation proof at `787a7f9` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSearchCancellation$' -count=1 -timeout=10m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The final focused real-Chrome cancellation run
completed in 9.63 seconds. The race-enabled focused run completed in 11.360
seconds overall, with the browser case itself taking 9.74 seconds. The exact
six-case CI-equivalent gate completed in 49.246 seconds:
`TestBackendVertical` in 19.99 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.04 seconds,
`TestBrowserSequenceGapRecovery` in 5.33 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.37 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.47 seconds, and
`TestBrowserSearchCancellation` in 1.53 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The first real run exposed an ambiguous role locator because both the primary
button and empty-state action contained “Run search”; the proof now uses the
stable primary-button test ID. Adversarial reviews then drove independent
pre-server and pre-browser response gates, a second discrete cancel click,
virtual-time reconnect fencing, browser-route and server-side request counts,
an explicit canceled-executor-exit count, a shared WebSocket URL matcher, and
a guarded create-response waiter that cannot reject during teardown. Final
correctness, quality, efficiency, reuse, and fixture reviews reported no
remaining blocker.

### Versioned progress and coalesced recovery

The protocol slice at `b5502a3` passed the focused producer/protocol/server
tests, replay/cancellation coverage, race-enabled protocol packages, generated
binding checks, and the full Go suite. The browser slice at `522b0ac` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The exact five-case CI-equivalent run completed
in 48.854 seconds:
`TestBackendVertical` in 21.16 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.02 seconds,
`TestBrowserSequenceGapRecovery` in 5.34 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.32 seconds, and
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.48 seconds.

After the final test-observer cleanup, typecheck, lint, all 42 frontend tests,
and diff hygiene passed again; the focused REST-first Chrome/Docker case also
passed from a fresh staged build in 12.61 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

Three independent adversarial reviews covered revision semantics, terminal
phase/metric separation, recovery-cycle scheduling, request and render
efficiency, fixture causality, false-positive risk, and code reuse. The final
REST-first proof confirms browser receipt of `K+1` before permitting `K+2`,
then waits a timer task and two animation frames while `K+1` is isolated. The
observer records the exact scanned-row `<strong>` value, is disconnected on
both success and failure, and cannot let React batching hide a transient
regression. Final rereviews reported no blocker.

### REST-only terminal recovery after a sequence gap

The `d1286a4` companion first failed red after every causal barrier had
passed: the entire three-frame terminal WebSocket projection was captured but
withheld, authoritative GET2 returned the real completed job, and the decoded
result response was held before browser delivery, yet the zero-row banner
remained `resyncing` for the full 45-second assertion window. The completed
REST path now selects `finalizing` whenever the preview lifecycle is active,
including `resyncing` with no snapshot, while `disabled` and all
failed/canceled/expired paths retain their prior behavior. Repeated terminal
discovery is idempotent and does not repeat the finalizing announcement.

The exact committed state passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGap(RESTTerminal)?Recovery$' \
  -count=1 -timeout=12m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(ExpiredRecovery|Gap(RESTTerminal)?Recovery))$' \
  -count=1 -timeout=15m -v
git diff --check
```

All 32 frontend tests passed. Both gap fixtures passed under the race detector
in 27.854 seconds: the WebSocket-terminal case in 20.59 seconds and the
REST-only case in 5.60 seconds after the shared staged build. The final exact
CI-equivalent gate passed in 50.833 seconds:
`TestBackendVertical` in 21.83 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.08 seconds,
`TestBrowserSequenceGapRecovery` in 11.14 seconds, and
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.38 seconds.
A read-only Docker check confirmed no Open Splunk/ClickHouse test container
remained.

Design reviewers required withholding completed state and final progress in
addition to the terminal event so WebSocket could not leak completion. The
final simplify and adversarial passes also made finalizing idempotent,
preserved the exact upstream protobuf result bytes, consolidated duplicated
assertions, isolated inherited scenario flags, and found no remaining
correctness, determinism, performance, CI, or code-quality blocker. The older
valid-zero-row announcement wording edge remains deferred until it has its
own red test; it is not exercised by the post-gap path, which has no snapshot.

### Real browser sequence-gap recovery validation

The implementation at `f72f184` passed the full Go suite, all 32 frontend
tests, TypeScript, lint, the production build, the dedicated real
Chrome gap case, the gap case under the Go race detector, the combined
expiration/gap Chrome run, and the CI-equivalent Docker/ClickHouse plus both
Chrome recovery cases:

```sh
go test ./... -count=1
go test ./integration -count=1
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGapRecovery$' -count=1 -timeout=6m -v
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(Expired|Gap)Recovery)$' \
  -count=1 -timeout=15m -v
git diff --check
```

The exact final combined gate passed in 46.743 seconds:
`TestBackendVertical` in 22.70 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.18 seconds, and
`TestBrowserSequenceGapRecovery` in 11.22 seconds. A read-only Docker check
confirmed that no Open Splunk/ClickHouse test container remained. Three final
adversarial rereviews found no concrete issue after the causal GET2 barrier
and explicit close-completion promises were added.

The `ed28182` follow-up first made the no-post-gap-preview case red: the real
terminal arrived, but the banner remained `resyncing` for the full assertion
window. The completed-only status fix then passed the strengthened real Chrome
case, its race-enabled run, a three-run timing repetition, both recovery cases,
the full Go/frontend/build gates, and the exact CI-equivalent gate. The final
combined run completed in 47.198 seconds:
`TestBackendVertical` in 23.33 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.17 seconds, and
`TestBrowserSequenceGapRecovery` in 11.28 seconds. Three blocker-focused
rereviews found no remaining issue after completed-state gating, exact
zero-row visible and aria-live copy, immediate forbidden-append rejection, and
final status removal were pinned.

### Browser sequence-expiration recovery validation

The exact browser proof at `ad4a77b`, built on the production recovery
implementation at `8f6d569`, passed the complete ordinary suite,
affected-package race suite, all 32 frontend tests, static gates, production
build, the dedicated staged Chrome recovery case, and the combined real
Docker-backed ClickHouse/Chrome vertical:

```sh
go test ./... -count=1
go test -race ./integration ./internal/server ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^TestBrowserSequenceExpiredRecovery$' -count=1 -timeout=12m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequenceExpiredRecovery)$' \
  -count=1 -timeout=12m -v
git diff --check
```

The dedicated final case completed in approximately 21 seconds; the combined
gate completed in approximately 34 seconds. The staged real-browser case also
passed under `go test -race`, the focused harness race tests passed ten
repetitions, and the process-isolation review verified that no staged
repository or raw `.next`/`out` collision remained.

The recovery test proves exact `K` retention across an expired replay,
an actual transient HTTP 503, rejection of a stale versioned snapshot,
suspension while new live events arrive, queued-frame application only after a
successful three-row authoritative snapshot, rejection of an in-flight
snapshot captured before a later live update, and normal preview-to-final
operation on the recovered connection. Final focused semantics,
fixture-correctness, and false-positive reviewers found no remaining concrete
issue after the upstream-snapshot capture order was hardened.

The preceding real-manager replay/expiration and offline-cancellation tests
had already passed 30 consecutive ordinary runs and five race-enabled
repetitions:

```sh
go test ./internal/server \
  -run '^TestSearchWebSocketRealManager(ReplaysOneEventThenExpiresSequence|ReplaysCancellationAfterOfflineSocket)$' \
  -count=30
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManager(ReplaysOneEventThenExpiresSequence|ReplaysCancellationAfterOfflineSocket)$' \
  -count=5
```

### Resumable WebSocket checkpoint validation

The final implementation passed the full ordinary Go suite, the WebSocket
package under the race detector, all 29 frontend protocol tests, TypeScript
type checking, lint, the production frontend build, and patch cleanliness:

```sh
go test ./... -count=1
go test -race ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

The final focused backend regressions were:

```sh
go test ./internal/searchws \
  -run '^(TestCompletedTargetRemovedAtFirstExpiryPollRetiresTarget|TestWebSocketSubscriptionIdentityLimitRequiresReconnect)$' \
  -count=1
```

The first-expiry and identifier-exhaustion tests also passed 20 repeated runs
under the focused adversarial review. A full repository race run had passed
before the last two small boundary fixes; the final changed Go package then
passed its complete race suite. No real-browser disruption claim was made by
that earlier checkpoint; it was subsequently completed by the real recovery
checkpoint above.

### Crash-restart checkpoint validation

The real collector/server restart vertical passed twice on the final restart
implementation:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The runs completed in approximately 24 and 17 seconds. Each hard-killed and
restarted both processes while reusing the same durable state. The permitted
checkpoint-before-local-WAL-ack boundary produced five physical ClickHouse
rows: four distinct stable event IDs and one replay of only the final
sentinel. `dedup event_id` returned the required four logical rows through the
query API and browser. The final clean shutdown reached the exact EOF
checkpoint and drained the collector WAL.

The ordinary, generated-code, race, static, and frontend gates passed on the
same implementation:

```sh
go test ./... -count=1 -timeout=5m
go test -race ./internal/collector/... -count=1 -timeout=5m
go vet ./...
go build ./...
make proto
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

Focused framing/input tests were rerun after the final formatting pass.
Independent final correctness and performance rereviews reported no remaining
checkpoint blocker. The performance review confirmed constant-memory,
cancellation-aware oversized-record discard, O(1) legacy checkpoint opening,
an 80-byte compact source mark, and no regression in large acknowledgment
aggregation. A pre-existing `time.After` allocation on each tailer poll remains
a later high-source-count profiling improvement; it is not on the crash path.
No ephemeral `open-splunk-*` container remained after the real verticals.

### Exact GradeThis corpus validation

The exact GradeThis corpus passed against the production-pinned ClickHouse
image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
  -count=1 -timeout=6m -v
```

The full existing query-executor integration suite also passed after its
compiler/executor smoke switched to the shared manifest:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' \
  -count=1 -timeout=6m -v
```

The exact test starts one container, applies production migrations, decodes
and stores one 20-event batch, runs the ten sequential jobs, and cleans up in
about seven seconds locally. The broader executor suite completed in about
twelve seconds. Do not count the ordinary environment-gated skip as database
validation.

The complete release-path test passed twice after adversarial hardening. The
final run completed in 19 seconds:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The browser override is optional when Playwright's pinned Chromium is
installed. The test itself builds the backend-mode frontend and both Go
binaries, starts pinned ClickHouse, runs the collector/server/browser flow,
and removes its ephemeral container and processes.

The ordinary backend and frontend gates passed against the final code:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
```

The GitHub Actions YAML parsed successfully, `git diff --check` was clean, and
the Docker inventory contained no `open-splunk-*` test container.

The latest corpus-specific review passes reported no remaining correctness,
security, performance, reuse, or code-quality checkpoint blocker. The focused
review also ran the affected packages with the race detector. The older
collector-to-browser re-audit likewise reported no remaining blocker after its
generated-code, exact-job/progress, UI-transition, promise-lifecycle,
external-resource, bounded-output, and cleanup fixes.

### Previous scalar-extrema validation evidence

The full ordinary backend suite passed after commit `90f7bbe`:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
```

The frontend/static-application gates passed:

```sh
npm run lint
npm run typecheck
npm run test:frontend
npm run build
```

The full Store integration suite passed against the production-pinned image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
```

The extrema coverage in that suite includes:

- numeric-only, lexical-only, mixed, punctuation, zero, null, missing, and
  multivalue groups;
- whitespace, `NaN`, overflowing exponents, empty String, invalid UTF-8, and
  overlong lexical fallback behavior;
- nested/container and flattened-object guards;
- incomplete `BY` eligibility, projected-away input, global/grouped empties,
  and tenant-scope poison isolation;
- downstream `bin`, re-aggregation, native Bool, and distinct nanosecond
  timestamps; and
- `EXPLAIN actions=1` proof of shared aggregate states with no `ArrayJoin`.

The full query executor and manager suite also passed against the pinned
ClickHouse image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
```

This pins parser-to-manager nullable `Mixed` transport, including numeric,
String, null, and binary `_raw` extrema results.

The opt-in lowering benchmark passed against ClickHouse `26.3.17.4` from
`clickhouse/clickhouse-server:26.3.17.4`. The resolved multi-platform OCI index
digest was
`sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
With 2,000,000 generated rows, seven measured queries, `max_threads=1`, and
`use_query_cache=0`, it reported:

| Lowering | Client time/op | Server median | Average memory | Peak memory | Rows/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Guarded Array | 360.2 ms | 359 ms | 8,320,064 B | 9,081,959 B | 5,571,031 |
| Scalar tuple | 333.3 ms | 331 ms | 6,860,420 B | 6,860,964 B | 6,042,296 |

For this corpus, the scalar helper was about 7.8% lower in server median,
17.5% lower in average memory, and 8.5% higher in rows/second. These are
checkpoint observations, not timing assertions or universal workload claims.
Reproduce them with:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
```

The focused post-review compiler/depth tests and `git diff --check` also
passed. No `open-splunk-*` test container remained at the checkpoint. Do not
count a skipped opt-in test as database validation.

## Current backend state

The backend now includes:

- durable collector queue/checkpoint and ingestion acknowledgement coupling;
- composite configured pre-WAL redaction with exact per-field markers,
  sequential-compatibility fallbacks, alias lineage coverage, and deterministic
  offline-WAL plus real ClickHouse/public-SPL proof;
- restart-safe ephemeral input resume from intact pending WAL source
  coordinates, without advancing the terminal checkpoint;
- a deterministic load generator with absolute target-rate pacing, bounded
  catch-up, live periodic flushing, safe append boundaries, and durable
  cancellation finalization;
- an opt-in 30,000-event real server/collector/ClickHouse load proof with
  server outage, collector crash/restart, exact raw/event-ID checks, public SPL
  searches, and storage/recovery observations;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- logically authoritative per-index event retention at one immutable search
  cutoff, independent of asynchronous physical TTL cleanup;
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with job-state revisions, replay,
  explicit resynchronization acknowledgement, coalesced REST recovery, bounded
  single-use subscription identities, expiration retirement, bounded
  server-side slow-consumer eviction, independently bounded preview-tailoring
  work, client-wide inbound frame/byte bounds, replay-on-pressure, serialized
  cross-generation listeners, and reconnect backoff;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with row `count`, exact `count(field)`, `dc`/`distinct_count`,
  bounded ordered `list`, `values`, typed `min`/`max`, `sum`, `avg`, and
  `p95`, plus deterministic bounded `earliest`/`latest`;
- `top`, `rare`, `timechart`, and bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and bounded
  explicit-path JSON `spath`;
- resource limits for rows, bytes, time, memory, commands, generated SQL,
  relational depth, extraction work, exact distinct state, and multivalue
  publication; and
- materialized-CTE single-scan lowering for runtime-wide and
  analyzer-sensitive paths; and
- a fixed-revision cumulative Go lint ratchet from `327a162`, plus a clean
  unrestricted repository-wide lint run with zero findings at `caadf3f`.

Event retention is evaluated against each search job's immutable
`IndexTimeCutoff`. An event with `expires_at` equal to that cutoff is not
visible; an event one stored millisecond later is visible. Physical
ClickHouse TTL cleanup remains asynchronous best-effort physical reclamation,
and direct ClickHouse reads outside compiled Open Splunk query paths may still
expose expired physical rows. New persisted policies are nonnegative whole
milliseconds, with zero meaning the deployment default; resolved retention is
positive.
Legacy version-3 reservation metadata may retain historical sub-millisecond
intent solely to finish or deduplicate an already durable batch; version 4
and all new control-plane writes use the strict contract.

## GradeThis v0.1 corpus completion

All ten exact searches in `docs/product-architecture-plan.md` are implemented
and share one `internal/testsupport/gradethiscorpus` manifest.
`internal/clickhouse/compatibility_corpus_test.go` sends that manifest through
parse, plan, and compile in the ordinary suite. The existing broad executor
smoke also consumes the manifest while keeping its unrelated
`distinct_count(logger)` extension clearly separate from the named ten.

`internal/queryexec/gradethis_compatibility_v0_1_integration_test.go` is the
authoritative exact acceptance test. It starts one pinned ephemeral ClickHouse
server, decodes the generated NDJSON with the collector, stores one batch
through the production Store and visibility ledger, and executes the ten jobs
sequentially through owner/tenant-scoped signed three-row pages. It checks the
terminal job schema and row count, every page's complete schema and stable
total, global row ordinals, every typed cell, cursor progress, and terminal
cursor exhaustion. Wrong-owner and wrong-tenant requests return not found, and
a modified cursor is rejected.

The committed deterministic synthetic profile has a fixed UTC base and a
pinned byte hash. Its 20 events use unique aggregate counts to avoid ambiguous
order:

- 11 `INFO`, 6 `ERROR`, and 3 `WARN` events;
- ten exact `Request metrics` events: seven
  `/api/v1/assessments` requests at `800ms` and three
  `/api/v1/submissions` requests at `300ms`;
- assessment statuses `200` four times and `503` three times; submission
  statuses `200` twice and `500` once;
- four `Heartbeat`, three `Dependency retry scheduled`, two
  `Database request failed`, and one `Request started` message; and
- one two-event synthetic trace, with only its database error containing
  `connection refused`.

This produces deterministic high-level results: severity counts
`INFO=11, ERROR=6, WARN=3`; response counts `assessments/200=4`,
`assessments/503=3`, `submissions/200=2`, `submissions/500=1`; the slow-route
result `assessments, count=7, p95_ms=800`; and top-message percentages
`50%, 20%, 15%, 10%, 5%`. NDJSON emission and decoded expected data derive
from that one profile rather than a hand-written ClickHouse fixture. A
100-year test retention keeps fixed historical rows ahead of ClickHouse TTL
without approaching `time.Duration` overflow.

The fixture contains only synthetic IDs, relative callers, TEST-NET IPs, and
reserved example data. Its ordinary scanner rejects secret-like keys,
credential-like values, email/user/session identifiers, SQL, stack traces,
workstation paths, production URLs, and non-documentation IPv4/IPv6
addresses. It also rejects duplicate JSON keys and does not echo rejected key
text in diagnostics. The local root `app.log` is large, ignored, and contains
user IDs, network metadata, SQL, stack traces, absolute paths, and secret-like
fields; never copy or commit it.

Raw-event result rows preserve sparse dynamic-field presence. The compiler
adds an internal names column only when the public raw `fields` payload is
present; the executor validates it, removes ClickHouse part-level phantom
nulls, and synthesizes explicit null only for a path named as present. Shared
canonical path parsing prevents literal-dot/backslash/percent collisions. One
fixture event carries `optional_note:null`, while neighboring events omit it,
and exact ordered-row assertions prove those two states remain distinct.

The aggregate normalized-name budget is an independent 1 MiB per durable
event. The validator applies it to the post-redaction clone with an
allocation-free early exit using the shared encoder's byte accounting, the
Store applies it defensively before writing, and result conversion applies it
before constructing a public object. Tests cover prefix-amplification within
the 1 MiB protobuf envelope and prove that a sensitive amplified subtree which
redaction collapses is still accepted.

Two compatibility facts must remain explicit:

- current local GradeThis request logs use `Request summary statistics`,
  whereas the product plan and exact v0.1 corpus use `Request metrics`; and
- real durations include microseconds, milliseconds, and seconds, whereas the
  plan's literal `replace(duration, "ms$", "")` query covers milliseconds
  only.

The corpus pins documented Open Splunk v0.1 behavior, including the current
approximate `p95`; it does not claim live Splunk oracle parity.

The frontend adapter regression uses a real generated-protobuf-shaped
`message,count,percent` result and now classifies `percent` as a measure, so
`top message` retains its statistics table and gains the intended categorical
visualization. A later release-path slice should reuse the manifest through
the protobuf server and one browser session rather than launching ten
independent stacks.

## Resume checklist

1. Work only from `main`; fast-forward it from `origin/main`. Inventory and
   preserve unexpected local changes before editing. A shared worktree may
   contain unrelated frontend work, so do not reset it merely to make it
   clean.
2. Read this file, `docs/product-architecture-plan.md`, and
   `docs/spl-compatibility-v0.1.md`.
3. Run `npm ci`, `go test ./...`, `go vet ./...`,
   `npm run test:frontend`, `npm run typecheck`, and `npm run lint`. Run the
   unrestricted repository-wide Go lint gate as well:

   ```sh
   golangci-lint run --timeout=5m \
     --max-issues-per-linter=0 --max-same-issues=0
   ```
4. If the next change touches the release path, install the pinned browser with
   `npx --no-install playwright install chromium` and run:

   ```sh
   OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
   go test ./integration \
     -run '^(TestBackendVertical|TestBrowser(FixedResultRendering|SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
     -count=1 -timeout=15m -v
   ```

5. The core real-process sustained-load/outage/restart proof is complete at
   `59b8f7c`, and its concurrent-search-during-recovery extension is complete
   at `9898b41`, on top of the log-generator foundation at `860acac`. The
   deterministic bounded-queue slow WebSocket consumer and browser inbound
   backpressure slice is complete at `4c4003f`; bounded fixed-payload browser
   rendering is complete at `9d6acc1`; high-source-count collector polling,
   reusable timers, clean-EOF framing, guard reuse, and scalable path sorting
   are complete at `f41720e`; composite configured pre-WAL redaction is
   complete at `34f3a9b`; bounded process output, diagnostic histories,
   frame evidence, and matching-WebSocket ownership are complete at
   `3f89229`; per-surface ordered configured-redaction replay and its
   safe-miss/duplicate/direct-hit performance coverage are complete at
   `1b89397`; statistics-only result specialization and authoritative
   result-kind dispatch are complete at `e647dd2`; full result-kind projection
   bounds, single-pass event decoding, and adaptation-local formatter reuse
   are complete at `c20204b`; deterministic committed release identity and
   embedded-asset verification are locally complete at `5ecd999`, with
   transaction cleanup proof at `f68630a`. Bounded ordered `list(field)` is
   complete at `4e2ddb4`, and the preceding run's reachable vulnerability and
   search-history race-fixture failures are repaired at `05c1eaf`. The
   cumulative Go lint ratchet and first boundary-hardening wave are complete at
   `b0c00f3`, `fbb8997`, and `4e00428`; the requested repository-wide cleanup
   is complete through `53a4dcc`, `b9c61ed`, and `caadf3f`, with the baseline
   unchanged. Run
   `30255910487` confirms the full workflow and independent release comparison;
   bounded chronological `earliest(field)` / `latest(field)` is complete across
   `932f403`, `ac721fb`, `e6acd1d`, `9714c79`, and `f9985a1`; percentile
   parser/planner support is committed at `efe4199`, with implementation and
   CI lint repair commits recorded at the top of this file. The bounded
   `relative_time` execution checkpoint is complete at `2a1245c`; bounded SPL1
   concatenation is complete through `875ddad`, `bc80006`, and `0b3f073`; and
   side-effect-free search validation is complete at `1919e2b`. Bounded search
   suggestions, scoped field completion, inline diagnostics, and connected
   time-picker semantics are complete through `9115465`, `2a82932`, `076ff43`,
   and `7eba237`; do not schedule those slices again. Broader `count` syntax
   remains a separate aggregate contract. The current preview-to-final
   resource-release audit pass is complete at `961cba2`, the sanitized current
   GradeThis Open Splunk path proof at `c576e85`, logical event retention at
   `458c8b4`, clock-driven job/result/export expiration at `b2b2839`, and
   stale-duplicate injection at `b80bf0a`. Add a red unit or integration test
   before implementation, run read-only adversarial reviews, fix concrete
   findings, then commit and push `main`.

## Remaining work, in priority order

### 1. Finish the first-release product proof

The uninterrupted collector-to-browser gate, exact GradeThis v0.1 corpus,
collector/server process-restart proof, resumable WebSocket unit contract,
real-browser retained replay, real-manager one-event replay expiration, and
offline cancellation publication/replay are complete. The dedicated real
browser `SEQUENCE_EXPIRED` fixture now also proves exact checkpoint retention,
stale-snapshot rejection, a genuine transient HTTP 503, queued-frame
suspension/application, live-update fencing, and same-connection
preview-to-final behavior without re-execution. The explicit sequence-gap
fixture is also complete: it drops `K+1`, forwards `K+2`, proves exact
reconnect/replay and live continuation, fences REST-first/replay-later progress
by the manager's state revision, and requires normal terminal socket cleanup
without re-execution. Its REST-only companion withholds the full terminal
projection, accepts completion only through authoritative polling, and proves
zero-row finalization behind a gated result response. The browser now
single-flights the whole REST recovery cycle, so duplicate triggers cannot
multiply backoff or follow-up scheduling. Honest browser cancellation is also
complete: it proves exactly one cancel POST, one executor context cancellation,
authoritative canceled presentation, and zero post-cancel reconnects.
Recovered-socket stale-duplicate fencing is complete as well: the browser
receives exact old preview and RUNNING frames after contiguous recovery, never
regresses preview or terminal presentation, accepts subsequent live frames,
and neither reconnects nor issues an extra authoritative GET. Clock-driven
terminal job/result/export expiration is now complete too: exact expiry
boundaries, preview and artifact invalidation, pinned leases, tombstone
retirement, socket closure, timer release, and polling quiescence are proven
through real managers and the production HTTP/WebSocket handler.

Logical event retention is complete at `458c8b4`. The common scan enforces
`expires_at > IndexTimeCutoff`, and the pinned non-vacuous integration fixture
proves exact-boundary exclusion across direct search, real-manager
preview/results, export re-execution, timeline, field discovery, and field
summary while the rows are still physically stored. Whole-millisecond policy,
native-driver timestamp range, version-3 reservation replay, version-4
metadata, and legacy SQLite migration boundaries are also pinned. ClickHouse
TTL remains merge-driven physical reclamation rather than the visibility
contract.

The sanitized current GradeThis Open Splunk path proof is complete at
`c576e85`. The committed config, real collector binary, scoped token,
checkpoint/WAL path, trusted metadata, 20-event sanitized manifest, and six
representative investigations are exercised through the public backend. The
separate exact v0.1 corpus remains unchanged and green. This proves the
replacement path inside Open Splunk; it does not cut over the target
GradeThis Compose deployment, which still uses its OTel filelog-to-ClickHouse
service.

Remaining first-release deployment work is the target GradeThis Compose
cutover from OTel filelog → direct ClickHouse to Open Splunk Collector →
Open Splunk server → ClickHouse. Perform that external-repository change in a
dedicated GradeThis worktree/branch, then rerun acknowledgment, public search,
and browser acceptance. Do not treat the Open Splunk harness proof as that
deployment mutation.

The current preview-to-final resource-release audit pass is complete at
`961cba2`. Search-job, executor, snapshot, and retained-result ownership were
clean; export lookup, cleanup, and final-download deletion now hand off safely
to shutdown without manager-wide lock/I/O coupling.

The core sustained-load proof is complete at `59b8f7c`. It runs 30,000 events
at a target 1,000 events/second through the real log generator, collector,
server, and pinned ClickHouse; proves a 5,000-event offline window plus
headroom; crash-restarts the collector from intact pending WAL; and requires
exact source, storage,
event-ID, raw/extracted-field, checkpoint, dead-letter, WAL, and public SPL
results. Its controlled red exposed and drove the pending-WAL resume fix.
The concurrent-search extension is complete at `9898b41`: ready-gated
eight-job waves prove an exact contiguous public source prefix, cohort-wide
monotonic visibility, strict source and searchable-prefix advancement, process
liveness, unique job identity, and public result metadata during live
recovery. Scan, queue, lifecycle, wall, and overlap measurements remain
observational until hardware and capacity decisions are made.

Release-proof implementation history and remaining confirmation:

- The deterministic bounded-queue slow WebSocket consumer and replay recovery
  path is complete at `4c4003f`. The separate fixed 1,000-row by 64-column
  browser rendering proof, including stable-DOM/two-animation-frame gates and
  interaction-wide DOM bounds, is complete at `9d6acc1`.
- High-source-count collector profiling is complete at `f41720e`. Reusable
  poll timers, clean-EOF framing avoidance, allocation-free steady guard
  fingerprints, scalable path sorting, and copy-truncate/drain race coverage
  are pinned.
- Composite configured pre-WAL redaction is complete at `34f3a9b`.
  Differential and fuzz coverage pins ambiguous encoded keys/values,
  prose-wrapped embedded JSON, duplicate-key canonicalization, exact names,
  marker precedence/cascades, binary transitions, specialized raw extents,
  aliases, and depth-limit fail-closed behavior. Match-heavy free text
  intentionally retains ordered replay and has direct/reverse-order benchmark
  coverage.
- The syntax-bearing configured-marker follow-up is complete at `1b89397`.
  Hazardous configurations detect changes once per independent event surface
  and replay only affected text; duplicate-only JSON canonicalizes once, and a
  direct typed field begins at its last matching policy before running the
  suffix. Differential fuzzing, independent goldens, unknown-wire
  confidentiality regressions, allocation checks, and safe-miss,
  duplicate-JSON, hit-only, sparse-hit, and direct-final-policy benchmarks pin
  both exact output and the performance tradeoff.
- Integration/browser harness hardening is complete at `3f89229`. Process
  output, each text diagnostic, diagnostic histories, stale-state evidence,
  matching WebSocket ownership, matching page sockets, raw frames, and
  pending Blob conversions all have explicit bounds. The real page self-test,
  compiled-browser cases, and adversarial review pin late safety latches and
  cleanup behavior.
- Statistics-only result specialization is complete at `e647dd2`. The adapter
  no longer builds events, derived fields, or an event histogram for
  transforming results; result-kind dispatch is authoritative and unsupported
  values fail closed.
- Full Events/Time Series specialization and formatter reuse are complete at
  `c20204b`. Events build only their event projection and decode each typed
  cell once; time series build only timeline points and avoid whole-row
  decoding. Event, time-series, and authoritative server-timeline formatters
  are lazy, adaptation-local, and count-pinned across repeated, empty, invalid,
  and timezone-changing calls.
- Release revision consistency across embedded UI, server, protobuf schema,
  and migrations is locally complete at `5ecd999`, with cleanup proof at
  `f68630a`. Local repeated builds are byte-identical, and CI defines
  independent Linux amd64/macOS builds plus a byte-for-byte canonical-proof
  comparison. The remote `f2e6915` workflow passed frontend, protobuf,
  vertical, and pinned GradeThis jobs but skipped the release comparison after
  dependency jobs failed. `05c1eaf` fixes the race fixture and reachable
  vulnerability. The fixed-revision lint ratchet and first cleanup wave are
  complete at `b0c00f3`, `fbb8997`, and `4e00428`; the full inherited
  inventory is eliminated through `53a4dcc`, `b9c61ed`, and `caadf3f`
  without advancing the adoption baseline. Run `30255910487` passed the
  entire workflow, including both production builds and their cross-platform
  canonical-proof comparison.
- Keep `app.log` only as local test input after a fixture secret scan. Do not
  commit unsanitized GradeThis production logs.

CI currently rebuilds the frontend in the frontend, vertical, and packaging
jobs. A follow-up should cache the vertical's Next.js work or, preferably,
build the backend-mode UI/server/collector once and pass the exact tested
artifacts to packaging without making the acceptance test depend on stale
outputs. Cache the pinned Playwright browser download where the CI environment
supports it.

At this checkpoint, `npm audit --omit=dev --audit-level=critical` exits
successfully but reports three high-severity findings in Next.js's transitive
PostCSS/Sharp chain. npm's offered force-fix would install the breaking
`next@9.3.3` downgrade; do not apply it blindly. Re-evaluate a safe Next.js,
PostCSS, or Sharp upgrade/override with the complete frontend and browser gates.

### 2. Continue explicitly selected Phase 2 aggregate correctness

The architecture plan's initial eval-function list, including bounded SPL1
period concatenation, is complete. Side-effect-free `/search/validate`,
bounded `/search/suggestions`, index/time-scoped field completion, inline
diagnostics, and the connected time-picker semantics are also complete.
Select any further aggregate/compiler slice explicitly; do not schedule those
completed search-assistance slices again.

The scalar-String extrema optimization, bounded ordered `list(field)`, and
bounded chronological `earliest(field)` / `latest(field)` are complete.
The bounded integer-suffix percentile family is complete and published at the
implementation and repair commits recorded at the top of this file.
The chronological history is `932f403`, `ac721fb`, `e6acd1d`, `9714c79`, and
`f9985a1`. If SPL expansion is the chosen next priority, implement one bounded
aggregate contract at a time:

- conditional `count(eval(...)) AS field` is complete at `66b2b16`; broader
  wildcard `count` forms still need a separate syntax and differential
  contract, while exact-field `c(field)` is complete;
- decimal suffixes, SPL2 two-argument `perc`, `upperperc`, and `exactperc`
  remain separate percentile contracts and are not part of the first bounded
  integer-suffix slice;
- `eventstats` should follow only after the aggregate library is stable;
  `streamstats` remains outside the first release unless scope changes; and
- exact Decimal comparison/aggregation remains separate work from the current
  finite-`Float64` runtime compatibility path.

Chronological nonblocking P3 opportunities are direction-aware Dynamic
selector generation when only `earliest` or only `latest` is requested, and
reducing the validation envelope's additional logical final-result pass
without weakening one-evaluation or atomic-error guarantees.

Extend `chart`, `rex`, or `spath` only behind compatibility and pinned
ClickHouse tests. Deferred `spath` surface includes auto-extraction, `{}`
multivalue output, XML, terminal containers, escaped literal-dot keys, and the
`spath()` eval function.

### 3. Improve compiler maintainability

- Move pending aggregate publication/validation descriptors out of persistent
  `compileState` into a typed aggregate-compilation result.
- Replace string-prefix presence rebinding with an explicit field-presence
  policy shared by projection, rename, eval, extraction, and aggregate output.
- Consolidate exact-string/candidate descriptors without weakening dc-only
  scalar lowering, values/dc sharing, extrema type propagation, or two-stage
  validation depth.
- Split the large pinned ClickHouse fixture/bootstrap and aggregate assertions
  into smaller helpers while retaining one reusable ephemeral container.
- Keep regressions for physical aggregate sharing, later hidden overflow,
  binary multivalue transport, tenant/index/time scope poison, calculated
  overwrite, and relational depth.

### 4. Continue Phase 3/4 product hardening

- Per-index retention and permissions, index/app administration, collector
  fleet operations, reports/dashboards, HEC compatibility, RBAC, and audit
  search.
- Migration upgrades, backup/restore and disaster recovery, load shedding,
  fair scheduling, per-user concurrency, alerts/scheduled searches, packaging,
  installers, upgrades, and signed releases.
- Keep the first release single-node; distributed control/search remains
  outside the current plan.

Resource/lifecycle audit findings to retain in the backlog:

- Implement physical `DELETE_DATA` only after composing the Store/replay fence
  and proven bounded outbox drain from `8676b4d` with a durable restartable
  operation, outcome-ambiguous ClickHouse mutation reconciliation, and
  terminal zero-row verification. Synchronous `KEEP_DATA` deletion is complete
  in `66f36d1`.

The architecture plan still requires product decisions for capacity-planning
retention/event size, target hardware, concurrent search load, immediate
Windows collector support, and whether dashboards are first-release scope.
Do not guess those decisions if they materially affect the implementation.

## Known compatibility boundaries

- A live licensed Splunk differential oracle is unavailable. Public
  documentation leaves several multivalue, null, binary, symbol-ordering, and
  error behaviors unspecified; keep Open Splunk choices explicit.
- Eligible Dynamic integers, Decimals, and bounded numeric Strings compare,
  sort, and aggregate through the normalized exact-decimal key. Physical
  Float/literal paths retain native `Float64`; a Float in a generic Dynamic
  ordering contributes ClickHouse's canonical rendered decimal spelling, not
  its exact binary rational.
- Ordinary String numeric classification is capped at 4,096 bytes. A validated
  semantic Decimal may use 4,097 bytes in exact comparison, sort, and extrema
  ordering to carry one compiler-added normalization byte. Other
  Decimal-consuming functions retain their documented 4 KiB limits.
- Numeric candidates sort before all lexical candidates in Open Splunk v0.1;
  punctuation within the lexical class uses raw-byte order. Symbol placement
  is not claimed as verified Splunk parity.
- Public Splunk documentation establishes ordinary occurrence semantics for
  `count(field)` but does not pin null multivalue members or typed containers.
  Open Splunk counts immediate non-null containers atomically.
- Collector decoding does not preserve every original numeric token spelling.
  String-oriented aggregates operate on stored canonical values.
- Collector checkpoint format v2 deliberately rejects nonempty v1 state. This
  is a greenfield, pre-release reset boundary rather than a migration promise.
- Default aggregate names containing parentheses cannot be referenced by the
  downstream field grammar; use `AS`.
- The 512 KiB `values` bound is a publication bound, not an aggregate-state
  memory guarantee. ClickHouse query memory remains authoritative before the
  post-aggregate check.
- Duplicate JSON member selection follows the pinned ClickHouse parser's
  first-member behavior.
- Interactive browser result schemas are deliberately limited to 1–64 columns,
  and a REST result response may not exceed its requested page size. Supporting
  broader schemas requires a product decision and column virtualization rather
  than silently materializing an unbounded row width.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm the checked-out branch is `main` and compare it with `origin/main`.
   Inventory and preserve every unexpected local change; do not reset unrelated
   work merely to obtain a clean tree.
2. Read the three documents listed at the top and inspect the latest `main`
   commits, especially `ab0514e`, `67689e8`, `a03aa33`, `72b1b11`, `347a015`,
   `e312ae9`, `9115465`, `2a82932`, `076ff43`, `7eba237`,
   `782da43`, `125b2bc`, `f3fc981`,
   `c84de56`, `f7a06b7`, `8161f2d`, `2a1245c`, `72b7936`, `421ba4d`, `6e18333`,
   `7dd3209`, `825c1e4`, `4966a7d`, `fe4b7bc`, `983e125`, `4d34c8a`,
   `c9221de`, `da587a4`, `1e78bf4`, `7bf4f6f`, `28c27e2`, `fe94e37`,
   `8d4d7b8`, `df5c13a`, `8d4911c`, `1314fc9`, `7d52001`, `33134e9`,
   `3ad5359`, `8accd61`, `d758a07`, `93ec477`, `faa88c1`,
   `39c0fd4`, `4233a5a`, `5f604b8`, `527a4ca`,
   `a316a4b`, `a4a07e3`, `3996659`, `738fabf`, `f68cacc`, `fed3276`,
   `c1ad25b`, `cfaa75b`, `2d35c66`, `070d24f`,
   `f9985a1`, `9714c79`, `e6acd1d`, `ac721fb`, `932f403`,
   `4e00428`, `fbb8997`, `b0c00f3`, `4e2ddb4`,
   `05c1eaf`, `f68630a`, `5ecd999`,
   `c20204b`, `e647dd2`,
   `1b89397`, `3f89229`, `34f3a9b`, `f41720e`, `9d6acc1`, `4c4003f`,
   `9898b41`, `59b8f7c`, `860acac`, `961cba2`, `c576e85`, `458c8b4`,
   `b2b2839`, `b80bf0a`,
   `cdb60df`, `787a7f9`, and `522b0ac`; the preceding progress/recovery
   foundations are `b5502a3`, `f72f184`, `ed28182`, and `d1286a4`.
3. Confirm no stale `open-splunk-*` Docker test containers are running.
4. Run the ordinary Go/frontend gates above and the focused exact corpus:

   ```sh
   OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
   go test ./internal/queryexec \
     -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
     -count=1 -timeout=6m -v
   ```

   Run both broader opt-in pinned ClickHouse suites before changing
   chronological, extrema, or bin metadata behavior.
5. The fixed-payload browser rendering measurement is complete at `9d6acc1`,
   and high-source-count collector profiling and polling are complete at
   `f41720e`; composite configured pre-WAL redaction is complete at `34f3a9b`.
   Bounded harness process output, diagnostics, evidence, and matching socket
   ownership are complete at `3f89229`, and per-surface ordered redaction replay
   is complete at `1b89397`. Statistics-only result specialization and
   authoritative result-kind dispatch are complete at `e647dd2`; complete
   result-kind projection bounds, single-pass event decoding, and
   adaptation-local formatter reuse are complete at `c20204b`; deterministic
   committed release identity, transactional publication, and embedded-asset
   proof are locally complete at `5ecd999` and `f68630a`; bounded ordered
   `list(field)` is complete at `4e2ddb4`, with CI vulnerability/race repair at
   `05c1eaf`. The cumulative lint ratchet and first cleanup wave are complete
   at `b0c00f3`, `fbb8997`, and `4e00428`; the zero-inventory cleanup is
   complete through `53a4dcc`, `b9c61ed`, and `caadf3f`, with the baseline
   fixed.
   Run `30255910487` passed the complete workflow and cross-platform release
   comparison. Bounded chronological `earliest(field)` / `latest(field)` is
   complete across `932f403`, `ac721fb`, `e6acd1d`, `9714c79`, and `f9985a1`;
   the bounded percentile family is published after parser/planner commit
   `efe4199`, exact-field `c(field)` is complete at `070d24f`, and native
   `isnull`/`isnotnull` predicates are complete at `2d35c66`, as described at
   the top of this file. Typed fixed-scalar `if` is complete across `cfaa75b`,
   `c1ad25b`, and `fed3276`; typed conditional count is complete at `66b2b16`.
   Typed Unicode `lower`/`upper` is complete through `8e68c7e`, `3d9d5f8`,
   `53b1f55`, and `8e4cf5f`; typed UTF-8 `len`/`length` is complete through
   `64004dc`, `e3a32e2`, and `5aebc70`. Subsequent bounded `substr`,
   default `tostring`, `round`, `ceil`/`ceiling`, `floor`, `mvcount`, `match`,
   `like`, `now`, `strftime`, `strptime`, and `relative_time` slices are
   complete. The current `relative_time` validation, plan, semantic-hardening,
   and execution checkpoints are `6e18333`, `421ba4d`, `72b7936`, and
   `2a1245c`. Bounded SPL1 period concatenation is complete through `875ddad`,
   `bc80006`, and `0b3f073`, and side-effect-free search validation is
   complete at `1919e2b`. Exact mixed numeric comparison, automatic sort, and
   extrema are complete at `a03aa33`. Explicit physical-file visibility
   ownership and bounded shutdown are complete at `ab0514e`. Do not infer the
   next slice from this historical list; wait for the user's next instruction.
   The
   generator foundation, current preview-to-final
   resource-release audit pass, sanitized current GradeThis Open Splunk path
   proof, logical event retention,
   clock-driven job/result/export expiration,
   recovered-socket stale-duplicate fencing, authoritative browser
   cancellation, versioned
   REST-first/replay-later progress, recovery-cycle coalescing, REST-only and
   accepted-WebSocket terminal discovery after a sequence gap, explicit
   browser sequence-gap recovery, browser `SEQUENCE_EXPIRED`, transient
   recovery-GET failure, retained replay, real-manager
   expiration/cancellation, the uninterrupted collector-to-browser path,
   exact GradeThis corpus, collector/server process-restart proof, and the
   protocol unit contract are already complete.
6. For the next scalar or aggregate slice, write an explicit syntax, null,
   multivalue, type, precision, approximation, timezone, and resource contract
   before implementation. Keep `eventstats` behind a stable aggregate library.
7. Preserve chronological event-order/member-order and atomic validation,
   scalar/Dynamic path separation, numeric grammar sharing,
   punctuation/UTF-8/zero/overlong boundaries,
   native timestamp precision, private calculated types, downstream `bin`,
   re-aggregation, scope poison, binary transport, physical state sharing, and
   relational-depth regressions.
8. Keep working on `main`; commit and push every cohesive green slice.
9. Preserve unexpected local changes and never reset them away.
