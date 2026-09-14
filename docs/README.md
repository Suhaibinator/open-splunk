# Open Splunk documentation

Open Splunk is under major-version-zero development. These documents describe
the current source tree and v0 fresh-state contract, not a stable compatibility
surface or the sequence in which features were built. Persistent data and
clients are expected to track the same exact release or source revision unless
a document explicitly says otherwise.

## Reference

- [Architecture](architecture.md) explains the implemented components,
  authority boundaries, and data flow.
- [API](api.md) describes the `open_splunk` protobuf package, HTTP routes,
  create-request retry receipts, collector gRPC stream, and search WebSocket.
- [SPL](spl.md) is the cumulative authored-search contract.
- [Timechart](timechart.md) defines exact grids, split selection, pipeline
  composition, bucket metadata, and resource behavior.
- [Event patterns](patterns.md) describes retained snapshot grouping, exact
  members, exports, and resource bounds.
- [Dashboards](dashboards.md) describes all eight visualizations, typed values,
  result coverage, and panel execution limits.
- [Knowledge](knowledge.md) covers field knowledge, lookups, immutable
  snapshots, lifecycle, and security.
- [Theming](theming.md) defines the two-tier colour token layer, the
  non-colour scales beside it, the palette and light/dark axes a theme
  resolves through, and the rule that no literal may live outside them.
- [Ingestion](ingestion.md) covers native collectors, token constraints,
  quotas, and collector operations.
- [Insert coalescing](insert-coalescing.md) defines the durable logical-batch
  and physical-write-group protocol, recovery rules, resource bounds, and
  acceptance measurements shared by native and HEC ingestion.
- [Collector configuration](collector-configuration.md) enumerates the
  collector CLI, YAML schema, defaults, environment templating, processors,
  and TLS boundaries.
- [HTTP Event Collector](hec.md) covers the HEC protocol, operational counters
  and distributions, deployment, recovery, load, soak, and slow-client gates.
- [Auditing](auditing.md) covers mutation and search-attempt journals.
- [Search sharing, schedules, and alerts](search-sharing-alerts.md) defines
  nearby-event navigation, link lifecycles, saved-search scope labels,
  scheduled retention, and signed webhook delivery.
- [Roadmap](roadmap.md) lists only work that is not implemented.
- [Build and publication status](releasing.md) defines current development
  artifacts, official v0 publication, and the work required before v1.

Schema bootstrap, migration-ledger, and database recovery invariants live in
[`migrations/README.md`](../migrations/README.md). Production-shaped deployment
and coordinated recovery procedures live in
[`deploy/README.md`](../deploy/README.md); executable integration instructions
live in [`integration/README.md`](../integration/README.md).

## Bundled Help

The static `/help/` site includes these guides and their linked configuration
examples in every UI build. Navigation and full-text search need neither
internet access nor a working backend API while the local static asset server
remains reachable. Local Markdown and example links are validated and rewritten
to Help routes during the build. Code samples and Markdown HTML are rendered as
text; they are never executed. The footer identifies the source revision used
to build the documentation.
