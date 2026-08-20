# Open Splunk documentation

Open Splunk is under pre-release development. These documents describe the
current source tree, not a released compatibility surface or the sequence in
which features were built. Persistent data and clients are expected to track
the same source revision unless a document explicitly says otherwise.

## Reference

- [Architecture](architecture.md) explains the implemented components,
  authority boundaries, and data flow.
- [API](api.md) describes the `open_splunk` protobuf package, HTTP routes,
  collector gRPC stream, and search WebSocket.
- [SPL](spl.md) is the cumulative authored-search contract.
- [Knowledge](knowledge.md) covers field knowledge, lookups, immutable
  snapshots, lifecycle, and security.
- [Ingestion](ingestion.md) covers native collectors, token constraints,
  quotas, and collector operations.
- [HTTP Event Collector](hec.md) covers the HEC protocol and its deployment,
  recovery, load, soak, and slow-client gates.
- [Auditing](auditing.md) covers mutation and search-attempt journals.
- [Roadmap](roadmap.md) lists only work that is not implemented.
- [Build and publication status](releasing.md) defines current development
  artifacts and the work required before the first supported release.

Database bootstrap and recovery rules live in
[`migrations/README.md`](../migrations/README.md). Deployment and executable
integration instructions live in [`deploy/README.md`](../deploy/README.md) and
[`integration/README.md`](../integration/README.md).
