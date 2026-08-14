# SPL compatibility v0.2 activation evidence

This is the accepted-capable evidence authority for the final v0.2 closeout.
Its checked-in carrier and a materialized qualification candidate remain
non-accepted; only a verifier-bound direct-child evidence revision may be
accepted. A synthetic local fixture is explicitly labeled and is not remote
or publication authority. The historical [`../spl-v0.2/`](../spl-v0.2/)
bundle and the immutable negative
[`../spl-v0.2-closeout-2026-08-12/`](../spl-v0.2-closeout-2026-08-12/)
bundle remain unchanged.

## Satisfiable two-revision protocol

The runtime/release revision `R` contains every v0.2 semantic correction and
keeps `internal/spl/doc.go` at exactly `0.2`. In `R`, this manifest and the
acceptance report say `qualification-candidate`. `R` cannot contain its own
Git object ID or post-commit CI results, so those values are deliberately
unbound. The candidate verifier derives `R` from the clean checkout and checks
the exact hashes of every stable artifact. Candidate binaries are quarantined
qualification artifacts and are not authorized for stable publication.

Only after the exact `R` is remotely reachable and its complete CI and release
gates are terminal-success may one direct-child evidence revision `E` be
created. `E` may change only:

- `docs/spl-compatibility-v0.2-acceptance.md`;
- `docs/evidence/spl-v0.2-activation/manifest.json`; and
- regular sanitized files under
  `docs/evidence/spl-v0.2-activation/receipts/`.

`E` records `R`, its tree, candidate report and manifest hashes, every stable
artifact hash, an immutable runtime tag and remote readback, the complete set
of terminal-success CI jobs, release source identity, the server's exact
`spl_compatibility_version=0.2` readback, binary identities, artifact digests,
and checksummed receipts. It does not embed its own hash. A release tag points
to `E`; the publication verifier returns `R`, and the image workflow builds
that exact qualified source revision.

The accepted manifest indexes exactly eight strict JSON receipts:
`source-identity`, `quality-gates`, `clickhouse-gates`,
`compatibility-audit`, `ci-run`, `ci-jobs`, `release-identity`, and
`release-artifacts`. Their paths and JSON shapes are closed by the verifier;
the CI and release receipts must exactly reproduce the corresponding manifest
authorities. SPL v0.2 is intentionally application version `0.1.0`; its
release tag is exactly `v0.1.0` and points remotely to `E`. Qualification is a
protocol phase, not a SemVer prerelease suffix. Before creating the tag, remote
readback must prove that `v0.1.0` is absent or already resolves to this exact
`E`; a conflicting immutable tag stops activation.

## Phase verification

The expected phase is mandatory. Dirty-tree tolerance exists only when the
implementation-checkpoint phase is being verified:

```sh
node scripts/verify-spl-v02-acceptance.mjs \
  --phase implementation-checkpoint --allow-dirty
node scripts/verify-spl-v02-acceptance.mjs \
  --phase qualification-candidate
node scripts/verify-spl-v02-acceptance.mjs \
  --phase accepted
```

Stable publication additionally performs live GitHub readback of `R`, the
release tag for `E`, the CI run, and the complete job set. On success stdout
contains only `R`, so workflow output cannot be confused with diagnostics:

```sh
GITHUB_TOKEN=... GITHUB_REPOSITORY=Suhaibinator/open-splunk \
node scripts/verify-spl-v02-acceptance.mjs \
  --phase accepted --publication --print-runtime-revision
```

The strict verifier rejects duplicate or unknown JSON fields, symlinks,
unsafe or unindexed receipt paths, dirty candidate/evidence trees, merge
evidence commits, non-allowlisted `E` changes, mismatched Git objects or
hashes, mutable runtime refs, incomplete/skipped/failed CI jobs, release
identity disagreement, stale remote readback, and any attempt to publish the
checkpoint or candidate phase.

This initial-activation record authorizes only its exact `R`. Later runtime
changes require new release provenance and cannot reuse this direct-parent
evidence commit.

Later compatibility evidence must bind the accepted v0.2 prerequisite by its
immutable evidence commit, rather than rereading mutable v0.2 paths from a
newer carrier. If `E` is an ancestor of the current checkout, the following
replays the complete accepted verification from the exact blobs at `E` and
prints `E` plus the SHA-256 of its accepted manifest:

```sh
node scripts/verify-spl-v02-acceptance.mjs \
  --phase accepted \
  --evidence-revision "$V02_E" \
  --print-evidence-binding
```

This ancestor mode does not perform stable publication and never substitutes
current-worktree files for the evidence revision.
