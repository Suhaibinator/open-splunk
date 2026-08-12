# SPL compatibility v0.2 acceptance evidence

This directory is the durable, reviewable evidence bundle for runtime target
`dffde13c84d9a2ef0567e89dd527ec4776f5ca42` and tree
`70a99f34c187c6f1c46fa06549af78f33ed2e017`.

The target is the exact committed source revision exercised by the local
quality, integration, benchmark, release, and read-only compatibility-audit
gates. The later commit that carries this directory is documentation
provenance only; it is not the runtime activation target. Its identity is
deliberately resolved from Git history instead of being embedded here, which
avoids a recursively self-referential commit.

## Contents

- `manifest.json` binds source, toolchain, commands, receipts, audit results,
  release identities, and CI provenance.
- `manifest.schema.json` defines the pending and complete manifest states.
- `audit/report.json` is the exact redacted output from the first of four
  byte-identical audit runs.
- `audit/dispositions.json` classifies every report tuple exactly once; its
  adjacent schema defines the ledger format.
- `release/` contains the exact release verification, cross-binary identity,
  and static-asset manifest emitted by the target build. The much larger
  binaries are not committed; their hashes, sizes, and modes are recorded in
  the manifest and release receipt.
- `receipts/` contains the exact command output captured from the clean,
  isolated target checkout. Text receipts are used so they are neither hidden
  by the repository's log ignore rule nor treated as repository SPL sources by
  the compatibility auditor.
- `verify.mjs` checks bundle structure, identities, receipt bytes, audit-ledger
  coverage, release proof, placeholder state, and the final checksum index.

The synthetic control database is intentionally not included. Its digest was
unchanged before and after all audit reads, but SQLite database bytes are not a
cross-generation compatibility contract. Duplicate audit reports two through
four are also omitted because their equality and hashes are recorded in the
audit receipt.

## Verification

While the remote CI run is still pending, validate all completed evidence and
print the remaining machine-readable placeholders:

```sh
node docs/evidence/spl-v0.2/verify.mjs --allow-placeholders
```

After CI provenance replaces every `PENDING::...` sentinel and `SHA256SUMS` is
generated, the final command is:

```sh
node docs/evidence/spl-v0.2/verify.mjs
```

The strict form fails for any placeholder, unindexed file, digest mismatch,
source-identity mismatch, non-successful gate, audit coverage gap, unresolved
finding, or non-successful CI job.

