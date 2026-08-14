# SPL compatibility v0.3 activation evidence

This bundle is the machine-readable authority for the v0.3 activation phase.
It is non-accepted in both permitted pre-evidence phases: the checked-in
carrier is an implementation checkpoint, and a materialized runtime `R` is a
qualification candidate. Neither phase is accepted v0.3 evidence or a v0.3
CI, release, or publication claim.

## Satisfiable two-revision protocol

The runtime revision `R` contains the complete implementation, changes
`internal/spl/doc.go` to `0.3`, and changes both the report and manifest to
`qualification-candidate`. `R` cannot contain its own Git object ID or the CI
result produced after it is committed, so those fields remain unbound in `R`.
The candidate verifier derives `R` from the clean checkout and checks every
stable artifact hash. CI artifacts from `R` are quarantined qualification
artifacts; they are not stable tag, package, or deployment publication.

After `R` is reachable and its exact CI and release gates are terminal-success,
create one direct-child documentation-only revision `E`. `E` changes only:

- `docs/spl-compatibility-v0.3-acceptance.md`;
- `docs/evidence/spl-v0.3/manifest.json`; and
- regular sanitized files below `docs/evidence/spl-v0.3/receipts/`.

`E` records `R`, its tree, immutable remote tag, the SHA-256 of the candidate
report, stable artifact hashes, terminal CI jobs, exact release identities,
artifact digests, and checksummed receipts. It does not embed its own hash.
The `release-readback` receipt is byte-for-byte `release-verification.txt` from
the qualified build. Its `ui_build_id` is the literal deterministic `r`-prefixed
Next.js build identifier derived from the application version and `R`, never a
SHA-256 substitution; `ui_sha256` is the separate UI-content digest.
SPL v0.3 advances the independent application release from v0.2's `0.1.0` to
`0.2.0`; the release tag is exactly `v0.2.0` and points to `E`. Qualification
is a protocol phase, not a SemVer prerelease suffix. Remote readback must prove
that `v0.2.0` is absent or already resolves to this exact `E`; a conflicting
immutable tag stops activation. The publication verifier returns `R`, and the
image workflow checks out and builds that qualified runtime source.

## Phase commands

The expected phase is mandatory. A dirty tree is tolerated only for verifying
the present checkpoint while assembling changes:

```sh
node scripts/verify-spl-v03-acceptance.mjs \
  --phase implementation-checkpoint --allow-dirty
node scripts/verify-spl-v03-acceptance.mjs \
  --phase qualification-candidate
node scripts/verify-spl-v03-acceptance.mjs \
  --phase accepted
node scripts/verify-spl-v03-acceptance.mjs \
  --phase accepted --evidence-revision <E> --print-evidence-binding
```

The direct accepted command is valid only while the checkout itself is `E`.
On a later descendant, CI resolves the most recent revision that changed
the activation manifest and uses the exact-E command. That mode requires full
history, proves `E` is an ancestor, replays the immutable R/E objects, and
prints only `<E> <manifest-sha256>`. A shallow clone fails closed. It never
authorizes publication from the descendant.

Stable publication uses live GitHub and immutable remote readback in addition
to local bundle checks. On success, stdout contains only `R` so workflow output
cannot be confused with diagnostics:

```sh
GITHUB_TOKEN=... GITHUB_REPOSITORY=Suhaibinator/open-splunk \
node scripts/verify-spl-v03-acceptance.mjs \
  --phase accepted --publication --print-runtime-revision
```

The strict verifier rejects duplicate or unknown JSON fields, symlinks,
unsafe receipt paths, dirty candidate/evidence trees, merge evidence commits,
non-allowlisted `E` changes, mismatched Git objects/hashes, an unaccepted v0.2
prerequisite, missing/skipped CI jobs, release identity disagreement, and any
attempt to publish the checkpoint or candidate phase. Historical replay also
rejects shallow repositories and legacy Git grafts so neither incomplete nor
locally rewritten topology can establish the R/E chain.

`--publication` remains mutually exclusive with `--evidence-revision` and is
accepted only when the checked-out `HEAD` is exactly the release-tagged `E`.

When completed by verified non-synthetic remote evidence, this bundle
authorizes only the initial v0.3 activation artifact `R`. A synthetic/local
accepted fixture exercises protocol mechanics but never authorizes publication.
A later runtime revision needs its own release provenance; it cannot reuse this
direct-parent evidence record as proof for changed source.
