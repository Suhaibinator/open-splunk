# Versioning

Open Splunk has one current product release, `0.4.0`. Its authored SPL
compatibility identity is `0.4`; API, storage, and catalog-object versions are
separate technical domains and do not select alternate product profiles.

| Domain | Current release | Authority | Purpose |
| --- | --- | --- | --- |
| product | `0.4.0` | release tag/build metadata | Names server, collector, and UI artifacts. |
| authored SPL | `0.4` | `internal/spl.CompatibilityVersion` | Identifies the cumulative parser, planner, compiler, and execution contract. |
| HTTP/protobuf API | `v1` | protobuf package and route prefix | Governs append-only transport compatibility. |
| storage schemas | migration sequence | SQLite and ClickHouse migration manifests | Advances persistent database layouts. |
| catalog objects | per-object integer | control-plane mutation transaction | Provides optimistic concurrency and immutable historical versions. |

Reusable calculated-field expressions intentionally use a smaller closed
grammar than authored searches. Knowledge snapshots also retain an internal
compiler identity and immutable lookup authority so persisted work can be
validated and rebuilt safely. Neither is a public, caller-selectable runtime
profile.

## Compatibility releases are cumulative

The authored-search documents are normative deltas. Version 0.4 incorporates
unchanged rules from v0.1 through v0.3 and adds lookup behavior; it does not
copy the earlier contracts into one large file. Files and tests named `v02` or
`v03` therefore identify the release that introduced a rule, not an older
runtime selected at execution time.

The repository retains earlier contracts, corpora, and migration guides
because they provide regression authority and explain source transitions for
saved searches. Historical activation ceremony is not runtime behavior and
must not remain in a current contract after the release process changes.

## Retained authority

Every admitted search records the exact authored compiler version. Knowledge
snapshots separately retain the internal compiler and immutable lookup evidence
needed to validate their content. A rebuild-based export, inspection, or
analysis fails closed if retained evidence differs from the running compiler;
history rerun reparses stored source under the current profile and records a new
job.

Callers cannot choose a compatibility version or enable individual syntax
flags. A fully composed production binary advertises authored compatibility
`0.4` and its available capabilities through bootstrap. Jobs, history,
inspection, analysis, export, and release verification retain the applicable
authored identity and reject incompatible rebuilds.

## Current release

Product `0.4.0` is the single active release profile and carries cumulative
authored SPL compatibility `0.4`. Release publication derives the product
version from tag `v0.4.0`, reads the authored compatibility identity from its
canonical source declaration, and qualifies that pair before publishing server
and collector artifacts.

Earlier tags, compatibility contracts, corpora, and migration guides remain in
the repository as regression and persisted-data evidence. They do not create
runtime-selectable profiles in the current product, and current release tooling
does not rebuild historical artifacts from current source.
