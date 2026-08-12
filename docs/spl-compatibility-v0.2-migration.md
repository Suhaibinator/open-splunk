# SPL compatibility v0.2 migration and audit guide

**Compatibility target:** authored searches `0.2`

**Knowledge-expression boundary:** Tier-1 calculated fields remain `0.1`

Version 0.2 adds arithmetic, scalar grouping, single-quoted scalar fields,
`==`, and eval-language membership. It also reserves `+`, `-`, `*`, `/`, and
`%` as operators in authored `eval` and `where` scalar expressions. Saved
search and history source remains byte-for-byte authored text; the server does
not rewrite it during upgrade or rerun.

The normative behavior is in
[`spl-compatibility-v0.2.md`](spl-compatibility-v0.2.md). This guide is an
operator checklist, not an additional compatibility contract. Final rollout
evidence belongs in the
[`v0.2 pre-activation checkpoint`](spl-compatibility-v0.2-acceptance.md).

## Supported operator examples

```spl
index=api
| eval latency_seconds=duration_ms/1000
| where latency_seconds>=0.5
| stats avg(latency_seconds) AS mean_seconds BY route
```

```spl
index=api
| eval request_kib='request-bytes'/1024
| where status IN (400, 401, 403, 404, 500, 502, 503)
| eval weighted_cost=(request_bytes+response_bytes)*retry_count
```

```spl
index=api
| eval class=if(environment IN ("production", "staging"), "managed", "other")
| eval label="status=" . status
```

Arithmetic produces nullable `Float64`. Use a Float64 conditional alternative,
such as `0.0`, when the other branch is arithmetic. Membership is a Boolean
predicate: consume it directly in `where`, `if`, `case`, Boolean composition,
or `count(eval(...))`; do not assign it directly or compare it with `true`.

## Source changes to review

| Earlier intent or spelling | Version 0.2 spelling | Reason |
| --- | --- | --- |
| `eval kib=request-bytes/1024` where `request-bytes` is one field | `eval kib='request-bytes'/1024` | unquoted `-` is subtraction |
| `where used-capacity>0` where `used-capacity` is one field | `where 'used-capacity'>0` | special-character scalar fields require single quotes |
| `eval copied="HTTP Status"` where a field was intended | `eval copied='HTTP Status'` | double quotes are String literals; single quotes are field references |
| `eval label="status="+status` | `eval label="status=" . status` | `+` is numeric only; period concatenates Strings |
| `eval value=if(status=200, bytes+delta, 0)` | `eval value=if(status=200, bytes+delta, 0.0)` | v0.2 does not widen the existing conditional branch types |
| `index=api status IN (500, 503)` | `index=api | where status IN (500, 503)` | base-search membership remains unsupported |
| `eval selected=status IN (500, 503)` | `eval selected=if(status IN (500, 503), 1, 0)` | a Boolean membership result cannot be assigned directly |
| `where (status IN (500, 503))=true` | `where status IN (500, 503)` | membership is not a scalar and cannot be compared with a Boolean literal |

Quoted fields are enabled only in scalar `eval`/`where` positions and as an
`eval` destination. They are not accepted in `table`, `fields`, `sort`, `BY`,
or the other command-specific field grammars listed in the contract. When a
later command needs a special-character field, first copy it to an ordinary
alias:

```spl
index=api
| eval request_bytes='request-bytes'
| table request_bytes
```

Do not copy v0.2 expressions into Tier-1 calculated-field knowledge objects.
Their closed `SPLExpressionV01` profile continues to reject arithmetic,
grouping, membership, and quoted fields. An authored v0.2 search may consume
the output of an already validated v0.1 knowledge prelude.

## Run the read-only compatibility audit

The `audit-spl-v0.2` subcommand inventories ambiguous unspaced operator
characters in saved searches and repository-owned SPL fixtures. Supply at
least one explicit input; the command never infers the running server's paths.
For release evidence, use a quiesced, sanitized copy of the control database
and the exact repository revision being activated.

```sh
make build-server

./build/open-splunk-server audit-spl-v0.2 \
  -control-db /absolute/path/to/sanitized-control.db \
  -repository /absolute/path/to/open-splunk
```

Either source can be scanned independently:

```sh
./build/open-splunk-server audit-spl-v0.2 \
  -control-db /absolute/path/to/sanitized-control.db

./build/open-splunk-server audit-spl-v0.2 \
  -repository /absolute/path/to/open-splunk
```

The control database is opened through the query-only control-plane path; it
is not migrated, checkpointed, or mutated. The repository traversal does not
follow symbolic links and excludes VCS, generated, dependency, cache, and
build trees. Both scans have fixed object, file, byte, and finding ceilings.

The command writes one deterministic JSON report to standard output. It
contains only the target version, a scanned-object count, and redacted
identities/locations:

```json
{
  "compatibility_version": "0.2",
  "scanned_objects": 2,
  "findings": [
    {
      "object_id": "saved-search-id",
      "source_location": "control-db/saved_searches/saved-search-id:1:37",
      "kind": "ambiguous_unspaced_scalar_operator"
    }
  ]
}
```

Authored SPL and field values never appear in the report. Protect the output
as operational metadata because saved-search IDs and repository locations may
still be sensitive.

## Resolve findings

Each finding identifies an operator-shaped source location, not a proven
incompatibility. The audit cannot infer whether `request-bytes` means one
legacy field or the arithmetic expression `request - bytes`.

For every finding:

1. Open the identified saved search or repository fixture through its normal
   authorized workflow.
2. Decide whether the operator is intentional arithmetic.
3. If one punctuation-bearing field was intended, single-quote the exact field
   in its scalar position. If arithmetic was intended, keep the operator and
   add spacing when that improves clarity.
4. Validate the complete source with the v0.2 backend before saving it.
5. Rerun the audit on the same bounded inputs and retain the redacted report.

The audit performs no automatic rewrite. A zero-finding report means no
ambiguous operator-shaped candidates were found in the inputs that were
actually scanned; it is not evidence that unscanned history, external assets,
or another control database is compatible.

## Rollback behavior

Rolling back to a v0.1 binary does not reinterpret or execute a persisted v0.2
AST or SQL because neither is stored. A v0.2 saved-search source may fail the
older parser, and a retained source rerun always records the compatibility
identity of the binary performing that new run. Existing running jobs continue
under the process and immutable snapshot that admitted them.
