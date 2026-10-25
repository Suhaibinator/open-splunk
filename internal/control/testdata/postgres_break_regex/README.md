# GLOB and LIKE differential fixtures

`postgres_break_regex_test.go` checks every pattern-matching predicate of the
PostgreSQL control-plane schema against SQLite's verdicts on the final SQLite
schema (`migrations/sqlite` 0001-0012 applied).

- `cases.json` lists the predicates. An `atom` is one GLOB or LIKE over a
  column `x` with its PostgreSQL translation. A `check` is a whole CHECK
  constraint, found in the PostgreSQL catalog by table and name. A `trigger`
  is the `IF ... THEN` condition of a trigger function, paired with the WHEN
  clause of the SQLite triggers it replaces. `target` is the fuzzed column.
  Other columns take their values from `contexts`, cycling by input position.
  `seeds` add structured starting values, such as UUIDs and app ids.
- `sqlite_verdicts.tsv` holds SQLite's `t`/`f`/`n` verdict for each generated
  input, with the SHA-256 of the corpus it was recorded for.
- `sqlite_oracle.py` records those verdicts. Before evaluating, it checks two
  things. Every case's SQLite text must appear verbatim in the database. Every
  SQLite CHECK and trigger that uses GLOB or LIKE must have a case.

The test generates the corpus deterministically from each case id (2,500
inputs per case). The corpus covers NULL, the empty string, every code point
below U+0100, regex and GLOB syntax, backslash runs, Unicode case and
normalization traps, and strings shaped by the expression's classes and
length bounds. On the PostgreSQL side, the test also fails when a CHECK,
function, index predicate, default or trigger WHEN clause pattern-matches
strings without a case, or when a case no longer matches the schema.

## Regenerating the verdicts

Only needed after changing `cases.json` or the corpus generator. You need the
final SQLite schema as a database file:

```
OPEN_SPLUNK_BREAK_REGEX_DUMP=/path/to/dump.json \
  go test -run TestPostgresGlobAndLikePortsMatchSQLiteVerdicts ./internal/control/
python3 -I internal/control/testdata/postgres_break_regex/sqlite_oracle.py \
  /path/to/final.db /path/to/dump.json
```

Review the diff of `sqlite_verdicts.tsv` before committing it.
