# SPL compatibility v0.4 migration guide

**Authored-search target:** `0.4`

**Product target:** `0.4.0`

Version 0.4 adds exact immutable CSV lookup enrichment and cumulatively
activates the v0.3 command surface. Before upgrading a deployment, validate
retained searches against a v0.4 binary, publish lookup assets privately, and
verify automatic selectors on production-shaped data.

Use explicit conversion when an event key is not already a String:

```spl
index=api
| eval status_key=tostring(status)
| lookup http_statuses code AS status_key OUTPUTNEW description category
```

`OUTPUT` overwrites listed destinations on a match. `OUTPUTNEW` preserves every
present non-null destination and fills a missing or null destination. No match
preserves the row and all existing fields.

CSV empty cells are empty Strings, not nulls. Duplicate composite keys prevent
publication. Every replacement publishes a new immutable logical definition
version; a replacement with CSV also advances the physical asset version,
while a metadata-only replacement reuses it. Admitted jobs and retained
derived products continue using their captured old versions.

Rollback to an older binary never executes retained v0.4 SQL or an unresolved
lookup. Saved searches containing `lookup` fail validation until a v0.4 binary
and the referenced published asset are restored.
