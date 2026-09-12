# Event patterns

The Patterns tab groups the **retained result rows** of a completed search. It
uses the final string value of `_raw`, including any transformation performed by
the search. A missing, null, or non-string `_raw` value is excluded. Empty strings
are eligible. The panel shows retained, eligible, and excluded counts; coverage
is `group event count / eligible event count`, not a percentage of the full
search's event count.

Patterns reads the durable result snapshot. It does not execute the search
again. Pagination, later ingestion, deletion of the original indexed events,
and a server restart cannot change a surviving snapshot's groups or members.
An expired or unavailable snapshot produces an error; it does not silently
switch to a new search. The server still checks access to the retained job on
every request.

A result retention limit can make the snapshot smaller than the full search.
For example, a search with 10,001 events can retain 10,000 rows. Patterns covers
those 10,000 rows and explicitly discloses truncation. An ordinary full-search
export can contain all 10,001 events.

## Sensitivity and algorithm

Algorithm `1` is deterministic. Every sensitivity first validates UTF-8,
collapses runs of Unicode whitespace to one ASCII space, and trims leading and
trailing whitespace. Case and other characters remain unchanged.

| Sensitivity | Normalization |
| --- | --- |
| Precise | Replace whole ASCII hexadecimal tokens of at least 16 digits with `<hex>`. |
| Balanced | Also replace whole signed ASCII integers with `<int>`. Decimal and exponent forms remain literal. |
| Broad | Keep the first two whitespace-delimited tokens, then also replace decimal and exponent forms with `<decimal>`. |

A token boundary is defined using ASCII `[A-Za-z0-9_]`, independent of locale.
Non-ASCII letters are outside this set. Hexadecimal classification takes
precedence over decimal classification, which takes precedence over integer
classification. An adjacent decimal point continues a numeric token rather
than permitting a partial integer or hexadecimal replacement. Numeric tokens
embedded in ASCII words and malformed numeric continuations remain literal.

Integers permit an optional `+` or `-` followed by one or more ASCII digits.
Decimals permit an optional sign, digits with an optional decimal point or a
leading point followed by digits, and an optional `e` or `E` exponent with an
optional sign and at least one digit. A decimal must contain a point or an
exponent. Balanced preserves the entire decimal token, including its integer
substrings. Hexadecimal tokens have no prefix and use only `0–9`, `a–f`, and
`A–F`; they must contain at least 16 digits.

Grouping uses typed placeholders, so literal text cannot impersonate a
normalized value. Display signatures escape literal backslashes and `<`
characters with a backslash. Thus the literal text `<int>` displays as
`\<int>`, while an integer placeholder displays as `<int>`. A literal `*`
remains a literal star. An empty or whitespace-only event has an empty
signature; Broad does not append a wildcard to short or empty input.

All groups are available through pagination, ordered by descending event count
and then ascending signature. Identifiers bind the search job, immutable
snapshot generation, algorithm, sensitivity, and typed signature.

## Exact events and exports

**View events** opens the exact member rows in the Events view, in original
retained ordinal order. It does not turn the displayed signature into an SPL
wildcard query. A pattern filter identifies the selected group; **Clear
pattern** restores the ordinary retained result view without rerunning the
search. Changing the job, snapshot, or sensitivity clears the selection.

A pattern summary export includes every group and typed `pattern`, `count`,
and `percent` columns. A pattern member export includes every exact member of
the selected group and preserves source cell types. Both export sources pin
the same immutable retained relation, including the truncation provenance.
They are separate from ordinary full-search exports and their safeguards.

## API

Both endpoints use protobuf requests and require the opaque `snapshot_ref`
from a final `ResultPage`:

- `POST /api/search/jobs/patterns/list` accepts the job, snapshot reference,
  sensitivity, and page request. It returns groups, exact eligible/excluded/
  retained counts, truncation/completeness metadata, and algorithm version.
- `POST /api/search/jobs/patterns/members` also requires the pattern identifier
  and accepts an exact column projection. It returns original typed result
  rows. Its cursor binds the projection as well as the group and snapshot.

Cursors are opaque. Reuse them only with the same request identity. A page can
be shorter than the requested size because of the response byte budget;
continue using its next cursor until none remains.

## Resource bounds

The service has configurable admission and memory limits. Defaults are two
workers, 15 seconds per operation, 100,000 input rows and groups, 128 MiB of
input, 64 KiB per displayed signature, 128 MiB of charged working memory per
analysis, a 64 MiB cache, and 320 MiB globally including pinned exports.
Decoded buffers, group maps, ordinal indexes, responses, and export pins count
toward their corresponding budgets. A catalog too large for the cache can be
delivered through a bounded uncached operation. Exceeding an analysis limit
fails the operation atomically; the server does not report a partial prefix as
a complete set of patterns.

The Patterns artifact reader reserves memory before JSON decoding. A framed
header reserves 32 times its encoded length plus 1 MiB; a row reserves 32 times
its encoded length plus 256 KiB. This conservative allowance includes the
encoded buffer, decoded JSON values and containers, and the immutable typed
value tree. Legacy artifacts reserve 32 times the entire encoded file plus
1 MiB because their streaming decoder can buffer beyond the current row.
A reservation that cannot fit fails before allocating or decoding that frame.
Ordinary retained-result readers keep their existing behavior.

Responses retain their reservation through conversion, protobuf marshaling,
and the HTTP write. Member pages and group pages use a conservative 7 MiB
response envelope and an 8 MiB final protobuf limit. Page size defaults to 20,
with a configurable ceiling no greater than 100. A smaller ceiling clamps the
requested maximum; it does not change the relation selected by the cursor.
