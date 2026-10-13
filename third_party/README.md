# Pinned GoCommon sources

`go-common/` is the Go module source snapshot consumed by this repository's local
module replacement. It includes the shared authentication, organization and
middleware changes required by this integration without relying on an unpublished
remote commit. `go-common-provenance.json` records upstream revision and exact
file hashes, including local integration changes. The upstream Go module has no
repository license file; its copied README records that fact and the current
distribution scope. The frontend package separately declares ISC.

Run `node scripts/sync-go-common.mjs /path/to/go-common` to deliberately update the
Go snapshot, and `node scripts/check-go-common.mjs` to verify its contents. The
snapshot includes Go sources, protobuf source contracts and embedded SQL
migrations. It excludes tests, fixture data, credentials and build output.

`go-common-core/` contains the matching compiled frontend client and its
repacking instructions. Both snapshots are checked-in release inputs.
