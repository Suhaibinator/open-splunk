// Package collectorfleet persists bounded, tenant-scoped collector identity,
// active-lease fencing, and latest-wins operational telemetry.
//
// SQL migrations are the sole schema authority. This package uses explicit
// GORM models only for typed queries and never calls AutoMigrate.
//
// This package is a persistence primitive, not yet the ingest admission
// boundary. A server must invalidate prior-boot leases before accepting
// traffic. Runtime integration must also combine credential revalidation,
// accepted-token use, enabled-state validation, and lease claim in one
// immediate SQLite transaction; composing those operations sequentially is
// not safe.
package collectorfleet
