// Package audit persists bounded, immutable control-plane security audit
// events. Versioned SQLite migrations are the schema authority; the GORM
// models in this package are explicit projections and are never migrated by
// GORM.
package audit
