// Package control owns SQLite-backed indexes, saved searches, history,
// settings, and tokens. Versioned SQL migrations define the physical schema;
// GORM models provide typed control-plane access over the same configured pool.
package control
