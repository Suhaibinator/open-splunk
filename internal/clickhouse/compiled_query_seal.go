package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
)

type compiledSQLSeal [sha256.Size]byte

func sealCompiledQuerySQL(compiled CompiledQuery) CompiledQuery {
	compiled.sqlSeal = sha256.Sum256([]byte(compiled.SQL))
	return compiled
}

// HasValidSQLSeal reports whether SQL is the unchanged output of Compiler.
// The seal covers query structure only: Args remain mutable so callers can
// retain typed driver binding without serializing values into SQL.
func (compiled CompiledQuery) HasValidSQLSeal() bool {
	expected := sha256.Sum256([]byte(compiled.SQL))
	return subtle.ConstantTimeCompare(expected[:], compiled.sqlSeal[:]) == 1
}
