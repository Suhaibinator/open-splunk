package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

type compiledSQLSeal [sha256.Size]byte

// compiledReadScope is immutable compiler evidence that SQL reads exactly the
// listed tenant/index scope. Its digest binds both the SQL structure and scope,
// so neither may be copied or changed independently after compilation.
type compiledReadScope struct {
	tenantID          string
	indexNames        []string
	argumentPositions []int
	seal              compiledSQLSeal
}

type compiledReadScopeArgument struct {
	ordinal int
	value   string
}

func (scope compiledReadScope) sealedForSQL(sql string) compiledReadScope {
	scope.seal = compiledReadScopeDigest(
		sql,
		scope.tenantID,
		scope.indexNames,
		scope.argumentPositions,
	)
	return scope
}

func (scope compiledReadScope) openForSQL(sql string, args []any) (string, []string, bool) {
	if !scope.hasValidSealForSQL(sql) {
		return "", nil, false
	}
	// The compiler tags tenant/index bind values before lowering, then records
	// their final positions after outer operators have prepended their own
	// placeholders. Validate those exact positions in the current public Args
	// slice before exposing the scope, preventing callers from retaining a valid
	// structural seal while redirecting the physical read through altered bind
	// values.
	tenantPosition := scope.argumentPositions[0]
	if tenantPosition < 0 || tenantPosition >= len(args) {
		return "", nil, false
	}
	boundTenantID, ok := args[tenantPosition].(string)
	if !ok || boundTenantID != scope.tenantID {
		return "", nil, false
	}
	for index, indexName := range scope.indexNames {
		position := scope.argumentPositions[index+1]
		if position < 0 || position >= len(args) {
			return "", nil, false
		}
		boundIndexName, ok := args[position].(string)
		if !ok || boundIndexName != indexName {
			return "", nil, false
		}
	}
	return scope.tenantID, slices.Clone(scope.indexNames), true
}

func (scope compiledReadScope) hasValidSealForSQL(sql string) bool {
	if scope.tenantID == "" || len(scope.indexNames) == 0 {
		return false
	}
	if slices.Contains(scope.indexNames, "") {
		return false
	}
	if len(scope.argumentPositions) != len(scope.indexNames)+1 {
		return false
	}
	previousPosition := -1
	for _, position := range scope.argumentPositions {
		if position <= previousPosition {
			return false
		}
		previousPosition = position
	}
	expected := compiledReadScopeDigest(
		sql,
		scope.tenantID,
		scope.indexNames,
		scope.argumentPositions,
	)
	return subtle.ConstantTimeCompare(expected[:], scope.seal[:]) == 1
}

func compiledReadScopeDigest(
	sql, tenantID string,
	indexNames []string,
	argumentPositions []int,
) compiledSQLSeal {
	digest := sha256.New()
	writeTokenPart(digest, "open-splunk-compiled-read-scope-v2")
	writeTokenPart(digest, sql)
	writeTokenPart(digest, tenantID)
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(indexNames)))
	_, _ = digest.Write(count[:])
	for _, indexName := range indexNames {
		writeTokenPart(digest, indexName)
	}
	binary.BigEndian.PutUint64(count[:], uint64(len(argumentPositions)))
	_, _ = digest.Write(count[:])
	for _, position := range argumentPositions {
		// Every digest caller first proves that positions are nonnegative and
		// strictly increasing; a nonnegative int is representable as uint64.
		//nolint:gosec // The checked compiler position cannot overflow uint64.
		binary.BigEndian.PutUint64(count[:], uint64(position))
		_, _ = digest.Write(count[:])
	}
	var result compiledSQLSeal
	digest.Sum(result[:0])
	return result
}

func sealCompiledQuerySQL(compiled CompiledQuery) CompiledQuery {
	compiled.readScope = compiled.readScope.sealedForSQL(compiled.SQL)
	return compiled
}

func sealCompiledQueryReadScope(
	compiled CompiledQuery,
	tenantID string,
	indexNames []string,
) (CompiledQuery, error) {
	if tenantID == "" || len(indexNames) == 0 {
		return CompiledQuery{}, errors.New("seal compiled ClickHouse read scope: tenant and indexes are required")
	}
	if slices.Contains(indexNames, "") {
		return CompiledQuery{}, errors.New("seal compiled ClickHouse read scope: index names must be nonempty")
	}

	arguments := slices.Clone(compiled.Args)
	positions := make([]int, len(indexNames)+1)
	for index := range positions {
		positions[index] = -1
	}
	for position, argument := range arguments {
		scopeArgument, ok := argument.(compiledReadScopeArgument)
		if !ok {
			continue
		}
		if scopeArgument.ordinal < 0 || scopeArgument.ordinal >= len(positions) {
			return CompiledQuery{}, fmt.Errorf(
				"seal compiled ClickHouse read scope: marker ordinal %d is out of range",
				scopeArgument.ordinal,
			)
		}
		expected := tenantID
		if scopeArgument.ordinal > 0 {
			expected = indexNames[scopeArgument.ordinal-1]
		}
		if scopeArgument.value != expected {
			return CompiledQuery{}, fmt.Errorf(
				"seal compiled ClickHouse read scope: marker ordinal %d has an unexpected value",
				scopeArgument.ordinal,
			)
		}
		if positions[scopeArgument.ordinal] != -1 {
			return CompiledQuery{}, fmt.Errorf(
				"seal compiled ClickHouse read scope: marker ordinal %d is duplicated",
				scopeArgument.ordinal,
			)
		}
		arguments[position] = scopeArgument.value
		positions[scopeArgument.ordinal] = position
	}
	previousPosition := -1
	for ordinal, position := range positions {
		if position < 0 {
			return CompiledQuery{}, fmt.Errorf(
				"seal compiled ClickHouse read scope: marker ordinal %d is missing",
				ordinal,
			)
		}
		if position <= previousPosition {
			return CompiledQuery{}, errors.New(
				"seal compiled ClickHouse read scope: markers are out of order",
			)
		}
		previousPosition = position
	}
	compiled.Args = arguments
	compiled.readScope = compiledReadScope{
		tenantID:          tenantID,
		indexNames:        slices.Clone(indexNames),
		argumentPositions: positions,
	}
	return sealCompiledQuerySQL(compiled), nil
}

// HasValidSQLSeal reports whether SQL and its private read scope are unchanged
// outputs of Compiler. Args remain mutable so callers can retain typed driver
// binding without serializing values into SQL.
func (compiled CompiledQuery) HasValidSQLSeal() bool {
	return compiled.readScope.hasValidSealForSQL(compiled.SQL)
}

// ReadScope returns a detached tenant/index scope only when it is still bound
// to this exact compiler-produced SQL text.
func (compiled CompiledQuery) ReadScope() (string, []string, bool) {
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}

// ReadScope returns a detached tenant/index scope only when it is still bound
// to this exact compiler-produced timeline SQL text.
func (compiled CompiledTimeline) ReadScope() (string, []string, bool) {
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}

// ReadScope returns a detached tenant/index scope only when it is still bound
// to this exact compiler-produced field-catalog SQL text.
func (compiled CompiledFieldCatalog) ReadScope() (string, []string, bool) {
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}

// ReadScope returns a detached tenant/index scope only when it is still bound
// to this exact compiler-produced field-summary SQL text.
func (compiled CompiledFieldSummary) ReadScope() (string, []string, bool) {
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}

// ReadScope returns a detached tenant/index scope only when it is still bound
// to this exact compiler-produced field-suggestion SQL text.
func (compiled CompiledFieldSuggestions) ReadScope() (string, []string, bool) {
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}
