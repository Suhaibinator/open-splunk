package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"hash"
	"slices"
	"strings"
)

// v1 binds a specialized executable to both the complete compiler authority
// it was derived from and the complete public surface that reaches the driver.
// A distinct kind token prevents equal byte sequences from crossing result
// contracts.
const derivedExecutionSealDomain = "open-splunk-derived-clickhouse-execution-v1"

type derivedExecutionKind string

const (
	derivedExecutionTimeline         derivedExecutionKind = "timeline"
	derivedExecutionFieldCatalog     derivedExecutionKind = "field-catalog"
	derivedExecutionFieldSummary     derivedExecutionKind = "field-summary"
	derivedExecutionFieldSuggestions derivedExecutionKind = "field-suggestions"
)

type derivedExecutionSeal [sha256.Size]byte

// derivedExecutionAuthority is compiler-owned provenance for an executable
// projection. sourceDigest commits to all parser-owned compilation evidence;
// seal additionally commits to the specialized SQL, typed arguments, result
// contract, and final read scope.
type derivedExecutionAuthority struct {
	sourceDigest [sha256.Size]byte
	seal         derivedExecutionSeal
}

type derivedExecutionSpecWriter func(hash.Hash) bool

func sealDerivedExecution(
	kind derivedExecutionKind,
	source CompiledQuery,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (*derivedExecutionAuthority, error) {
	sourceDigest, ok := source.ExecutionAuthorityDigest()
	if !ok {
		return nil, errors.New("seal derived ClickHouse execution: source authority is invalid")
	}
	authority := &derivedExecutionAuthority{sourceDigest: sourceDigest}
	seal, ok := derivedExecutionDigest(kind, authority, sql, args, readScope, writeSpec)
	if !ok {
		return nil, errors.New("seal derived ClickHouse execution: contract is invalid")
	}
	authority.seal = seal
	return authority, nil
}

func hasValidDerivedExecution(
	kind derivedExecutionKind,
	authority *derivedExecutionAuthority,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) bool {
	if authority == nil {
		return false
	}
	expected, ok := derivedExecutionDigest(kind, authority, sql, args, readScope, writeSpec)
	return ok && subtle.ConstantTimeCompare(expected[:], authority.seal[:]) == 1
}

func derivedExecutionDigest(
	kind derivedExecutionKind,
	authority *derivedExecutionAuthority,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (derivedExecutionSeal, bool) {
	if authority == nil || kind == "" || writeSpec == nil {
		return derivedExecutionSeal{}, false
	}
	if _, _, ok := readScope.openForSQL(sql, args); !ok {
		return derivedExecutionSeal{}, false
	}
	digest := sha256.New()
	writeTokenPart(digest, derivedExecutionSealDomain)
	writeTokenPart(digest, string(kind))
	_, _ = digest.Write(authority.sourceDigest[:])
	writeTokenPart(digest, sql)
	writeBool(digest, args == nil)
	writeUint64(digest, uint64(len(args)))
	for _, argument := range args {
		if !writeCompiledArgument(digest, argument, 0) {
			return derivedExecutionSeal{}, false
		}
	}
	if !writeSpec(digest) {
		return derivedExecutionSeal{}, false
	}
	_, _ = digest.Write(readScope.seal[:])
	var result derivedExecutionSeal
	digest.Sum(result[:0])
	return result, true
}

func cloneDerivedExecutionSurface(
	sql string,
	args []any,
	readScope compiledReadScope,
	authority *derivedExecutionAuthority,
) (string, []any, compiledReadScope, *derivedExecutionAuthority, bool) {
	if authority == nil {
		return "", nil, compiledReadScope{}, nil, false
	}
	clonedArgs := make([]any, len(args))
	if args == nil {
		clonedArgs = nil
	} else {
		for index, argument := range args {
			cloned, ok := cloneCompiledArgument(argument)
			if !ok {
				return "", nil, compiledReadScope{}, nil, false
			}
			clonedArgs[index] = cloned
		}
	}
	clonedScope := compiledReadScope{
		tenantID:          strings.Clone(readScope.tenantID),
		indexNames:        cloneStrings(readScope.indexNames),
		argumentPositions: slices.Clone(readScope.argumentPositions),
		seal:              readScope.seal,
	}
	clonedAuthority := *authority
	return strings.Clone(sql), clonedArgs, clonedScope, &clonedAuthority, true
}

func writeTimelineSpec(writer hash.Hash, spec TimelineSpec) bool {
	return writeTime(writer, spec.FirstBucket) &&
		writeTimelineSpecRemainder(writer, spec)
}

func writeFieldCatalogSpec(writer hash.Hash, spec FieldCatalogSpec) bool {
	writeUint64(writer, uint64(spec.MaximumFields))
	return true
}

func writeFieldSummarySpec(writer hash.Hash, spec FieldSummarySpec, fieldKnown bool) bool {
	writeTokenPart(writer, spec.FieldName)
	writeUint64(writer, uint64(spec.MaximumValues))
	writeUint64(writer, uint64(spec.MaximumDistinctValues))
	writeUint64(writer, uint64(spec.MaximumValueBytes))
	writeBool(writer, fieldKnown)
	return true
}

func writeFieldSuggestionSpec(writer hash.Hash, spec FieldSuggestionSpec) bool {
	writeTokenPart(writer, spec.Prefix)
	writeUint64(writer, uint64(spec.MaximumFields))
	return true
}

func sealCompiledTimelineExecution(
	source CompiledQuery,
	compiled CompiledTimeline,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecution(
		derivedExecutionTimeline,
		source,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeTimelineSpec(writer, compiled.Spec) },
	)
}

func sealCompiledFieldCatalogExecution(
	source CompiledQuery,
	compiled CompiledFieldCatalog,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecution(
		derivedExecutionFieldCatalog,
		source,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeFieldCatalogSpec(writer, compiled.Spec) },
	)
}

func sealCompiledFieldSummaryExecution(
	source CompiledQuery,
	compiled CompiledFieldSummary,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecution(
		derivedExecutionFieldSummary,
		source,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool {
			return writeFieldSummarySpec(writer, compiled.Spec, compiled.FieldKnown)
		},
	)
}

func sealCompiledFieldSuggestionsExecution(
	source CompiledQuery,
	compiled CompiledFieldSuggestions,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecution(
		derivedExecutionFieldSuggestions,
		source,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeFieldSuggestionSpec(writer, compiled.Spec) },
	)
}

func writeTimelineSpecRemainder(writer hash.Hash, spec TimelineSpec) bool {
	writeInt64(writer, spec.SpanSeconds)
	writeUint64(writer, spec.BucketCount)
	return writeTime(writer, spec.Earliest) && writeTime(writer, spec.Latest)
}

func (compiled CompiledTimeline) hasValidExecutionSeal() bool {
	return hasValidDerivedExecution(
		derivedExecutionTimeline,
		compiled.executionAuthority,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeTimelineSpec(writer, compiled.Spec) },
	)
}

// HasValidExecutionSeal reports whether the timeline remains the exact
// compiler-produced executable and result contract.
func (compiled CompiledTimeline) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

// CloneForExecution validates and deeply detaches the complete timeline
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledTimeline) CloneForExecution() (CompiledTimeline, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledTimeline{}, false
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok =
		cloneDerivedExecutionSurface(
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	if !ok || !cloned.hasValidExecutionSeal() {
		return CompiledTimeline{}, false
	}
	return cloned, true
}

func (compiled CompiledFieldCatalog) hasValidExecutionSeal() bool {
	return hasValidDerivedExecution(
		derivedExecutionFieldCatalog,
		compiled.executionAuthority,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeFieldCatalogSpec(writer, compiled.Spec) },
	)
}

// HasValidExecutionSeal reports whether the catalog remains the exact
// compiler-produced executable and result contract.
func (compiled CompiledFieldCatalog) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

// CloneForExecution validates and deeply detaches the complete catalog
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldCatalog) CloneForExecution() (CompiledFieldCatalog, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledFieldCatalog{}, false
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok =
		cloneDerivedExecutionSurface(
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	if !ok || !cloned.hasValidExecutionSeal() {
		return CompiledFieldCatalog{}, false
	}
	return cloned, true
}

func (compiled CompiledFieldSummary) hasValidExecutionSeal() bool {
	return hasValidDerivedExecution(
		derivedExecutionFieldSummary,
		compiled.executionAuthority,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool {
			return writeFieldSummarySpec(writer, compiled.Spec, compiled.FieldKnown)
		},
	)
}

// HasValidExecutionSeal reports whether the summary remains the exact
// compiler-produced executable and result contract.
func (compiled CompiledFieldSummary) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

// CloneForExecution validates and deeply detaches the complete summary
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldSummary) CloneForExecution() (CompiledFieldSummary, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledFieldSummary{}, false
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok =
		cloneDerivedExecutionSurface(
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	cloned.Spec.FieldName = strings.Clone(compiled.Spec.FieldName)
	if !ok || !cloned.hasValidExecutionSeal() {
		return CompiledFieldSummary{}, false
	}
	return cloned, true
}

func (compiled CompiledFieldSuggestions) hasValidExecutionSeal() bool {
	return hasValidDerivedExecution(
		derivedExecutionFieldSuggestions,
		compiled.executionAuthority,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool { return writeFieldSuggestionSpec(writer, compiled.Spec) },
	)
}

// HasValidExecutionSeal reports whether the suggestions remain the exact
// compiler-produced executable and result contract.
func (compiled CompiledFieldSuggestions) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

// CloneForExecution validates and deeply detaches the complete suggestion
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldSuggestions) CloneForExecution() (CompiledFieldSuggestions, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledFieldSuggestions{}, false
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok =
		cloneDerivedExecutionSurface(
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	cloned.Spec.Prefix = strings.Clone(compiled.Spec.Prefix)
	if !ok || !cloned.hasValidExecutionSeal() {
		return CompiledFieldSuggestions{}, false
	}
	return cloned, true
}
