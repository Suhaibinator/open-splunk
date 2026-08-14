package clickhouse

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"hash"
	"slices"
	"strings"
)

// v3 additionally binds the exact lookup external-table authority inherited
// from the ordinary compiler source.
//
// v2 additionally binds compiler-owned resource evidence carried by a
// specialized executable. In particular, a field catalog's knowledge field
// count cannot be substituted to select a different executor memory class.
// The seal binds a specialized executable to both the complete compiler authority
// it was derived from and the complete public surface that reaches the driver.
// A distinct kind token prevents equal byte sequences from crossing result
// contracts.
const derivedExecutionSealDomain = "open-splunk-derived-clickhouse-execution-v3"

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
	lookupTables []compiledLookupExternalTable
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
	return sealDerivedExecutionContext(
		context.Background(),
		kind,
		source,
		sql,
		args,
		readScope,
		writeSpec,
	)
}

func sealDerivedExecutionContext(
	ctx context.Context,
	kind derivedExecutionKind,
	source CompiledQuery,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (*derivedExecutionAuthority, error) {
	if ctx == nil {
		return nil, errors.New("seal derived ClickHouse execution: context is nil")
	}
	sourceDigest, ok, err := source.ExecutionAuthorityDigestContext(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("seal derived ClickHouse execution: source authority is invalid")
	}
	authority := &derivedExecutionAuthority{
		sourceDigest: sourceDigest,
		lookupTables: cloneCompiledLookupExternalTables(source.lookupTables),
	}
	seal, ok, err := derivedExecutionDigestContext(
		ctx,
		kind,
		authority,
		sql,
		args,
		readScope,
		writeSpec,
	)
	if err != nil {
		return nil, err
	}
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
	valid, _ := hasValidDerivedExecutionContext(
		context.Background(),
		kind,
		authority,
		sql,
		args,
		readScope,
		writeSpec,
	)
	return valid
}

func hasValidDerivedExecutionContext(
	ctx context.Context,
	kind derivedExecutionKind,
	authority *derivedExecutionAuthority,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("validate derived ClickHouse execution: context is nil")
	}
	if authority == nil {
		return false, nil
	}
	expected, ok, err := derivedExecutionDigestContext(
		ctx,
		kind,
		authority,
		sql,
		args,
		readScope,
		writeSpec,
	)
	if err != nil {
		return false, err
	}
	return ok && subtle.ConstantTimeCompare(expected[:], authority.seal[:]) == 1, nil
}

func derivedExecutionDigest(
	kind derivedExecutionKind,
	authority *derivedExecutionAuthority,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (derivedExecutionSeal, bool) {
	digest, ok, _ := derivedExecutionDigestContext(
		context.Background(),
		kind,
		authority,
		sql,
		args,
		readScope,
		writeSpec,
	)
	return digest, ok
}

func derivedExecutionDigestContext(
	ctx context.Context,
	kind derivedExecutionKind,
	authority *derivedExecutionAuthority,
	sql string,
	args []any,
	readScope compiledReadScope,
	writeSpec derivedExecutionSpecWriter,
) (derivedExecutionSeal, bool, error) {
	if ctx == nil {
		return derivedExecutionSeal{}, false, errors.New(
			"hash derived ClickHouse execution: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return derivedExecutionSeal{}, false, err
	}
	if authority == nil || kind == "" || writeSpec == nil {
		return derivedExecutionSeal{}, false, nil
	}
	referenced, err := compiledLookupExternalTablesReferencedContext(
		ctx,
		sql,
		authority.lookupTables,
	)
	if err != nil {
		return derivedExecutionSeal{}, false, err
	}
	if !referenced {
		return derivedExecutionSeal{}, false, nil
	}
	if _, _, ok := readScope.openForSQL(sql, args); !ok {
		return derivedExecutionSeal{}, false, nil
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
			return derivedExecutionSeal{}, false, nil
		}
	}
	written, err := writeCompiledLookupExternalTablesContext(
		ctx,
		digest,
		authority.lookupTables,
	)
	if err != nil {
		return derivedExecutionSeal{}, false, err
	}
	if !written {
		return derivedExecutionSeal{}, false, nil
	}
	if !writeSpec(digest) {
		return derivedExecutionSeal{}, false, nil
	}
	_, _ = digest.Write(readScope.seal[:])
	var result derivedExecutionSeal
	digest.Sum(result[:0])
	return result, true, nil
}

func cloneDerivedExecutionSurface(
	sql string,
	args []any,
	readScope compiledReadScope,
	authority *derivedExecutionAuthority,
) (string, []any, compiledReadScope, *derivedExecutionAuthority, bool) {
	sql, clonedArgs, clonedScope, clonedAuthority, ok, _ :=
		cloneDerivedExecutionSurfaceContext(
			context.Background(),
			sql,
			args,
			readScope,
			authority,
		)
	return sql, clonedArgs, clonedScope, clonedAuthority, ok
}

func cloneDerivedExecutionSurfaceContext(
	ctx context.Context,
	sql string,
	args []any,
	readScope compiledReadScope,
	authority *derivedExecutionAuthority,
) (string, []any, compiledReadScope, *derivedExecutionAuthority, bool, error) {
	if ctx == nil {
		return "", nil, compiledReadScope{}, nil, false, errors.New(
			"clone derived ClickHouse execution: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return "", nil, compiledReadScope{}, nil, false, err
	}
	if authority == nil {
		return "", nil, compiledReadScope{}, nil, false, nil
	}
	clonedArgs := make([]any, len(args))
	if args == nil {
		clonedArgs = nil
	} else {
		for index, argument := range args {
			if err := ctx.Err(); err != nil {
				return "", nil, compiledReadScope{}, nil, false, err
			}
			cloned, ok := cloneCompiledArgument(argument)
			if !ok {
				return "", nil, compiledReadScope{}, nil, false, nil
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
	clonedAuthority.lookupTables = cloneCompiledLookupExternalTables(
		authority.lookupTables,
	)
	return strings.Clone(sql), clonedArgs, clonedScope, &clonedAuthority, true, nil
}

func writeTimelineSpec(writer hash.Hash, spec TimelineSpec) bool {
	return writeTime(writer, spec.FirstBucket) &&
		writeTimelineSpecRemainder(writer, spec)
}

func writeFieldCatalogSpec(
	writer hash.Hash,
	spec FieldCatalogSpec,
	knowledgeGeneratedFields uint32,
) bool {
	if knowledgeGeneratedFields > MaximumClickHouseKnowledgeGeneratedFields {
		return false
	}
	writeUint64(writer, uint64(spec.MaximumFields))
	writeUint64(writer, uint64(knowledgeGeneratedFields))
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
	return sealCompiledTimelineExecutionContext(
		context.Background(),
		source,
		compiled,
	)
}

func sealCompiledTimelineExecutionContext(
	ctx context.Context,
	source CompiledQuery,
	compiled CompiledTimeline,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecutionContext(
		ctx,
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
	return sealCompiledFieldCatalogExecutionContext(
		context.Background(),
		source,
		compiled,
	)
}

func sealCompiledFieldCatalogExecutionContext(
	ctx context.Context,
	source CompiledQuery,
	compiled CompiledFieldCatalog,
) (*derivedExecutionAuthority, error) {
	knowledgeGeneratedFields, ok, err :=
		fieldCatalogKnowledgeGeneratedFieldsFromSourceContext(ctx, source)
	if err != nil {
		return nil, err
	}
	if !ok || compiled.knowledgeGeneratedFields != knowledgeGeneratedFields {
		return nil, errors.New(
			"seal derived ClickHouse field catalog execution: generated-field resource evidence is invalid",
		)
	}
	return sealDerivedExecutionContext(
		ctx,
		derivedExecutionFieldCatalog,
		source,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool {
			return writeFieldCatalogSpec(
				writer,
				compiled.Spec,
				compiled.knowledgeGeneratedFields,
			)
		},
	)
}

func sealCompiledFieldSummaryExecution(
	source CompiledQuery,
	compiled CompiledFieldSummary,
) (*derivedExecutionAuthority, error) {
	return sealCompiledFieldSummaryExecutionContext(
		context.Background(),
		source,
		compiled,
	)
}

func sealCompiledFieldSummaryExecutionContext(
	ctx context.Context,
	source CompiledQuery,
	compiled CompiledFieldSummary,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecutionContext(
		ctx,
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
	return sealCompiledFieldSuggestionsExecutionContext(
		context.Background(),
		source,
		compiled,
	)
}

func sealCompiledFieldSuggestionsExecutionContext(
	ctx context.Context,
	source CompiledQuery,
	compiled CompiledFieldSuggestions,
) (*derivedExecutionAuthority, error) {
	return sealDerivedExecutionContext(
		ctx,
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
	valid, _ := compiled.hasValidExecutionSealContext(context.Background())
	return valid
}

func (compiled CompiledTimeline) hasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return hasValidDerivedExecutionContext(
		ctx,
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

func (compiled CompiledTimeline) HasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return compiled.hasValidExecutionSealContext(ctx)
}

// CloneForExecution validates and deeply detaches the complete timeline
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledTimeline) CloneForExecution() (CompiledTimeline, bool) {
	cloned, ok, _ := compiled.CloneForExecutionContext(context.Background())
	return cloned, ok
}

func (compiled CompiledTimeline) CloneForExecutionContext(
	ctx context.Context,
) (CompiledTimeline, bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledTimeline{}, false, err
	}
	if !valid {
		return CompiledTimeline{}, false, nil
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok, err =
		cloneDerivedExecutionSurfaceContext(
			ctx,
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	if err != nil {
		return CompiledTimeline{}, false, err
	}
	valid, err = cloned.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledTimeline{}, false, err
	}
	if !ok || !valid {
		return CompiledTimeline{}, false, nil
	}
	return cloned, true, nil
}

func (compiled CompiledFieldCatalog) hasValidExecutionSeal() bool {
	valid, _ := compiled.hasValidExecutionSealContext(context.Background())
	return valid
}

func (compiled CompiledFieldCatalog) hasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return hasValidDerivedExecutionContext(
		ctx,
		derivedExecutionFieldCatalog,
		compiled.executionAuthority,
		compiled.SQL,
		compiled.Args,
		compiled.readScope,
		func(writer hash.Hash) bool {
			return writeFieldCatalogSpec(
				writer,
				compiled.Spec,
				compiled.knowledgeGeneratedFields,
			)
		},
	)
}

// HasValidExecutionSeal reports whether the catalog remains the exact
// compiler-produced executable and result contract.
func (compiled CompiledFieldCatalog) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

func (compiled CompiledFieldCatalog) HasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return compiled.hasValidExecutionSealContext(ctx)
}

// CloneForExecution validates and deeply detaches the complete catalog
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldCatalog) CloneForExecution() (CompiledFieldCatalog, bool) {
	cloned, ok, _ := compiled.CloneForExecutionContext(context.Background())
	return cloned, ok
}

func (compiled CompiledFieldCatalog) CloneForExecutionContext(
	ctx context.Context,
) (CompiledFieldCatalog, bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldCatalog{}, false, err
	}
	if !valid {
		return CompiledFieldCatalog{}, false, nil
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok, err =
		cloneDerivedExecutionSurfaceContext(
			ctx,
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	if err != nil {
		return CompiledFieldCatalog{}, false, err
	}
	valid, err = cloned.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldCatalog{}, false, err
	}
	if !ok || !valid {
		return CompiledFieldCatalog{}, false, nil
	}
	return cloned, true, nil
}

func (compiled CompiledFieldSummary) hasValidExecutionSeal() bool {
	valid, _ := compiled.hasValidExecutionSealContext(context.Background())
	return valid
}

func (compiled CompiledFieldSummary) hasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return hasValidDerivedExecutionContext(
		ctx,
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

func (compiled CompiledFieldSummary) HasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return compiled.hasValidExecutionSealContext(ctx)
}

// CloneForExecution validates and deeply detaches the complete summary
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldSummary) CloneForExecution() (CompiledFieldSummary, bool) {
	cloned, ok, _ := compiled.CloneForExecutionContext(context.Background())
	return cloned, ok
}

func (compiled CompiledFieldSummary) CloneForExecutionContext(
	ctx context.Context,
) (CompiledFieldSummary, bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldSummary{}, false, err
	}
	if !valid {
		return CompiledFieldSummary{}, false, nil
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok, err =
		cloneDerivedExecutionSurfaceContext(
			ctx,
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	cloned.Spec.FieldName = strings.Clone(compiled.Spec.FieldName)
	if err != nil {
		return CompiledFieldSummary{}, false, err
	}
	valid, err = cloned.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldSummary{}, false, err
	}
	if !ok || !valid {
		return CompiledFieldSummary{}, false, nil
	}
	return cloned, true, nil
}

func (compiled CompiledFieldSuggestions) hasValidExecutionSeal() bool {
	valid, _ := compiled.hasValidExecutionSealContext(context.Background())
	return valid
}

func (compiled CompiledFieldSuggestions) hasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return hasValidDerivedExecutionContext(
		ctx,
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

func (compiled CompiledFieldSuggestions) HasValidExecutionSealContext(
	ctx context.Context,
) (bool, error) {
	return compiled.hasValidExecutionSealContext(ctx)
}

// CloneForExecution validates and deeply detaches the complete suggestion
// executable. Invalid or hand-constructed values are never repaired.
func (compiled CompiledFieldSuggestions) CloneForExecution() (CompiledFieldSuggestions, bool) {
	cloned, ok, _ := compiled.CloneForExecutionContext(context.Background())
	return cloned, ok
}

func (compiled CompiledFieldSuggestions) CloneForExecutionContext(
	ctx context.Context,
) (CompiledFieldSuggestions, bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldSuggestions{}, false, err
	}
	if !valid {
		return CompiledFieldSuggestions{}, false, nil
	}
	cloned := compiled
	var ok bool
	cloned.SQL, cloned.Args, cloned.readScope, cloned.executionAuthority, ok, err =
		cloneDerivedExecutionSurfaceContext(
			ctx,
			compiled.SQL,
			compiled.Args,
			compiled.readScope,
			compiled.executionAuthority,
		)
	cloned.Spec.Prefix = strings.Clone(compiled.Spec.Prefix)
	if err != nil {
		return CompiledFieldSuggestions{}, false, err
	}
	valid, err = cloned.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledFieldSuggestions{}, false, err
	}
	if !ok || !valid {
		return CompiledFieldSuggestions{}, false, nil
	}
	return cloned, true, nil
}
