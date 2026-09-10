package clickhouse

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"hash"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// eventResultLimitProof authenticates the ordinary finalizer's permission to
// append a LIMIT after its complete deterministic ORDER BY. It has a separate
// domain bound to the canonical execution digest so search-specific execution
// bounds never change retained/export authority or ordinary SQL goldens.
type eventResultLimitProof struct {
	ordinaryEventRows bool
	seal              [sha256.Size]byte
}

func eventResultLimitDigest(sourceDigest [sha256.Size]byte) [sha256.Size]byte {
	digest := sha256.New()
	writeTokenPart(digest, "open-splunk-event-result-limit-proof-v1")
	_, _ = digest.Write(sourceDigest[:])
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func sealEventResultLimitContext(
	ctx context.Context,
	compiled CompiledQuery,
	logical *plan.Query,
	state compileState,
) (CompiledQuery, error) {
	eligible := compiled.eventResultLimit.ordinaryEventRows &&
		state.eventRows && state.context != nil &&
		!state.context.atomicResult &&
		!state.context.requiresMaterializedValidationSettings &&
		len(state.chronologicalBarriers) == 0 &&
		compiled.Chart == nil && compiled.Timechart == nil
	compiled.eventResultLimit = eventResultLimitProof{}
	if !eligible {
		return compiled, nil
	}
	// Keep this whitelist deliberately small. A new event-preserving command
	// does not gain early termination until its complete validation contract is
	// considered, even if it does not currently mark the result atomic.
	for _, operator := range logical.Operators {
		switch operator.(type) {
		case *plan.Scan, *plan.Filter, *plan.RegexFilter, *plan.Project,
			*plan.Sort, *plan.Limit:
		default:
			return compiled, nil
		}
	}
	sourceDigest, valid, err := compiled.ExecutionAuthorityDigestContext(ctx)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !valid {
		return CompiledQuery{}, errors.New("seal event result limit: source authority is invalid")
	}
	compiled.eventResultLimit = eventResultLimitProof{
		ordinaryEventRows: true,
		seal:              eventResultLimitDigest(sourceDigest),
	}
	return compiled, nil
}

// CompiledEventResultLimit is a search-only execution surface derived from an
// unlimited canonical query. MaximumOutputRows includes the caller's overflow
// sentinel; the result sink remains responsible for truncation and validation.
type CompiledEventResultLimit struct {
	SQL               string
	Args              []any
	MaximumOutputRows uint64

	readScope          compiledReadScope
	executionAuthority *derivedExecutionAuthority
}

// CompileEventResultLimitContext adds an execution bound only when the compiler
// proved the ordinary event pipeline permits early termination. An ineligible
// query returns false without changing its canonical authority. The caller must
// use its admitted search limit; exports and analyses keep the unlimited source.
func CompileEventResultLimitContext(
	ctx context.Context,
	source CompiledQuery,
	maximumOutputRows uint64,
) (CompiledEventResultLimit, bool, error) {
	if maximumOutputRows == 0 {
		return CompiledEventResultLimit{}, false, errors.New("compile event result limit: row limit must be positive")
	}
	sourceDigest, valid, err := source.ExecutionAuthorityDigestContext(ctx)
	if err != nil {
		return CompiledEventResultLimit{}, false, err
	}
	if !valid {
		return CompiledEventResultLimit{}, false, errors.New("compile event result limit: source authority is invalid")
	}
	if source.eventResultLimit == (eventResultLimitProof{}) {
		return CompiledEventResultLimit{}, false, nil
	}
	expected := eventResultLimitDigest(sourceDigest)
	if !source.eventResultLimit.ordinaryEventRows ||
		subtle.ConstantTimeCompare(expected[:], source.eventResultLimit.seal[:]) != 1 ||
		source.RequiresAtomicResult() || source.Chart != nil || source.Timechart != nil {
		return CompiledEventResultLimit{}, false, errors.New("compile event result limit: finalizer proof is invalid")
	}
	sql := source.SQL + " LIMIT " + strconv.FormatUint(maximumOutputRows, 10)
	if len(sql) > maxCompiledQueryBytes {
		// An optimization must not make a previously admitted query too large.
		return CompiledEventResultLimit{}, false, nil
	}
	compiled := CompiledEventResultLimit{
		SQL:               sql,
		Args:              source.Args,
		MaximumOutputRows: maximumOutputRows,
		readScope:         source.readScope.sealedForSQL(sql),
	}
	compiled.executionAuthority, err = sealDerivedExecutionContext(
		ctx, derivedExecutionEventResultLimit, source, compiled.SQL, compiled.Args,
		compiled.readScope, compiled.writeSpec,
	)
	if err != nil {
		return CompiledEventResultLimit{}, false, err
	}
	return compiled.CloneForExecutionContext(ctx)
}

func (compiled CompiledEventResultLimit) writeSpec(writer hash.Hash) bool {
	if compiled.MaximumOutputRows == 0 {
		return false
	}
	writeUint64(writer, compiled.MaximumOutputRows)
	return true
}

// HasValidExecutionSeal verifies the complete derived SQL, arguments, source
// execution digest, read scope, and output row ceiling.
func (compiled CompiledEventResultLimit) HasValidExecutionSeal() bool {
	return hasValidDerivedExecution(
		derivedExecutionEventResultLimit, compiled.executionAuthority,
		compiled.SQL, compiled.Args, compiled.readScope, compiled.writeSpec,
	)
}

// CloneForExecutionContext detaches and validates the derived driver surface.
func (compiled CompiledEventResultLimit) CloneForExecutionContext(
	ctx context.Context,
) (CompiledEventResultLimit, bool, error) {
	sql, args, scope, authority, valid, err := cloneDerivedExecutionSurfaceContext(
		ctx, compiled.SQL, compiled.Args, compiled.readScope, compiled.executionAuthority,
	)
	if err != nil || !valid {
		return CompiledEventResultLimit{}, false, err
	}
	cloned := CompiledEventResultLimit{
		SQL: sql, Args: args, MaximumOutputRows: compiled.MaximumOutputRows,
		readScope: scope, executionAuthority: authority,
	}
	valid, err = hasValidDerivedExecutionContext(
		ctx, derivedExecutionEventResultLimit, cloned.executionAuthority,
		cloned.SQL, cloned.Args, cloned.readScope, cloned.writeSpec,
	)
	if err != nil || !valid {
		return CompiledEventResultLimit{}, false, err
	}
	return cloned, true, nil
}
