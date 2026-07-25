package clickhouse

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// maximumCompiledRelationalDepth bounds the longest generated SELECT/UNION
// dependency chain. It complements the SQL byte ceiling: a short query can
// still make ClickHouse's analyzer recurse deeply when each stage wraps the
// previous relation.
const maximumCompiledRelationalDepth = 96

// compiledRelation keeps structural complexity beside the SQL fragment that
// owns it. A SELECT or UNION contributes one level above the deepest relation
// or relational subquery it consumes. WITH declarations and materialization
// do not reset that dependency chain.
type compiledRelation struct {
	sql        string
	depth      int
	ownerRange spl.Range
}

func newScanRelation(sql string, ownerRange spl.Range) compiledRelation {
	return compiledRelation{sql: sql, depth: 1, ownerRange: ownerRange}
}

// selectFrom records one generated SELECT whose deepest relational input is
// the receiver. The supplied SQL must be the complete new relation.
func (relation compiledRelation) selectFrom(sql string, ownerRange spl.Range) compiledRelation {
	return compiledRelation{
		sql:        sql,
		depth:      relation.depth + 1,
		ownerRange: ownerRange,
	}
}

// relationalNodeDepth returns the height of one SELECT or UNION above all of
// its relational inputs. An input-free SELECT has depth one.
func relationalNodeDepth(inputs ...int) int {
	maximum := 0
	for _, depth := range inputs {
		if depth > maximum {
			maximum = depth
		}
	}
	return maximum + 1
}

func validateRelationalDepth(depth int, ownerRange spl.Range) error {
	if depth <= 0 {
		return errors.New("compile ClickHouse query: relational depth was not reported")
	}
	if depth <= maximumCompiledRelationalDepth {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("compiled query exceeds %d relational levels", maximumCompiledRelationalDepth),
		Range:   ownerRange,
	}
}

func withCompiledRelationalDepth(
	compiled CompiledQuery,
	depth int,
	ownerRange spl.Range,
) CompiledQuery {
	compiled.relationalDepth = depth
	compiled.relationalDepthRange = ownerRange
	return compiled
}

func validateCompiledRelationalDepth(compiled CompiledQuery) error {
	return validateRelationalDepth(compiled.relationalDepth, compiled.relationalDepthRange)
}

func validateFinalizedRelationalDepth(input compiledRelation, compiled CompiledQuery) error {
	if err := validateCompiledRelationalDepth(compiled); err != nil {
		return err
	}
	if compiled.relationalDepth <= input.depth {
		return errors.New(
			"compile ClickHouse query: finalizer did not report a relational wrapper above its input",
		)
	}
	return nil
}
