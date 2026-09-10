package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// TimechartWorkRowsColumn is a private cumulative expansion receipt, independent
// of aggregate cells, row occupancy, public schema, and bucket provenance.
const TimechartWorkRowsColumn = "__os_timechart_mvexpand_rows"
const timechartObservedWork = "timechart_observed_work"
const timechartCarriedWork = "__os_mvexpand_carried_rows"

// HasTimechartWorkReceipt declares the sealed final native transport column.
func (compiled CompiledQuery) HasTimechartWorkReceipt() bool { return compiled.timechartWorkReceipt }

// TimechartWorkFloor is the validated work inherited from completed stages.
func (compiled CompiledQuery) TimechartWorkFloor() uint64 { return compiled.timechartWorkFloor }

type timechartWorkInput struct{ counter string }

func prepareTimechartWorkInput(state compileState) *timechartWorkInput {
	if state.context.mvExpandWorkSQL == "" {
		return nil
	}
	return &timechartWorkInput{counter: state.context.mvExpandWorkSQL}
}

func (capture *timechartWorkInput) wrap(compiled CompiledQuery) (CompiledQuery, error) {
	if capture == nil {
		return compiled, nil
	}
	compiled.SQL = applyMaterializedValidationSettings("SELECT *, " + capture.counter + " AS " + quoteIdentifier(TimechartWorkRowsColumn) + " FROM (" + strings.TrimSuffix(compiled.SQL, materializedCTESettingsSQL) + ")")
	compiled.relationalDepth++
	compiled.timechartWorkReceipt = true
	if err := validateCompiledRelationalDepth(compiled); err != nil {
		return CompiledQuery{}, err
	}
	return compiled, nil
}

// Hoist the already validated expansion only when a later timechart needs its
// query-wide receipt. The same MATERIALIZED barrier feeds downstream rows and
// the scalar summary, so filtering every row cannot erase charged work.
func retainTimechartExpansionWork(relation compiledRelation, state compileState, args []any, operator *plan.ExpandMultivalue, later []plan.Operator, stage int) (compiledRelation, compileState, []any) {
	needed := false
	for _, candidate := range later {
		if _, ok := candidate.(*plan.Timechart); ok {
			needed = true
			break
		}
	}
	if !needed {
		return relation, state, args
	}
	name := quoteIdentifier(fmt.Sprintf("__os_timechart_expansion_%d", stage))
	invalid := quoteIdentifier(fmt.Sprintf("__os_timechart_expansion_invalid_%d", stage))
	barrier := &pendingChronologicalBarrier{name: name, sql: "SELECT *, toUInt8(0) AS " + invalid + " FROM (" + relation.sql + ")", validationColumns: []string{invalid}, fanout: 3, depth: relation.depth + 1, ownerRange: operator.Range}
	state, args = bindChronologicalBarrier(state, barrier, args)
	receipt := "(SELECT maxOrDefault(" + state.mvExpandQueryRowsSQL + ") FROM " + name + ")"
	if state.context.mvExpandWorkSQL != "" {
		receipt = "greatest(" + state.context.mvExpandWorkSQL + ", " + receipt + ")"
	}
	state.context.mvExpandWorkSQL = "assumeNotNull(" + receipt + ")"
	relation = compiledRelation{sql: "SELECT * EXCEPT (" + invalid + ") FROM " + name, depth: relation.depth + 2, ownerRange: operator.Range}
	return relation, state, args
}

func validateTimechartWork(work, floor uint64) error {
	if work > plan.MaximumMVExpandRowsPerQuery {
		return fmt.Errorf("%w: cumulative expansion work exceeds query limit", ErrTimechartResourceLimit)
	}
	if work < floor {
		return errors.New("timechart cumulative expansion work is invalid")
	}
	return nil
}

func carriedTimechartWorkProjection(work uint64) string {
	return "toUInt64(" + strconv.FormatUint(work, 10) + ") AS " + quoteIdentifier(timechartCarriedWork)
}

const timechartObservedInputRow = "timechart_observed_input_row"

func addObservedTimechartWorkHeader(relation compiledRelation, state compileState, scan *plan.Scan) (compiledRelation, compileState, error) {
	if state.context.mvExpandWorkSQL == "" {
		return relation, state, nil
	}
	header := make([]string, len(state.publicOrder))
	columns := make([]string, len(state.publicOrder))
	for i, name := range state.publicOrder {
		columns[i] = quoteIdentifier(name)
		switch name {
		case "_time":
			header[i] = "fromUnixTimestamp64Nano(" + strconv.FormatInt(scan.Earliest.UnixNano(), 10) + ", 'UTC')"
		case timechartObservedMeasure:
			if field := state.visible[name]; field.kind == fieldKindNumber {
				header[i] = "toUInt64(0)"
			} else {
				header[i] = "CAST(NULL AS Dynamic)"
			}
		case timechartObservedSplit:
			header[i] = "CAST(NULL AS Nullable(String))"
		case timechartObservedWork:
			header[i] = state.context.mvExpandWorkSQL
		default:
			return compiledRelation{}, state, errors.New("observed timechart work header has unexpected field")
		}
		header[i] += " AS " + columns[i]
	}
	marker := quoteIdentifier(timechartObservedInputRow)
	relation.sql = "SELECT " + strings.Join(columns, ", ") + ", toUInt64(1) AS " + marker + " FROM (" + strings.TrimSuffix(relation.sql, materializedCTESettingsSQL) + ") UNION ALL SELECT " + strings.Join(header, ", ") + ", toUInt64(0) AS " + marker
	relation.depth++
	state.publicOrder = append(state.publicOrder, timechartObservedInputRow)
	state.visible[timechartObservedInputRow] = fieldState{kind: fieldKindNumber, numberType: "UInt64", valueSQL: marker, existsSQL: "1", numericIntegral: true}
	state.privateColumns = nil
	state.order = []compiledSortKey{{valueSQL: quoteIdentifier("_time")}}
	state.tieBreakers = nil
	return relation, state, nil
}
