package clickhouse

import (
	"hash"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// continuationBudget carries cumulative compilation charges without retaining
// prior AST caches, regex programs, lookup cells, or an execution context.
type continuationBudget struct {
	extraction                                                                                 authoredKnowledgeCompilation
	matchWork, likeWork, strftimeWork, strptimeWork, relativeWork, relativeOperations          int
	arithmetic, membership, concatOperands                                                     int
	mvExpand                                                                                   uint8
	likeBytes, strftimeBytes, strptimeBytes, unixBytes, concatBytes, stringBytes, replaceBytes uint64
}

func timechartContinuationBudget(source *compileContext) continuationBudget {
	return continuationBudget{
		extraction: source.extractionBudget,
		matchWork:  source.patternBudgets.match.programWorkUnits, likeWork: source.patternBudgets.like.workUnits,
		strftimeWork: source.strftimeBudget.workUnits, strptimeWork: source.strptimeBudget.workUnits,
		relativeWork: source.relativeTimeBudget.workUnits, relativeOperations: source.relativeTimeBudget.operations,
		arithmetic: source.arithmeticOperators, membership: source.membershipCandidates, concatOperands: source.concatenationBudget.operands,
		mvExpand: source.mvExpandStages, likeBytes: source.patternBudgets.like.inputBytes, strftimeBytes: source.strftimeBudget.outputBytes,
		strptimeBytes: source.strptimeBudget.inputBytes, unixBytes: source.unixTimestampBudget.dynamicDecimalBytes,
		concatBytes: source.concatenationBudget.outputBytes, stringBytes: source.stringConversionBudget.dynamicDecimalBytes, replaceBytes: source.replaceOutputBytes,
	}
}
func (budget continuationBudget) apply(target *compileContext) {
	target.extractionBudget = budget.extraction
	target.patternBudgets.match.programWorkUnits = budget.matchWork
	target.patternBudgets.like.workUnits = budget.likeWork
	target.strftimeBudget.workUnits = budget.strftimeWork
	target.strptimeBudget.workUnits = budget.strptimeWork
	target.relativeTimeBudget.workUnits = budget.relativeWork
	target.relativeTimeBudget.operations = budget.relativeOperations
	target.arithmeticOperators = budget.arithmetic
	target.membershipCandidates = budget.membership
	target.concatenationBudget.operands = budget.concatOperands
	target.mvExpandStages = budget.mvExpand
	target.patternBudgets.like.inputBytes = budget.likeBytes
	target.strftimeBudget.outputBytes = budget.strftimeBytes
	target.strptimeBudget.inputBytes = budget.strptimeBytes
	target.unixTimestampBudget.dynamicDecimalBytes = budget.unixBytes
	target.concatenationBudget.outputBytes = budget.concatBytes
	target.stringConversionBudget.dynamicDecimalBytes = budget.stringBytes
	target.replaceOutputBytes = budget.replaceBytes
}
func (budget continuationBudget) write(digest hash.Hash) {
	budget.extraction.write(digest)
	for _, value := range []int{budget.matchWork, budget.likeWork, budget.strftimeWork, budget.strptimeWork, budget.relativeWork, budget.relativeOperations, budget.arithmetic, budget.membership, budget.concatOperands} {
		writeInt64(digest, int64(value))
	}
	for _, value := range []uint64{uint64(budget.mvExpand), budget.likeBytes, budget.strftimeBytes, budget.strptimeBytes, budget.unixBytes, budget.concatBytes, budget.stringBytes, budget.replaceBytes} {
		writeUint64(digest, value)
	}
}

// These are logical query charges: aggregation and materialization do not reset
// them. Physical SQL nesting, AST depth and generated SQL bytes are checked for
// each executable because a closed external relation starts a new SQL tree.
func accumulatedTimechartExtractionBudget(operators []plan.Operator, previous authoredKnowledgeCompilation, program knowledgeprogram.Charges) (authoredKnowledgeCompilation, error) {
	if err := validateSharedKnowledgeCompilationBudgets(knowledgeprogram.Charges{}, previous, 0); err != nil {
		return authoredKnowledgeCompilation{}, err
	}
	budget, err := validateCompiledExtractionBudgetsWithPrior(operators, previous)
	if err != nil {
		return authoredKnowledgeCompilation{}, err
	}
	if err := validateSharedKnowledgeCompilationBudgets(program, budget, 0); err != nil {
		return authoredKnowledgeCompilation{}, err
	}
	// The preceding shared checks prove these additions fit their query caps.
	budget.regexPrograms += program.RegexPrograms
	budget.regexWorkUnits += program.RegexWorkUnits
	budget.extractionOutputs += program.ExtractionOutputs
	budget.jsonEvaluationWork += program.JSONEvaluationWork
	return budget, nil
}

func (budget authoredKnowledgeCompilation) write(digest hash.Hash) {
	for _, value := range []uint64{uint64(budget.regexPrograms), budget.regexWorkUnits, uint64(budget.extractionOutputs), uint64(budget.jsonEvaluationWork), budget.matchStyleWorkUnits} {
		writeUint64(digest, value)
	}
}
