package clickhouse

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// preparedKnowledgeCompilation is the fail-closed handoff between logical-plan
// admission and physical ClickHouse lowering. prefixLength counts only the
// generated knowledge operators after Scan; authored operators begin at
// query.Operators[1+prefixLength]. A present empty program therefore has a
// zero prefixLength while remaining distinct from a legacy plan.
type preparedKnowledgeCompilation struct {
	present                       bool
	prefixLength                  int
	operatorKinds                 []knowledgeprogram.OperatorKind
	program                       knowledgeprogram.Program
	programCharges                knowledgeprogram.Charges
	programCommitment             [sha256.Size]byte
	authored                      authoredKnowledgeCompilation
	authoredScalarPredicates      uint32
	authoredScalarPredicatesExact bool
}

// prepareKnowledgeCompilation proves the complete logical knowledge prefix
// before any operator is lowered. The retained Program remains the semantic
// authority; the copied charges and commitment are exact convenience values
// for the compiler/evidence boundary and are never independently derived.
func prepareKnowledgeCompilation(query *plan.Query) (preparedKnowledgeCompilation, error) {
	if err := plan.ValidateKnowledgePreludeIntegrity(query); err != nil {
		return preparedKnowledgeCompilation{}, fmt.Errorf(
			"prepare ClickHouse knowledge compilation: %w",
			err,
		)
	}
	if query == nil || len(query.Operators) == 0 {
		return preparedKnowledgeCompilation{}, errors.New(
			"prepare ClickHouse knowledge compilation: logical plan is empty",
		)
	}
	if scan, ok := query.Operators[0].(*plan.Scan); !ok || scan == nil {
		return preparedKnowledgeCompilation{}, errors.New(
			"prepare ClickHouse knowledge compilation: first operator must be a non-nil Scan",
		)
	}

	preparation := preparedKnowledgeCompilation{}
	program, present := query.KnowledgePrelude()
	preparation.present = present
	if present {
		preparation.program = program.Clone()
		preparation.programCharges = program.Charges()
		preparation.operatorKinds = program.OperatorKinds()
		preparation.prefixLength = len(preparation.operatorKinds)
		commitment, ok := program.Commitment()
		if !ok {
			return preparedKnowledgeCompilation{}, errors.New(
				"prepare ClickHouse knowledge compilation: present program has no commitment",
			)
		}
		preparation.programCommitment = commitment
		if uint64(preparation.prefixLength) != uint64(preparation.programCharges.GeneratedOperators) {
			return preparedKnowledgeCompilation{}, errors.New(
				"prepare ClickHouse knowledge compilation: program operator charge disagrees with prefix",
			)
		}
	} else if !program.IsZero() {
		return preparedKnowledgeCompilation{}, errors.New(
			"prepare ClickHouse knowledge compilation: absent marker opened a program",
		)
	}

	authoredStart := 1 + preparation.prefixLength
	if authoredStart < 1 || authoredStart > len(query.Operators) {
		return preparedKnowledgeCompilation{}, errors.New(
			"prepare ClickHouse knowledge compilation: generated prefix exceeds logical plan",
		)
	}
	authored, err := validateCompiledExtractionBudgets(query.Operators[authoredStart:])
	if err != nil {
		return preparedKnowledgeCompilation{}, err
	}
	preparation.authored = authored
	preparation.authoredScalarPredicates, preparation.authoredScalarPredicatesExact =
		query.AuthoredScalarPredicateCount()
	if present && !preparation.authoredScalarPredicatesExact {
		return preparedKnowledgeCompilation{}, errors.New(
			"prepare ClickHouse knowledge compilation: knowledge prelude requires exact parser-owned authored predicate evidence",
		)
	}
	if err := validateSharedKnowledgeCompilationBudgets(
		preparation.programCharges,
		preparation.authored,
		preparation.authoredScalarPredicates,
	); err != nil {
		return preparedKnowledgeCompilation{}, err
	}

	// Keep every caller-owned slice detached even within the package-private
	// compiler handoff.
	preparation.operatorKinds = slices.Clone(preparation.operatorKinds)
	return preparation, nil
}

func validateSharedKnowledgeCompilationBudgets(
	program knowledgeprogram.Charges,
	authored authoredKnowledgeCompilation,
	authoredScalarPredicates uint32,
) error {
	checks := []struct {
		name     string
		program  uint64
		authored uint64
		maximum  uint64
	}{
		{
			name:     "regular-expression programs",
			program:  uint64(program.RegexPrograms),
			authored: uint64(authored.regexPrograms),
			maximum:  uint64(knowledgeprogram.MaximumRegexPrograms),
		},
		{
			name:     "regular-expression work units",
			program:  program.RegexWorkUnits,
			authored: authored.regexWorkUnits,
			maximum:  uint64(knowledgeprogram.MaximumRegexWorkUnits),
		},
		{
			name:     "extraction outputs",
			program:  uint64(program.ExtractionOutputs),
			authored: uint64(authored.extractionOutputs),
			maximum:  uint64(knowledgeprogram.MaximumExtractionOutputs),
		},
		{
			name:     "JSON evaluation work units",
			program:  uint64(program.JSONEvaluationWork),
			authored: uint64(authored.jsonEvaluationWork),
			maximum:  uint64(knowledgeprogram.MaximumJSONEvaluationWork),
		},
		{
			name:     "scalar predicate leaves",
			program:  uint64(program.ScalarPredicates),
			authored: uint64(authoredScalarPredicates),
			maximum:  uint64(knowledgeprogram.MaximumScalarPredicates),
		},
	}
	for _, check := range checks {
		if check.program > check.maximum || check.authored > check.maximum-check.program {
			return &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"knowledge and authored search require more than %d %s in one query",
					check.maximum,
					check.name,
				),
			}
		}
	}
	return nil
}
