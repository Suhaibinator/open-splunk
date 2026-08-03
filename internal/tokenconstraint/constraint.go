// Package tokenconstraint validates and canonicalizes bounded ingestion-token
// event constraints. Patterns retain their exact bytes: normalization only
// removes exact duplicates and orders the remaining values lexically.
package tokenconstraint

import (
	"fmt"
	"regexp/syntax"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaximumPatternsPerDimension  = 16
	MaximumPatternBytes          = 512
	MaximumDimensionBytes        = 4096
	MaximumPatternInstructions   = 4096
	MaximumDimensionInstructions = 16_384
)

// Normalize validates one host or source dimension, removes exact duplicate
// patterns, and returns a detached lexical projection.
func Normalize(patterns []string) ([]string, error) {
	mapCapacity := len(patterns)
	if mapCapacity > MaximumPatternsPerDimension+1 {
		mapCapacity = MaximumPatternsPerDimension + 1
	}
	unique := make(map[string]struct{}, mapCapacity)
	for _, pattern := range patterns {
		if _, duplicate := unique[pattern]; duplicate {
			continue
		}
		unique[pattern] = struct{}{}
		if len(unique) > MaximumPatternsPerDimension {
			return nil, fmt.Errorf(
				"constraint pattern count exceeds the maximum of %d",
				MaximumPatternsPerDimension,
			)
		}
	}
	normalized := make([]string, 0, len(unique))
	for pattern := range unique {
		normalized = append(normalized, pattern)
	}
	sort.Strings(normalized)
	if err := validate(normalized, false); err != nil {
		return nil, err
	}
	return normalized, nil
}

// ValidateNormalized validates a persisted or already-normalized dimension.
// It additionally requires strict lexical order, which rejects duplicates and
// makes ordinal projections deterministic.
func ValidateNormalized(patterns []string) error {
	return validate(patterns, true)
}

func validate(patterns []string, requireNormalized bool) error {
	if len(patterns) > MaximumPatternsPerDimension {
		return fmt.Errorf(
			"constraint pattern count exceeds the maximum of %d",
			MaximumPatternsPerDimension,
		)
	}
	totalBytes := 0
	totalInstructions := 0
	for index, pattern := range patterns {
		if requireNormalized && index > 0 && patterns[index-1] >= pattern {
			return fmt.Errorf("constraint patterns are not in strict lexical order")
		}
		if len(pattern) == 0 || len(pattern) > MaximumPatternBytes ||
			!utf8.ValidString(pattern) || strings.IndexByte(pattern, 0) >= 0 {
			return fmt.Errorf(
				"constraint pattern must contain between 1 and %d valid UTF-8 bytes without NUL",
				MaximumPatternBytes,
			)
		}
		totalBytes += len(pattern)
		if totalBytes > MaximumDimensionBytes {
			return fmt.Errorf(
				"constraint pattern bytes exceed the per-dimension maximum of %d",
				MaximumDimensionBytes,
			)
		}
		instructions, err := compiledInstructionCount(pattern)
		if err != nil {
			return fmt.Errorf("invalid RE2 constraint pattern: %w", err)
		}
		if instructions > MaximumPatternInstructions {
			return fmt.Errorf(
				"constraint pattern program exceeds the maximum of %d instructions",
				MaximumPatternInstructions,
			)
		}
		totalInstructions += instructions
		if totalInstructions > MaximumDimensionInstructions {
			return fmt.Errorf(
				"constraint pattern programs exceed the per-dimension maximum of %d instructions",
				MaximumDimensionInstructions,
			)
		}
	}
	return nil
}

func compiledInstructionCount(pattern string) (int, error) {
	expression, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return 0, err
	}
	program, err := syntax.Compile(expression.Simplify())
	if err != nil {
		return 0, err
	}
	return len(program.Inst), nil
}
