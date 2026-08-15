package knowledge

import (
	"slices"
	"unicode/utf8"
)

// SelectorImplies reports whether the source selector is conservatively
// proven to be a subset of target. The proof is independent for every
// dimension: an unrestricted target accepts anything; an unrestricted source
// cannot imply a constrained target (including `*`, because constrained
// dimensions reject missing/null metadata); a universal `*` target accepts
// every constrained source; source literals must match a target pattern; and
// source wildcard languages are accepted only when the target contains the
// identical canonical wildcard. The final rule deliberately fails closed
// instead of attempting nontrivial glob-language containment.
func SelectorImplies(source, target *Selector) bool {
	if source == nil || target == nil {
		return false
	}
	for dimension := DimensionIndex; dimension <= DimensionSourcetype; dimension++ {
		if !selectorDimensionImplies(source, target, dimension) {
			return false
		}
	}
	return true
}

// SelectorsProvablyDisjoint reports whether at least one selector dimension
// proves that the two selectors can never match the same event. The proof is
// deliberately conservative: two constrained dimensions are disjoint only
// when every cross-product pattern pair contains a literal which the other
// pattern does not match. Ambiguous wildcard-language pairs are treated as
// possibly overlapping.
func SelectorsProvablyDisjoint(left, right *Selector) bool {
	if left == nil || right == nil {
		return false
	}
	for dimension := DimensionIndex; dimension <= DimensionSourcetype; dimension++ {
		position := int(dimension - 1)
		leftDimension := &left.dimensions[position]
		rightDimension := &right.dimensions[position]
		if len(leftDimension.patterns) == 0 || len(rightDimension.patterns) == 0 {
			continue
		}
		if compiledDimensionsProvablyDisjoint(leftDimension, rightDimension) {
			return true
		}
	}
	return false
}

func compiledDimensionsProvablyDisjoint(left, right *compiledDimension) bool {
	if left == nil || right == nil {
		return false
	}
	for literal := range left.exact {
		if _, overlap := right.exact[literal]; overlap {
			return false
		}
		if right.wildcard != nil {
			for _, tokens := range right.wildcard.patterns {
				if globPatternMatchesLiteral(Pattern{tokens: tokens}, literal) {
					return false
				}
			}
		}
	}
	for literal := range right.exact {
		if left.wildcard != nil {
			for _, tokens := range left.wildcard.patterns {
				if globPatternMatchesLiteral(Pattern{tokens: tokens}, literal) {
					return false
				}
			}
		}
	}
	// Without a literal witness, wildcard-language disjointness is deliberately
	// unproven even when two spellings appear intuitively separate.
	return left.wildcard == nil || right.wildcard == nil
}

func selectorDimensionImplies(source, target *Selector, dimension Dimension) bool {
	targetPatterns := target.Patterns(dimension)
	if len(targetPatterns) == 0 {
		return true
	}
	sourcePatterns := source.Patterns(dimension)
	if len(sourcePatterns) == 0 {
		return false
	}
	if containsCanonicalPattern(targetPatterns, "*") {
		return true
	}
	targetCompiled := make([]Pattern, len(targetPatterns))
	for index, canonical := range targetPatterns {
		pattern, err := NormalizePattern(canonical)
		if err != nil || pattern.String() != canonical {
			return false
		}
		targetCompiled[index] = pattern
	}
	for _, canonical := range sourcePatterns {
		pattern, err := NormalizePattern(canonical)
		if err != nil || pattern.String() != canonical {
			return false
		}
		literal, isLiteral := pattern.Literal()
		if !isLiteral {
			if !containsCanonicalPattern(targetPatterns, canonical) {
				return false
			}
			continue
		}
		matched := false
		for _, candidate := range targetCompiled {
			if globPatternMatchesLiteral(candidate, literal) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func containsCanonicalPattern(values []string, target string) bool {
	return slices.Contains(values, target)
}

func globPatternMatchesLiteral(pattern Pattern, literal string) bool {
	valueIndex, tokenIndex := 0, 0
	starToken, starValue := -1, -1
	for valueIndex < len(literal) {
		if tokenIndex < len(pattern.tokens) {
			token := pattern.tokens[tokenIndex]
			value, width := utf8.DecodeRuneInString(literal[valueIndex:])
			if width == 0 || value == utf8.RuneError && width == 1 {
				return false
			}
			if token.kind == globLiteral && token.literal == value || token.kind == globOne {
				valueIndex += width
				tokenIndex++
				continue
			}
			if token.kind == globMany {
				starToken, starValue = tokenIndex, valueIndex
				tokenIndex++
				continue
			}
			if token.kind != globLiteral && token.kind != globOne {
				return false
			}
		}
		if starToken < 0 {
			return false
		}
		_, width := utf8.DecodeRuneInString(literal[starValue:])
		if width == 0 || width == 1 && literal[starValue] >= utf8.RuneSelf {
			return false
		}
		starValue += width
		valueIndex = starValue
		tokenIndex = starToken + 1
	}
	for tokenIndex < len(pattern.tokens) && pattern.tokens[tokenIndex].kind == globMany {
		tokenIndex++
	}
	return tokenIndex == len(pattern.tokens)
}
