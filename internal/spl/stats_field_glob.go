package spl

import "strings"

// IsStatsFieldGlob reports whether pattern is one valid unquoted stats
// wc-field. SPL stats wc-fields admit '*' as their only metacharacter. The
// remaining spelling must be representable as an exact unquoted field token;
// replacing stars before validation preserves the lexer's punctuation and
// resource boundaries without inventing a second field grammar.
func IsStatsFieldGlob(pattern string) bool {
	if pattern == "" || !strings.Contains(pattern, "*") {
		return false
	}
	candidate := strings.ReplaceAll(pattern, "*", "x")
	return !strings.HasPrefix(strings.ToLower(candidate), "__os_") &&
		IsExactUnquotedFieldName(candidate)
}

// MatchStatsFieldGlob reports whether one exact field name matches a stats
// wc-field pattern. Stats wc-fields use only '*' as a metacharacter; every
// other byte is literal. Parser validation guarantees UTF-8 source, so a byte
// comparison is also an exact Unicode comparison without case folding.
//
// The implementation walks the literal segments in order and never
// backtracks. This avoids regex compilation and exponential wildcard work.
func MatchStatsFieldGlob(pattern, name string) bool {
	_, matched := CaptureStatsFieldGlob(pattern, name)
	return matched
}

// CaptureStatsFieldGlob returns one substring per '*' in source order. Literal
// segments between stars use the earliest possible match; the final literal
// segment remains suffix-anchored. This makes multi-star AS substitution
// deterministic without regex backtracking.
func CaptureStatsFieldGlob(pattern, name string) ([]string, bool) {
	if !IsStatsFieldGlob(pattern) {
		return nil, false
	}
	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(name, parts[0]) {
		return nil, false
	}
	captures := make([]string, len(parts)-1)
	nameIndex := len(parts[0])
	for star := range captures {
		literal := parts[star+1]
		if star == len(captures)-1 {
			if literal == "" {
				captures[star] = name[nameIndex:]
				nameIndex = len(name)
				continue
			}
			literalStart := len(name) - len(literal)
			if literalStart < nameIndex || !strings.HasSuffix(name, literal) {
				return nil, false
			}
			captures[star] = name[nameIndex:literalStart]
			nameIndex = len(name)
			continue
		}
		relativeMatch := strings.Index(name[nameIndex:], literal)
		if relativeMatch < 0 {
			return nil, false
		}
		literalStart := nameIndex + relativeMatch
		captures[star] = name[nameIndex:literalStart]
		nameIndex = literalStart + len(literal)
	}
	return captures, nameIndex == len(name)
}

// SubstituteStatsFieldGlob replaces each '*' in pattern with the
// corresponding capture. A star-count mismatch is rejected atomically.
func SubstituteStatsFieldGlob(pattern string, captures []string) (string, bool) {
	if !IsStatsFieldGlob(pattern) || strings.Count(pattern, "*") != len(captures) {
		return "", false
	}
	parts := strings.Split(pattern, "*")
	var result strings.Builder
	result.Grow(len(pattern))
	result.WriteString(parts[0])
	for index, capture := range captures {
		result.WriteString(capture)
		result.WriteString(parts[index+1])
	}
	return result.String(), true
}

// ValidStatsFieldGlobAggregate reports whether aggregate is a well-formed
// ordinary wildcard arm. Logical planning uses this at its AST trust boundary
// before it consults any upstream schema fields.
func ValidStatsFieldGlobAggregate(aggregate StatsAggregate) bool {
	glob := aggregate.InputGlob
	if glob == nil || !IsStatsFieldGlob(glob.Pattern) || glob.Range == (Range{}) ||
		aggregate.Range == (Range{}) ||
		glob.Range.Start.Offset < aggregate.Range.Start.Offset ||
		glob.Range.End.Offset > aggregate.Range.End.Offset ||
		glob.Range.End.Offset <= glob.Range.Start.Offset ||
		aggregate.Sparkline != nil || aggregate.Input != "" ||
		aggregate.InputRange != (Range{}) || aggregate.InputQuoted ||
		aggregate.InputExpression != nil || aggregate.Predicate != nil ||
		aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return false
	}
	if glob.Implicit && (glob.Pattern != "*" ||
		glob.Range.Start != aggregate.Range.Start) {
		return false
	}
	if aggregate.AliasGlob == nil {
		if aggregate.ExplicitAlias || aggregate.AliasRange != aggregate.Range {
			return false
		}
	} else {
		alias := aggregate.AliasGlob
		if !aggregate.ExplicitAlias || alias.Implicit ||
			!IsStatsFieldGlob(alias.Pattern) || alias.Range == (Range{}) ||
			alias.Range != aggregate.AliasRange || alias.Range.End != aggregate.Range.End ||
			alias.Range.Start.Offset < glob.Range.End.Offset ||
			strings.Count(alias.Pattern, "*") != strings.Count(glob.Pattern, "*") {
			return false
		}
	}

	open := strings.IndexByte(aggregate.Alias, '(')
	if open <= 0 || aggregate.Alias[open:] != "("+glob.Pattern+")" {
		return false
	}
	name := aggregate.Alias[:open]
	spec, supported := statsAggregateSpecForName(name)
	if !supported || spec.canonicalName != name || spec.percentile != aggregate.Percentile {
		return false
	}
	effectiveFunction := spec.function
	if spec.inputFunction != AggregateFunctionInvalid {
		effectiveFunction = spec.inputFunction
	}
	return effectiveFunction == aggregate.Function
}

// ExpandStatsFieldGlob returns the ordinary exact-field aggregate represented
// by aggregate for one matching closed-schema field. The bool is false for
// forged or inconsistent wildcard metadata. The original source range is
// retained for diagnostics while wildcard provenance is consumed here.
func ExpandStatsFieldGlob(aggregate StatsAggregate, field string) (StatsAggregate, bool) {
	if !ValidStatsFieldGlobAggregate(aggregate) ||
		(!IsExactUnquotedFieldName(field) && !IsStatsLiteralFieldReference(field)) {
		return StatsAggregate{}, false
	}
	captures, matched := CaptureStatsFieldGlob(aggregate.InputGlob.Pattern, field)
	if !matched {
		return StatsAggregate{}, false
	}

	glob := aggregate.InputGlob
	open := strings.IndexByte(aggregate.Alias, '(')
	name := aggregate.Alias[:open]
	expanded := aggregate
	expanded.InputGlob = nil
	expanded.AliasGlob = nil
	expanded.Input = field
	expanded.InputQuoted = !IsExactUnquotedFieldName(field) ||
		!IsExactQuotedFieldName(field)
	expanded.InputRange = glob.Range
	expanded.AliasWildcardDerived = true
	if aggregate.AliasGlob == nil {
		expanded.Alias = name + "(" + field + ")"
	} else {
		output, ok := SubstituteStatsFieldGlob(
			aggregate.AliasGlob.Pattern,
			captures,
		)
		if !ok || !IsStatsLiteralOutputName(output) {
			return StatsAggregate{}, false
		}
		expanded.Alias = output
	}
	return expanded, true
}

// ValidStatsWildcardDerivedAlias validates the literal default output minted
// by ExpandStatsFieldGlob after its wildcard metadata has been consumed.
func ValidStatsWildcardDerivedAlias(aggregate StatsAggregate) bool {
	if !aggregate.AliasWildcardDerived ||
		aggregate.AliasQuoted || aggregate.AliasSourceDerived ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.AliasRange == (Range{}) ||
		!IsStatsLiteralOutputName(aggregate.Alias) {
		return false
	}
	if aggregate.Sparkline != nil {
		sparkline := aggregate.Sparkline
		if !aggregate.ExplicitAlias || aggregate.Function != AggregateFunctionInvalid ||
			aggregate.Input != "" || aggregate.InputRange != (Range{}) ||
			aggregate.InputQuoted || aggregate.InputExpression != nil ||
			aggregate.Predicate != nil || aggregate.Percentile != 0 ||
			sparkline.Input == "" || sparkline.InputRange == (Range{}) ||
			sparkline.InputGlob != nil {
			return false
		}
		if sparkline.InputQuoted {
			return IsStatsLiteralFieldReference(sparkline.Input)
		}
		return IsExactUnquotedFieldName(sparkline.Input)
	}
	if aggregate.Input == "" || aggregate.InputRange == (Range{}) ||
		aggregate.InputExpression != nil || aggregate.Predicate != nil {
		return false
	}
	if aggregate.InputQuoted {
		if !IsStatsLiteralFieldReference(aggregate.Input) {
			return false
		}
	} else if !IsExactUnquotedFieldName(aggregate.Input) {
		return false
	}
	if aggregate.ExplicitAlias {
		return aggregate.AliasRange.End == aggregate.Range.End
	}
	if aggregate.AliasRange != aggregate.Range {
		return false
	}
	open := strings.IndexByte(aggregate.Alias, '(')
	if open <= 0 || aggregate.Alias[open:] != "("+aggregate.Input+")" {
		return false
	}
	name := aggregate.Alias[:open]
	spec, supported := statsAggregateSpecForName(name)
	if !supported || spec.canonicalName != name || spec.percentile != aggregate.Percentile {
		return false
	}
	effectiveFunction := spec.function
	if spec.inputFunction != AggregateFunctionInvalid {
		effectiveFunction = spec.inputFunction
	}
	return effectiveFunction == aggregate.Function
}

// ValidStatsSparklineFieldGlobAggregate reports whether aggregate is a
// well-formed sparkline wildcard arm. The nested wildcard is expanded before
// any backend-neutral SparklineMeasure is constructed.
func ValidStatsSparklineFieldGlobAggregate(aggregate StatsAggregate) bool {
	sparkline := aggregate.Sparkline
	if sparkline == nil || sparkline.InputGlob == nil ||
		!IsStatsFieldGlob(sparkline.InputGlob.Pattern) ||
		sparkline.InputGlob.Range == (Range{}) || sparkline.Range == (Range{}) ||
		sparkline.InputGlob.Implicit || sparkline.Input != "" ||
		sparkline.InputQuoted || sparkline.InputRange != (Range{}) ||
		sparkline.InputGlob.Range.Start.Offset < sparkline.Range.Start.Offset ||
		sparkline.InputGlob.Range.End.Offset > sparkline.Range.End.Offset ||
		sparkline.InputGlob.Range.End.Offset <= sparkline.InputGlob.Range.Start.Offset ||
		aggregate.Function != AggregateFunctionInvalid || aggregate.Input != "" ||
		aggregate.InputGlob != nil || aggregate.InputQuoted ||
		aggregate.InputRange != (Range{}) || aggregate.InputExpression != nil ||
		aggregate.Predicate != nil || aggregate.Percentile != 0 ||
		aggregate.AliasQuoted || aggregate.AliasSourceDerived ||
		aggregate.AliasWildcardDerived ||
		aggregate.Range.Start != sparkline.Range.Start {
		return false
	}
	if !statsSparklineWildcardFunction(sparkline.Function) {
		return false
	}
	if aggregate.AliasGlob == nil {
		return !aggregate.ExplicitAlias && aggregate.Alias == "sparkline" &&
			aggregate.Range == sparkline.Range && aggregate.AliasRange == sparkline.Range
	}
	alias := aggregate.AliasGlob
	return aggregate.ExplicitAlias && aggregate.Alias == "sparkline" && !alias.Implicit &&
		IsStatsFieldGlob(alias.Pattern) && alias.Range != (Range{}) &&
		alias.Range == aggregate.AliasRange && alias.Range.End == aggregate.Range.End &&
		alias.Range.Start.Offset >= sparkline.Range.End.Offset &&
		strings.Count(alias.Pattern, "*") == strings.Count(sparkline.InputGlob.Pattern, "*")
}

// ExpandStatsSparklineFieldGlob returns one exact-input sparkline for a
// matching closed-schema field. The nested pointer is detached before it is
// mutated so expanding one authored term cannot alias another concrete term.
func ExpandStatsSparklineFieldGlob(
	aggregate StatsAggregate,
	field string,
) (StatsAggregate, bool) {
	if !ValidStatsSparklineFieldGlobAggregate(aggregate) ||
		(!IsExactUnquotedFieldName(field) && !IsStatsLiteralFieldReference(field)) {
		return StatsAggregate{}, false
	}
	sparkline := aggregate.Sparkline
	captures, matched := CaptureStatsFieldGlob(sparkline.InputGlob.Pattern, field)
	if !matched {
		return StatsAggregate{}, false
	}
	expanded := aggregate
	expandedSparkline := *sparkline
	expanded.Sparkline = &expandedSparkline
	expandedSparkline.Input = field
	expandedSparkline.InputQuoted = !IsExactUnquotedFieldName(field) ||
		!IsExactQuotedFieldName(field)
	expandedSparkline.InputRange = sparkline.InputGlob.Range
	expandedSparkline.InputGlob = nil
	expanded.AliasGlob = nil
	if aggregate.AliasGlob != nil {
		output, ok := SubstituteStatsFieldGlob(aggregate.AliasGlob.Pattern, captures)
		if !ok || !IsStatsLiteralOutputName(output) {
			return StatsAggregate{}, false
		}
		expanded.Alias = output
		expanded.AliasWildcardDerived = true
	}
	return expanded, true
}

func statsSparklineWildcardFunction(function AggregateFunction) bool {
	switch function {
	case AggregateFunctionCountValues,
		AggregateFunctionDistinctCount,
		AggregateFunctionAverage,
		AggregateFunctionStandardDeviationSample,
		AggregateFunctionStandardDeviationPopulation,
		AggregateFunctionVarianceSample,
		AggregateFunctionVariancePopulation,
		AggregateFunctionSum,
		AggregateFunctionSumSquares,
		AggregateFunctionMinimum,
		AggregateFunctionMaximum,
		AggregateFunctionRange:
		return true
	default:
		return false
	}
}
