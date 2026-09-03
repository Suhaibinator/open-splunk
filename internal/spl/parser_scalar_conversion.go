package spl

func (p *parser) parseConversionScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "tonumber":
		function = ScalarFunctionToNumber
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "tonumber requires exactly one argument")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"tonumber",
				arguments[0],
				"use isnull or isnotnull directly with where",
				"consume the Boolean with a supported conditional or conversion function",
			)
		}
	case "tostring":
		function = ScalarFunctionToString
		if len(arguments) != 1 && len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "tostring requires one or two arguments")
		}
		if len(arguments) == 2 {
			if err := validateToStringFormatLiteral(arguments[1]); err != nil {
				return nil, err
			}
		}
	case "isnull":
		function = ScalarFunctionIsNull
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "isnull requires exactly one argument")
		}
	case "isnotnull":
		function = ScalarFunctionIsNotNull
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "isnotnull requires exactly one argument")
		}
	case "coalesce":
		function = ScalarFunctionCoalesce
		if len(arguments) == 0 {
			return nil, invalidEvalArity(name.sourceRange, "coalesce requires at least one argument")
		}
	case "typeof":
		function = ScalarFunctionTypeOf
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "typeof requires exactly one argument")
		}
	case "nullif":
		// nullif(a, b) is sugar for if(a = b, null, a); it inherits the
		// comparison rules of if so no separate evaluation model exists.
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "nullif requires exactly two arguments")
		}
		for _, argument := range arguments {
			if scalarExpressionMayReturnBooleanFunction(argument) {
				return nil, booleanArgumentDiagnostic(
					"nullif",
					argument,
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				)
			}
		}
		if countErr := p.countEvalPredicate(arguments[0].SourceRange()); countErr != nil {
			return nil, countErr
		}
		callRange := Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End}
		return &ScalarIfExpr{
			Condition: &WhereComparisonExpr{
				Left:  arguments[0],
				Op:    CompareOpEqual,
				Right: arguments[1],
				Range: Range{Start: arguments[0].SourceRange().Start, End: arguments[1].SourceRange().End},
			},
			True: &ScalarLiteralExpr{
				Value: Literal{Kind: LiteralKindNull, Text: "null", Range: name.sourceRange},
				Range: name.sourceRange,
			},
			False: arguments[0],
			Range: callRange,
		}, nil
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
