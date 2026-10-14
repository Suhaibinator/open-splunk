package spl

// Conversion functions are command-specific keyword candidates, rather than
// scalar functions: advertising ctime in eval would promise unsupported syntax.
var convertSuggestionFunctionNames = []string{"ctime", "mktime", "dur2sec", "num", "auto", "rmunit", "none"}

func classifyConvertSuggestion(context SuggestionContext, tokens []token) SuggestionContext {
	functions := func(option, alias bool) SuggestionContext {
		keywords := append([]string(nil), convertSuggestionFunctionNames...)
		if option {
			keywords = append(keywords, "timeformat=")
		}
		if alias {
			keywords = append(keywords, "AS")
		}
		return fieldKeywordSuggestion(context, false, keywords...)
	}
	if len(tokens) == 0 {
		return functions(true, false)
	}
	if tokenWordEqual(tokens[0], "timeformat") {
		if len(tokens) < 3 || tokens[1].kind != tokenEqual || tokens[2].kind != tokenString {
			return context
		}
		tokens = tokens[3:]
	}
	if len(tokens) == 0 {
		return functions(false, false)
	}
	for len(tokens) > 0 {
		if tokens[0].kind != tokenWord || !containsASCIIFold(convertSuggestionFunctionNames, tokens[0].text) {
			return context
		}
		if len(tokens) < 2 || tokens[1].kind != tokenLeftParen {
			return context
		}
		if len(tokens) == 2 {
			return fieldKeywordSuggestion(context, true)
		}
		if len(tokens) < 4 || tokens[2].kind != tokenWord || tokens[3].kind != tokenRightParen {
			return context
		}
		allowsAlias := !tokenWordEqual(tokens[0], "none") && !IsFieldsFieldGlob(tokens[2].text)
		tokens = tokens[4:]
		if len(tokens) == 0 {
			return functions(false, allowsAlias)
		}
		if tokenWordEqual(tokens[0], "AS") {
			if !allowsAlias {
				return context
			}
			if len(tokens) == 1 {
				return fieldKeywordSuggestion(context, true)
			}
			if tokens[1].kind != tokenWord && tokens[1].kind != tokenString && tokens[1].kind != tokenQuotedField {
				return context
			}
			tokens = tokens[2:]
			if len(tokens) == 0 {
				return functions(false, false)
			}
		}
		if tokens[0].kind == tokenComma {
			tokens = tokens[1:]
			if len(tokens) == 0 {
				return functions(false, false)
			}
		}
	}
	return context
}
