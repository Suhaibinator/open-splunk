package searchsuggestions

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestNewRejectsMissingAndTypedNilDependencies(t *testing.T) {
	t.Parallel()

	valid := testConfig()
	var nilValidator *suggestionTestValidator
	var nilScopes *suggestionTestScopes
	var nilCompiler *suggestionTestCompiler
	var nilExecutor *suggestionTestExecutor
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "validator", mutate: func(config *Config) { config.Validator = nil }},
		{name: "typed nil validator", mutate: func(config *Config) { config.Validator = nilValidator }},
		{name: "scopes", mutate: func(config *Config) { config.Scopes = nil }},
		{name: "typed nil scopes", mutate: func(config *Config) { config.Scopes = nilScopes }},
		{name: "compiler", mutate: func(config *Config) { config.Compiler = nil }},
		{name: "typed nil compiler", mutate: func(config *Config) { config.Compiler = nilCompiler }},
		{name: "executor", mutate: func(config *Config) { config.Executor = nil }},
		{name: "typed nil executor", mutate: func(config *Config) { config.Executor = nilExecutor }},
		{name: "negative concurrency", mutate: func(config *Config) { config.MaxConcurrent = -1 }},
		{name: "excess concurrency", mutate: func(config *Config) {
			config.MaxConcurrent = maximumConcurrent + 1
		}},
		{name: "negative runtime", mutate: func(config *Config) { config.MaxRuntime = -time.Second }},
		{name: "excess runtime", mutate: func(config *Config) {
			config.MaxRuntime = maximumRuntime + time.Nanosecond
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.mutate(&config)
			if service, err := New(config); err == nil || service != nil {
				t.Fatalf("New() = (%#v, %v), want nil and error", service, err)
			}
		})
	}
}

func TestSuggestReturnsStaticAndIndexCandidatesWithoutStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		requested  []string
		candidates []string
		wantKind   spl.SuggestionKind
		wantLabel  string
	}{
		{
			name: "command", source: "index=main | he",
			candidates: []string{"main"}, wantKind: spl.SuggestionKindCommand, wantLabel: "head",
		},
		{
			name: "function", source: "index=main | stats co",
			candidates: []string{"main"}, wantKind: spl.SuggestionKindFunction, wantLabel: "count",
		},
		{
			name: "keyword", source: "index=main | rename host A",
			candidates: []string{"main"}, wantKind: spl.SuggestionKindKeyword, wantLabel: "AS",
		},
		{
			name: "requested index only", source: "index=ma",
			requested: []string{"main"}, candidates: []string{"metrics", "main", "main"},
			wantKind: spl.SuggestionKindIndex, wantLabel: "main",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			diagnostic := searchjobs.Diagnostic{
				Code: "SPL_INCOMPLETE", Message: "keep editing",
				ByteOffset: len(test.source), EndByteOffset: len(test.source),
				Line: 1, Column: len(test.source) + 1, EndLine: 1, EndColumn: len(test.source) + 1,
			}
			validator := &suggestionTestValidator{result: searchjobs.ValidationResult{
				Diagnostics: []searchjobs.Diagnostic{diagnostic},
			}}
			scopes := &suggestionTestScopes{failOnCall: true}
			compiler := &suggestionTestCompiler{failOnCall: true}
			executor := &suggestionTestExecutor{failOnCall: true}
			service := mustSuggestionService(t, Config{
				Validator: validator, Scopes: scopes, Compiler: compiler, Executor: executor,
			})

			request := validSuggestionRequest(test.source)
			request.RequestedIndexes = test.requested
			request.AuthorizedIndexCandidates = test.candidates
			result, err := service.Suggest(context.Background(), request)
			if err != nil {
				t.Fatalf("Suggest() error = %v", err)
			}
			if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != diagnostic.Code {
				t.Fatalf("Suggest() diagnostics = %#v", result.Diagnostics)
			}
			if !hasSuggestion(result.Suggestions, test.wantKind, test.wantLabel) {
				t.Fatalf("Suggest() suggestions = %#v, want %s %q", result.Suggestions, test.wantKind, test.wantLabel)
			}
			if test.wantKind == spl.SuggestionKindIndex &&
				hasSuggestion(result.Suggestions, spl.SuggestionKindIndex, "metrics") {
				t.Fatalf("Suggest() leaked index outside requested scope: %#v", result.Suggestions)
			}
			if scopes.calls != 0 || compiler.calls != 0 || executor.calls != 0 {
				t.Fatalf("storage calls = scope:%d compiler:%d executor:%d", scopes.calls, compiler.calls, executor.calls)
			}
		})
	}
}

func TestSuggestReturnsAnalyzerSyntaxDiagnosticsInBandWithoutStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "unterminated quote", source: `index=main | where "abc`, code: "SPL_UNTERMINATED_STRING"},
		{name: "unmatched closing parenthesis", source: "index=main )", code: "SPL_UNEXPECTED_TOKEN"},
		{
			name:   "token complexity",
			source: strings.Repeat("a ", 1_025),
			code:   "SPL_QUERY_TOO_COMPLEX",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validator := &suggestionTestValidator{result: searchjobs.ValidationResult{
				Diagnostics: []searchjobs.Diagnostic{activeDiagnostic(test.source, test.code)},
			}}
			scopes := &suggestionTestScopes{failOnCall: true}
			compiler := &suggestionTestCompiler{failOnCall: true}
			executor := &suggestionTestExecutor{failOnCall: true}
			service := mustSuggestionService(t, Config{
				Validator: validator, Scopes: scopes, Compiler: compiler, Executor: executor,
			})

			result, err := service.Suggest(
				context.Background(),
				validSuggestionRequest(test.source),
			)
			if err != nil {
				t.Fatalf("Suggest() error = %v", err)
			}
			if validator.calls != 1 ||
				len(result.Suggestions) != 0 ||
				!reflect.DeepEqual(result.Context, spl.SuggestionContext{}) ||
				len(result.Diagnostics) != 1 ||
				result.Diagnostics[0].Code != test.code {
				t.Fatalf("Suggest() = %#v, validator calls=%d", result, validator.calls)
			}
			if scopes.calls != 0 || compiler.calls != 0 || executor.calls != 0 {
				t.Fatalf("syntax error reached storage: scope=%d compiler=%d executor=%d",
					scopes.calls, compiler.calls, executor.calls)
			}
		})
	}
}

func TestSuggestRejectsValidatorThatContradictsBlockedAnalyzer(t *testing.T) {
	t.Parallel()

	service := mustSuggestionService(t, Config{
		Validator: validSuggestionValidator(),
		Scopes:    &suggestionTestScopes{failOnCall: true},
		Compiler:  &suggestionTestCompiler{failOnCall: true},
		Executor:  &suggestionTestExecutor{failOnCall: true},
	})
	result, err := service.Suggest(
		context.Background(),
		validSuggestionRequest(`index=main | where "abc`),
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("Suggest() = (%#v, %v), want zero and ErrInvalidResult", result, err)
	}
}

func TestSuggestFieldContextUsesOneImmutablePrefixScope(t *testing.T) {
	t.Parallel()

	source := "index=main | eval request_id=host | fields ho"
	request := validSuggestionRequest(source)
	maximum := uint32(12)
	request.MaxSuggestions = &maximum
	request.AuthorizedIndexes = []string{"metrics", "main"}
	request.RequestedIndexes = []string{"main"}
	request.AuthorizedIndexCandidates = []string{"main"}

	snapshot := validSuggestionSnapshot(t, request)
	validator := &suggestionTestValidator{result: searchjobs.ValidationResult{
		Diagnostics: []searchjobs.Diagnostic{activeDiagnostic(source, "SPL_EXPECTED_FIELD")},
	}}
	scopes := &suggestionTestScopes{snapshot: snapshot}
	var compiledPlan *plan.Query
	compiler := &suggestionTestCompiler{compile: func(
		logical *plan.Query,
		spec clickhouse.FieldSuggestionSpec,
	) (clickhouse.CompiledFieldSuggestions, error) {
		compiledPlan = logical
		if spec != (clickhouse.FieldSuggestionSpec{
			Prefix: "ho", MaximumFields: clickhouse.MaximumFieldSuggestions,
		}) {
			t.Fatalf("CompileFieldSuggestions() spec = %#v", spec)
		}
		return clickhouse.CompiledFieldSuggestions{SQL: "SELECT names", Spec: spec}, nil
	}}
	executor := &suggestionTestExecutor{result: queryexec.FieldSuggestionResult{
		FieldNames: []string{`ho\.id`, `ho\\name`, "host"},
	}}
	service := mustSuggestionService(t, Config{
		Validator: validator, Scopes: scopes, Compiler: compiler, Executor: executor,
	})

	authorizedBefore := slices.Clone(request.AuthorizedIndexes)
	requestedBefore := slices.Clone(request.RequestedIndexes)
	result, err := service.Suggest(context.Background(), request)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if scopes.calls != 1 || compiler.calls != 1 || executor.calls != 1 {
		t.Fatalf("calls = scope:%d compiler:%d executor:%d", scopes.calls, compiler.calls, executor.calls)
	}
	if !reflect.DeepEqual(request.AuthorizedIndexes, authorizedBefore) ||
		!reflect.DeepEqual(request.RequestedIndexes, requestedBefore) {
		t.Fatalf("Suggest() mutated request: %#v", request)
	}
	if compiledPlan == nil || len(compiledPlan.Operators) != 3 {
		t.Fatalf("compiled prefix plan = %#v, want scoped scan, base filter, and eval", compiledPlan)
	}
	if _, ok := compiledPlan.Operators[2].(*plan.Extend); !ok {
		t.Fatalf("compiled prefix final operator = %T, want *plan.Extend", compiledPlan.Operators[2])
	}
	scan, ok := compiledPlan.Operators[0].(*plan.Scan)
	if !ok || scan.TenantID != request.TenantID ||
		!reflect.DeepEqual(scan.Indexes, []string{"main"}) ||
		scan.VisibilityCutoff != snapshot.VisibilityCutoff ||
		!scan.IndexTimeCutoff.Equal(snapshot.IndexTimeCutoff) {
		t.Fatalf("compiled scan = %#v", compiledPlan.Operators[0])
	}
	for _, accepted := range []string{"host", `ho\\name`, `ho\.id`} {
		if !hasSuggestion(result.Suggestions, spl.SuggestionKindField, accepted) {
			t.Fatalf("Suggest() omitted representable field %q: %#v", accepted, result.Suggestions)
		}
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "SPL_EXPECTED_FIELD" {
		t.Fatalf("Suggest() diagnostics = %#v", result.Diagnostics)
	}
}

func TestSuggestFrequencyFieldCandidatesExcludeGeneratedAndCommittedNames(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		fields    []string
		required  []string
		forbidden []string
	}{
		{
			name:   "first tuple field",
			source: `index=main | top `,
			fields: []string{
				"BY", "by", "Count", "count", "Host", "host",
				"Percent", "percent", "source",
			},
			required:  []string{"Count", "Host", "host", "Percent", "source"},
			forbidden: []string{"BY", "by", "count", "percent"},
		},
		{
			name:   "next tuple field",
			source: `index=main | top host, `,
			fields: []string{
				"BY", "by", "Count", "count", "Host", "host",
				"Percent", "percent", "source",
			},
			required:  []string{"Count", "Host", "Percent", "source"},
			forbidden: []string{"BY", "by", "count", "host", "percent"},
		},
		{
			name:      "active next tuple prefix",
			source:    `index=main | top host, ho`,
			fields:    []string{"host", "hostname"},
			required:  []string{"hostname"},
			forbidden: []string{"host"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := validSuggestionRequest(test.source)
			wantContext, contextDiagnostic := spl.AnalyzeSuggestionContext(
				test.source,
				len(test.source),
			)
			if contextDiagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext() error = %v", contextDiagnostic)
			}
			validator := &suggestionTestValidator{result: searchjobs.ValidationResult{
				Diagnostics: []searchjobs.Diagnostic{
					activeDiagnostic(test.source, "SPL_EXPECTED_FIELD"),
				},
			}}
			scopes := &suggestionTestScopes{snapshot: validSuggestionSnapshot(t, request)}
			compiler := &suggestionTestCompiler{}
			executor := &suggestionTestExecutor{result: queryexec.FieldSuggestionResult{
				FieldNames: slices.Clone(test.fields),
			}}
			service := mustSuggestionService(t, Config{
				Validator: validator,
				Scopes:    scopes,
				Compiler:  compiler,
				Executor:  executor,
			})

			result, err := service.Suggest(context.Background(), request)
			if err != nil {
				t.Fatalf("Suggest() error = %v", err)
			}
			if validator.calls != 1 || scopes.calls != 1 || compiler.calls != 1 || executor.calls != 1 {
				t.Fatalf("dependency calls = validator:%d scope:%d compiler:%d executor:%d, want one each",
					validator.calls, scopes.calls, compiler.calls, executor.calls)
			}
			if !reflect.DeepEqual(result.Context.Exclusions, wantContext.Exclusions) {
				t.Fatalf(
					"context exclusions = %#v, want cloned analyzer exclusions %#v",
					result.Context.Exclusions,
					wantContext.Exclusions,
				)
			}
			for _, label := range test.required {
				if !hasSuggestion(result.Suggestions, spl.SuggestionKindField, label) {
					t.Errorf("Suggest() omitted required field %q: %#v", label, result.Suggestions)
				}
			}
			for _, label := range test.forbidden {
				for _, suggestion := range result.Suggestions {
					if suggestion.Label == label {
						t.Errorf("Suggest() returned excluded label %q: %#v", label, result.Suggestions)
						break
					}
				}
			}
		})
	}
}

func TestSuggestFrequencyFieldCandidatesExcludeLaterTupleNamesAtMidStageCursor(t *testing.T) {
	t.Parallel()

	source := `index=main | top ho,host`
	request := validSuggestionRequest(source)
	request.CursorByteOffset = len(`index=main | top ho`)
	validator := validSuggestionValidator()
	scopes := &suggestionTestScopes{snapshot: validSuggestionSnapshot(t, request)}
	compiler := &suggestionTestCompiler{}
	executor := &suggestionTestExecutor{result: queryexec.FieldSuggestionResult{
		FieldNames: []string{"host", "hostname"},
	}}
	service := mustSuggestionService(t, Config{
		Validator: validator,
		Scopes:    scopes,
		Compiler:  compiler,
		Executor:  executor,
	})

	result, err := service.Suggest(context.Background(), request)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if validator.calls != 1 || scopes.calls != 1 || compiler.calls != 1 || executor.calls != 1 {
		t.Fatalf("dependency calls = validator:%d scope:%d compiler:%d executor:%d, want one each",
			validator.calls, scopes.calls, compiler.calls, executor.calls)
	}
	if hasSuggestion(result.Suggestions, spl.SuggestionKindField, "host") ||
		!hasSuggestion(result.Suggestions, spl.SuggestionKindField, "hostname") {
		t.Fatalf("Suggest() suggestions = %#v, want hostname without later tuple field host", result.Suggestions)
	}
}

func TestSuggestBaseFieldContextBuildsOnlyScopedScan(t *testing.T) {
	t.Parallel()

	request := validSuggestionRequest("ho")
	snapshot := validSuggestionSnapshot(t, request)
	scopes := &suggestionTestScopes{snapshot: snapshot}
	compiler := &suggestionTestCompiler{compile: func(
		logical *plan.Query,
		spec clickhouse.FieldSuggestionSpec,
	) (clickhouse.CompiledFieldSuggestions, error) {
		if len(logical.Operators) != 1 {
			t.Fatalf("base plan operators = %#v, want one scan", logical.Operators)
		}
		if spec.MaximumFields != clickhouse.MaximumFieldSuggestions {
			t.Fatalf("base plan maximum = %d, want candidate window %d",
				spec.MaximumFields, clickhouse.MaximumFieldSuggestions)
		}
		return clickhouse.CompiledFieldSuggestions{SQL: "SELECT names", Spec: spec}, nil
	}}
	service := mustSuggestionService(t, Config{
		Validator: &suggestionTestValidator{result: searchjobs.ValidationResult{
			Diagnostics: []searchjobs.Diagnostic{activeDiagnostic(request.SPL, "SPL_EXPECTED_TERM")},
		}},
		Scopes: scopes, Compiler: compiler,
		Executor: &suggestionTestExecutor{result: queryexec.FieldSuggestionResult{FieldNames: []string{"host"}}},
	})

	result, err := service.Suggest(context.Background(), request)
	if err != nil || !hasSuggestion(result.Suggestions, spl.SuggestionKindField, "host") {
		t.Fatalf("Suggest() = (%#v, %v)", result, err)
	}
	if service.MaximumSuggestions() != spl.MaximumSuggestionLimit {
		t.Fatalf("MaximumSuggestions() = %d", service.MaximumSuggestions())
	}
}

func TestSuggestPreservesCompletedBaseAndSearchPredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		wantFilters int
	}{
		{
			name: "base index predicate", source: "index=main ho",
			wantFilters: 1,
		},
		{
			name:        "active search index predicate",
			source:      "index=main OR index=metrics | search index=main ho",
			wantFilters: 2,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := validSuggestionRequest(test.source)
			var captured *plan.Query
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{result: searchjobs.ValidationResult{
					Diagnostics: []searchjobs.Diagnostic{
						activeDiagnostic(test.source, "SPL_EXPECTED_TERM"),
					},
				}},
				Scopes: &suggestionTestScopes{
					snapshot: validSuggestionSnapshot(t, request),
				},
				Compiler: &suggestionTestCompiler{compile: func(
					logical *plan.Query,
					spec clickhouse.FieldSuggestionSpec,
				) (clickhouse.CompiledFieldSuggestions, error) {
					captured = logical
					return clickhouse.CompiledFieldSuggestions{SQL: "SELECT names", Spec: spec}, nil
				}},
				Executor: &suggestionTestExecutor{},
			})
			if _, err := service.Suggest(context.Background(), request); err != nil {
				t.Fatalf("Suggest() error = %v", err)
			}
			filterCount := 0
			for _, operator := range captured.Operators {
				if _, ok := operator.(*plan.Filter); ok {
					filterCount++
				}
			}
			if filterCount != test.wantFilters {
				t.Fatalf("completed-prefix filters = %d, want %d; plan=%#v",
					filterCount, test.wantFilters, captured)
			}
		})
	}
}

func TestSuggestSuppressesUnparseableCompletedBaseAndSearchPredicatesBeforeSnapshot(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"index=main AND ho",
		"index=main | search status=200 AND ho",
	} {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			scopes := &suggestionTestScopes{failOnCall: true}
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{result: searchjobs.ValidationResult{
					Diagnostics: []searchjobs.Diagnostic{
						activeDiagnostic(source, "SPL_EXPECTED_TERM"),
					},
				}},
				Scopes:   scopes,
				Compiler: &suggestionTestCompiler{failOnCall: true},
				Executor: &suggestionTestExecutor{failOnCall: true},
			})
			result, err := service.Suggest(
				context.Background(),
				validSuggestionRequest(source),
			)
			if err != nil || len(result.Diagnostics) != 1 {
				t.Fatalf("Suggest() = (%#v, %v)", result, err)
			}
			if scopes.calls != 0 {
				t.Fatalf("unparseable completed prefix took %d snapshots", scopes.calls)
			}
		})
	}
}

func TestSuggestComposesRealPrefixPlanAndFieldCompiler(t *testing.T) {
	t.Parallel()

	request := validSuggestionRequest("index=main | fields ho")
	var executed clickhouse.CompiledFieldSuggestions
	executor := &suggestionTestExecutor{execute: func(
		_ context.Context,
		compiled clickhouse.CompiledFieldSuggestions,
	) (queryexec.FieldSuggestionResult, error) {
		executed = compiled
		return queryexec.FieldSuggestionResult{}, nil
	}}
	service := mustSuggestionService(t, Config{
		Validator: validSuggestionValidator(),
		Scopes: &suggestionTestScopes{
			snapshot: validSuggestionSnapshot(t, request),
		},
		Compiler: clickhouse.Compiler{},
		Executor: executor,
	})

	result, err := service.Suggest(context.Background(), request)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if executed.SQL == "" || executed.Spec.Prefix != "ho" ||
		executed.Spec.MaximumFields != clickhouse.MaximumFieldSuggestions {
		t.Fatalf("executed compiled field suggestions = %#v", executed)
	}
	if len(executed.Args) == 0 {
		t.Fatal("real field compiler produced no scoped arguments")
	}
	if len(result.Suggestions) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("Suggest() = %#v, want empty successful result", result)
	}
}

func TestRepresentableFieldNameMatchesCurrentSingleTokenGrammar(t *testing.T) {
	t.Parallel()

	for _, accepted := range []string{
		"host", "_raw", "request.path", `request\.id`, `path\\name`, "münchen",
	} {
		if !representableFieldName(accepted) {
			t.Errorf("representableFieldName(%q) = false", accepted)
		}
	}
	for _, rejected := range []string{
		"", "+host", "-host", "bad field", "bad\tfield", "bad\nfield",
		`bad"field`, "bad|field", "bad(field", "bad,field", "bad=field",
		"bad!field", "bad<field", "bad>field", "bad*field", "__os_private",
		`bad\escape`,
	} {
		if representableFieldName(rejected) {
			t.Errorf("representableFieldName(%q) = true", rejected)
		}
	}
}

func TestValidateFieldResultAcceptsSeventeenSegmentQueryField(t *testing.T) {
	t.Parallel()

	fieldName := strings.Repeat("segment.", 16) + "leaf"
	err := validateFieldResult(
		queryexec.FieldSuggestionResult{FieldNames: []string{fieldName}},
		clickhouse.FieldSuggestionSpec{Prefix: "segment.", MaximumFields: 100},
	)
	if err != nil {
		t.Fatalf("validateFieldResult(seventeen-segment query field): %v", err)
	}
	err = validateFieldResult(
		queryexec.FieldSuggestionResult{FieldNames: []string{"edge_obj", "edge_obj.child"}},
		clickhouse.FieldSuggestionSpec{Prefix: "edge_", MaximumFields: 100},
	)
	if err != nil {
		t.Fatalf("validateFieldResult(visible parent and child): %v", err)
	}
}

func TestSuggestSuppressesUnsafeDynamicFieldLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		diagnostic   searchjobs.Diagnostic
		wantSnapshot bool
	}{
		{
			name: "forbidden index anywhere", source: "index=forbidden | fields ho",
			diagnostic: activeDiagnostic("index=forbidden | fields ho", "SPL_INDEX_FORBIDDEN"),
		},
		{
			name: "diagnostic before active fragment", source: "index=main | fields ho",
			diagnostic: searchjobs.Diagnostic{
				Code: "SPL_PARSE", Message: "earlier failure",
				ByteOffset: 0, EndByteOffset: 3, Line: 1, Column: 1, EndLine: 1, EndColumn: 4,
			},
		},
		{
			name: "positionless diagnostic fails closed", source: "index=main | fields ho",
			diagnostic: searchjobs.Diagnostic{Code: "SPL_PARSE", Message: "unknown location"},
		},
		{
			name: "out of bounds diagnostic fails closed", source: "index=main | fields ho",
			diagnostic: searchjobs.Diagnostic{
				Code: "SPL_PARSE", Message: "forged location",
				ByteOffset: 0, EndByteOffset: 1_000, Line: 1, Column: 1, EndLine: 1, EndColumn: 1_001,
			},
		},
		{
			name: "diagnostic ending at fragment may coexist", source: "index=main | fields ho",
			diagnostic: searchjobs.Diagnostic{
				Code: "SPL_PARSE", Message: "active failure",
				ByteOffset: 0, EndByteOffset: strings.LastIndex("index=main | fields ho", "ho"),
				Line: 1, Column: 1, EndLine: 1,
				EndColumn: strings.LastIndex("index=main | fields ho", "ho") + 1,
			},
			wantSnapshot: true,
		},
		{
			name: "transformed completed prefix is unavailable", source: "index=main | stats count | fields ho",
			diagnostic:   activeDiagnostic("index=main | stats count | fields ho", "SPL_EXPECTED_FIELD"),
			wantSnapshot: true,
		},
		{
			name: "invalid completed prefix is unavailable", source: "index=main | future thing | fields ho",
			diagnostic: activeDiagnostic("index=main | future thing | fields ho", "SPL_EXPECTED_FIELD"),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := validSuggestionRequest(test.source)
			scopes := &suggestionTestScopes{snapshot: validSuggestionSnapshot(t, request)}
			compiler := &suggestionTestCompiler{}
			executor := &suggestionTestExecutor{result: queryexec.FieldSuggestionResult{FieldNames: []string{"host"}}}
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{result: searchjobs.ValidationResult{
					Diagnostics: []searchjobs.Diagnostic{test.diagnostic},
				}},
				Scopes: scopes, Compiler: compiler, Executor: executor,
			})
			result, err := service.Suggest(context.Background(), request)
			if err != nil {
				t.Fatalf("Suggest() error = %v", err)
			}
			wantCalls := 0
			if test.wantSnapshot {
				wantCalls = 1
			}
			if scopes.calls != wantCalls {
				t.Fatalf("scope calls = %d, want %d", scopes.calls, wantCalls)
			}
			if strings.Contains(test.name, "transformed") || strings.Contains(test.name, "invalid completed") {
				if compiler.calls != 0 || executor.calls != 0 {
					t.Fatalf("unsupported prefix reached storage: compiler=%d executor=%d", compiler.calls, executor.calls)
				}
			} else if test.wantSnapshot {
				if compiler.calls != 1 || executor.calls != 1 ||
					!hasSuggestion(result.Suggestions, spl.SuggestionKindField, "host") {
					t.Fatalf("safe active diagnostic did not coexist: %#v", result)
				}
			} else if compiler.calls != 0 || executor.calls != 0 {
				t.Fatalf("unsafe diagnostic reached storage: compiler=%d executor=%d", compiler.calls, executor.calls)
			}
			if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != test.diagnostic.Code {
				t.Fatalf("diagnostics = %#v", result.Diagnostics)
			}
		})
	}
}

func TestSuggestRejectsStructuralAndScopeErrorsBeforeDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{name: "nil maximum not invalid", mutate: func(request *Request) {
			zero := uint32(0)
			request.MaxSuggestions = &zero
		}},
		{name: "maximum too large", mutate: func(request *Request) {
			over := uint32(spl.MaximumSuggestionLimit + 1)
			request.MaxSuggestions = &over
		}},
		{name: "negative cursor", mutate: func(request *Request) { request.CursorByteOffset = -1 }},
		{name: "cursor after source", mutate: func(request *Request) {
			request.CursorByteOffset = len(request.SPL) + 1
		}},
		{name: "cursor within rune", mutate: func(request *Request) {
			request.SPL = "høst"
			request.CursorByteOffset = 2
		}},
		{name: "invalid UTF-8", mutate: func(request *Request) {
			request.SPL = string([]byte{0xff})
			request.CursorByteOffset = 1
		}},
		{name: "NUL source", mutate: func(request *Request) {
			request.SPL = "ho\x00st"
			request.CursorByteOffset = len(request.SPL)
		}},
		{name: "source byte overflow", mutate: func(request *Request) {
			request.SPL = strings.Repeat("x", spl.MaximumSuggestionSourceBytes+1)
			request.CursorByteOffset = len(request.SPL)
		}},
		{name: "invalid tenant", mutate: func(request *Request) { request.TenantID = " tenant" }},
		{name: "no authorized indexes", mutate: func(request *Request) { request.AuthorizedIndexes = nil }},
		{name: "requested outside authorization", mutate: func(request *Request) {
			request.RequestedIndexes = []string{"secret"}
		}},
		{name: "candidate outside authorization", mutate: func(request *Request) {
			request.AuthorizedIndexCandidates = []string{"secret"}
		}},
		{name: "too many index candidates", mutate: func(request *Request) {
			request.AuthorizedIndexCandidates = make([]string, maximumIndexes+1)
			for index := range request.AuthorizedIndexCandidates {
				request.AuthorizedIndexCandidates[index] = "main"
			}
		}},
		{name: "invalid range", mutate: func(request *Request) { request.TimeRange = searchtime.Range{} }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validator := &suggestionTestValidator{failOnCall: true}
			request := validSuggestionRequest("ho")
			test.mutate(&request)
			service := mustSuggestionService(t, Config{
				Validator: validator,
				Scopes:    &suggestionTestScopes{failOnCall: true},
				Compiler:  &suggestionTestCompiler{failOnCall: true},
				Executor:  &suggestionTestExecutor{failOnCall: true},
			})
			result, err := service.Suggest(context.Background(), request)
			if !errors.Is(err, ErrInvalidRequest) || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("Suggest() = (%#v, %v), want zero and ErrInvalidRequest", result, err)
			}
			if validator.calls != 0 {
				t.Fatalf("validator calls = %d", validator.calls)
			}
		})
	}
}

func TestSuggestMapsDependencyErrorsWithoutLeakingDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validator  error
		scopes     error
		compiler   error
		executor   error
		want       error
		wantNoText string
	}{
		{name: "validation capacity", validator: fmtError(searchjobs.ErrCapacity, "private validation"), want: searchjobs.ErrCapacity, wantNoText: "private"},
		{name: "validation closed", validator: fmtError(searchjobs.ErrClosed, "private manager"), want: searchjobs.ErrClosed, wantNoText: "private"},
		{name: "snapshot storage", scopes: fmtError(searchjobs.ErrStorageUnavailable, "clickhouse host"), want: searchjobs.ErrStorageUnavailable, wantNoText: "host"},
		{name: "execution limit", executor: fmtError(searchjobs.ErrExecutionLimit, "query settings"), want: searchjobs.ErrExecutionLimit, wantNoText: "settings"},
		{name: "metadata unavailable", executor: queryexec.ErrFieldMetadataUnavailable, want: searchjobs.ErrStorageUnavailable},
		{name: "invalid result", executor: fmtError(searchjobs.ErrInvalidResult, "bad column"), want: searchjobs.ErrInvalidResult, wantNoText: "column"},
		{name: "spurious dependency cancellation", executor: context.Canceled, want: searchjobs.ErrInvalidResult},
		{name: "unknown compiler", compiler: errors.New("raw SQL failure"), want: searchjobs.ErrInvalidResult, wantNoText: "SQL"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := validSuggestionRequest("index=main | fields ho")
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{
					result: validSuggestionValidation(request), err: test.validator,
				},
				Scopes: &suggestionTestScopes{
					snapshot: validSuggestionSnapshot(t, request), err: test.scopes,
				},
				Compiler: &suggestionTestCompiler{err: test.compiler},
				Executor: &suggestionTestExecutor{err: test.executor},
			})
			result, err := service.Suggest(context.Background(), request)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("Suggest() = (%#v, %v), want zero and %v", result, err, test.want)
			}
			if test.wantNoText != "" && strings.Contains(err.Error(), test.wantNoText) {
				t.Fatalf("Suggest() leaked dependency detail: %v", err)
			}
		})
	}
}

func TestSuggestRejectsChangedDependencyContractsAtomically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot func(searchjobs.AnalysisScopeSnapshot) searchjobs.AnalysisScopeSnapshot
		compiled func(clickhouse.CompiledFieldSuggestions) clickhouse.CompiledFieldSuggestions
		result   queryexec.FieldSuggestionResult
	}{
		{name: "changed tenant", snapshot: func(snapshot searchjobs.AnalysisScopeSnapshot) searchjobs.AnalysisScopeSnapshot {
			snapshot.TenantID = "other"
			return snapshot
		}},
		{name: "changed scope", snapshot: func(snapshot searchjobs.AnalysisScopeSnapshot) searchjobs.AnalysisScopeSnapshot {
			snapshot.RequestedIndexes = []string{"metrics"}
			return snapshot
		}},
		{name: "changed compiler prefix", compiled: func(compiled clickhouse.CompiledFieldSuggestions) clickhouse.CompiledFieldSuggestions {
			compiled.Spec.Prefix = "other"
			return compiled
		}},
		{name: "changed compiler maximum", compiled: func(compiled clickhouse.CompiledFieldSuggestions) clickhouse.CompiledFieldSuggestions {
			compiled.Spec.MaximumFields++
			return compiled
		}},
		{name: "unsorted field names", result: queryexec.FieldSuggestionResult{FieldNames: []string{"host_z", "host_a"}}},
		{name: "bytewise order differs from rank", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"hoZ", "hoa"},
		}},
		{name: "exact prefix is not first", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"hoa", "ho"},
		}},
		{name: "field outside prefix", result: queryexec.FieldSuggestionResult{FieldNames: []string{"status"}}},
		{name: "field with whitespace", result: queryexec.FieldSuggestionResult{FieldNames: []string{"ho bad"}}},
		{name: "field with operator", result: queryexec.FieldSuggestionResult{FieldNames: []string{"ho*bad"}}},
		{name: "field with Unicode format", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"ho\u200bbad"},
		}},
		{name: "oversized field name", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"ho" + strings.Repeat(
				"x",
				eventfields.MaximumNormalizedFieldNameBytes,
			)},
		}},
		{name: "noncanonical field path", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{`ho\q`},
		}},
		{name: "control field name", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"ho\x01st"},
		}},
		{name: "truncated short result", result: queryexec.FieldSuggestionResult{
			FieldNames: []string{"host"}, Truncated: true,
		}},
		{name: "too many fields", result: queryexec.FieldSuggestionResult{
			FieldNames: repeatedFields(int(clickhouse.MaximumFieldSuggestions) + 1),
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := validSuggestionRequest("index=main | fields ho")
			snapshot := validSuggestionSnapshot(t, request)
			if test.snapshot != nil {
				snapshot = test.snapshot(snapshot)
			}
			maximum := uint32(20)
			compiler := &suggestionTestCompiler{compile: func(
				_ *plan.Query,
				spec clickhouse.FieldSuggestionSpec,
			) (clickhouse.CompiledFieldSuggestions, error) {
				compiled := clickhouse.CompiledFieldSuggestions{SQL: "SELECT names", Spec: spec}
				if test.compiled != nil {
					compiled = test.compiled(compiled)
				}
				return compiled, nil
			}}
			result := test.result
			if result.FieldNames == nil {
				result.FieldNames = []string{"host"}
			}
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{result: validSuggestionValidation(request)},
				Scopes:    &suggestionTestScopes{snapshot: snapshot},
				Compiler:  compiler,
				Executor:  &suggestionTestExecutor{result: result},
			})
			suggestRequest := request
			suggestRequest.MaxSuggestions = &maximum
			got, err := service.Suggest(context.Background(), suggestRequest)
			if !errors.Is(err, searchjobs.ErrInvalidResult) || !reflect.DeepEqual(got, Result{}) {
				t.Fatalf("Suggest() = (%#v, %v), want zero and ErrInvalidResult", got, err)
			}
		})
	}
}

func TestSuggestRejectsPartialValidationMetadataAtomically(t *testing.T) {
	t.Parallel()

	request := validSuggestionRequest("index=main | he")
	valid := validSuggestionValidation(request)
	invalidDiagnostic := activeDiagnostic(request.SPL, "SPL_EXPECTED_COMMAND")
	tests := []struct {
		name   string
		result searchjobs.ValidationResult
	}{
		{name: "valid missing normalized SPL", result: func() searchjobs.ValidationResult {
			result := valid
			result.NormalizedSPL = ""
			return result
		}()},
		{name: "valid missing result kind", result: func() searchjobs.ValidationResult {
			result := valid
			result.PredictedResultKind = searchjobs.ValidationResultKindInvalid
			return result
		}()},
		{name: "valid changed indexes", result: func() searchjobs.ValidationResult {
			result := valid
			result.ReferencedIndexes = []string{"secret"}
			return result
		}()},
		{name: "valid with diagnostics", result: func() searchjobs.ValidationResult {
			result := valid
			result.Diagnostics = []searchjobs.Diagnostic{invalidDiagnostic}
			return result
		}()},
		{name: "valid unsorted fields", result: func() searchjobs.ValidationResult {
			result := valid
			result.ReferencedFields = []string{"z", "a"}
			return result
		}()},
		{name: "valid private referenced field", result: func() searchjobs.ValidationResult {
			result := valid
			result.ReferencedFields = []string{"__os_private"}
			return result
		}()},
		{name: "valid malformed referenced field", result: func() searchjobs.ValidationResult {
			result := valid
			result.ReferencedFields = []string{`bad\escape`}
			return result
		}()},
		{name: "invalid with normalized SPL", result: searchjobs.ValidationResult{
			NormalizedSPL: request.SPL,
			Diagnostics:   []searchjobs.Diagnostic{invalidDiagnostic},
		}},
		{name: "invalid with referenced metadata", result: searchjobs.ValidationResult{
			Diagnostics:         []searchjobs.Diagnostic{invalidDiagnostic},
			ReferencedIndexes:   []string{"main"},
			ReferencedFields:    []string{"host"},
			PredictedResultKind: searchjobs.ValidationResultKindEvents,
		}},
		{name: "invalid with NUL diagnostic code", result: searchjobs.ValidationResult{
			Diagnostics: []searchjobs.Diagnostic{{
				Code: "SPL\x00PARSE", Message: "invalid",
			}},
		}},
		{name: "invalid with NUL diagnostic hint", result: searchjobs.ValidationResult{
			Diagnostics: []searchjobs.Diagnostic{{
				Code: "SPL_PARSE", Message: "invalid", Suggestions: []string{"bad\x00hint"},
			}},
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := mustSuggestionService(t, Config{
				Validator: &suggestionTestValidator{result: test.result},
				Scopes:    &suggestionTestScopes{failOnCall: true},
				Compiler:  &suggestionTestCompiler{failOnCall: true},
				Executor:  &suggestionTestExecutor{failOnCall: true},
			})
			result, err := service.Suggest(context.Background(), request)
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				!reflect.DeepEqual(result, Result{}) {
				t.Fatalf("Suggest() = (%#v, %v), want zero and ErrInvalidResult", result, err)
			}
		})
	}
}

func TestSuggestAcceptsNarrowedValidationIndexSubset(t *testing.T) {
	t.Parallel()

	request := validSuggestionRequest("index=main | he")
	validation := validSuggestionValidation(request)
	validation.ReferencedIndexes = []string{"main"}
	service := mustSuggestionService(t, Config{
		Validator: &suggestionTestValidator{result: validation},
		Scopes:    &suggestionTestScopes{failOnCall: true},
		Compiler:  &suggestionTestCompiler{failOnCall: true},
		Executor:  &suggestionTestExecutor{failOnCall: true},
	})
	result, err := service.Suggest(context.Background(), request)
	if err != nil || !hasSuggestion(result.Suggestions, spl.SuggestionKindCommand, "head") {
		t.Fatalf("Suggest() = (%#v, %v)", result, err)
	}
}

func TestSuggestAdmissionCancellationAndClose(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	validator := &suggestionTestValidator{validate: func(
		ctx context.Context,
		_ searchjobs.ValidateRequest,
	) (searchjobs.ValidationResult, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return searchjobs.ValidationResult{}, ctx.Err()
	}}
	service := mustSuggestionService(t, Config{
		Validator:     validator,
		Scopes:        &suggestionTestScopes{failOnCall: true},
		Compiler:      &suggestionTestCompiler{failOnCall: true},
		Executor:      &suggestionTestExecutor{failOnCall: true},
		MaxConcurrent: 1,
		MaxRuntime:    time.Minute,
	})
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.Suggest(context.Background(), validSuggestionRequest("index=main | he"))
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first Suggest() did not enter validator")
	}

	if result, err := service.Suggest(
		context.Background(),
		validSuggestionRequest("index=main | he"),
	); !errors.Is(err, searchjobs.ErrCapacity) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("second Suggest() = (%#v, %v), want capacity", result, err)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- service.Close(context.Background()) }()
	select {
	case err := <-firstDone:
		if !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("active Suggest() error = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("active Suggest() did not stop on Close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not finish")
	}
	if result, err := service.Suggest(
		context.Background(),
		validSuggestionRequest("index=main | he"),
	); !errors.Is(err, searchjobs.ErrClosed) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("Suggest(after Close) = (%#v, %v)", result, err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
}

func TestSuggestPreservesCallerCancellationAndCloseContract(t *testing.T) {
	t.Parallel()

	service := mustSuggestionService(t, testConfig())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := service.Suggest(
		canceled,
		validSuggestionRequest("index=main | he"),
	); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("Suggest(canceled) = (%#v, %v)", result, err)
	}
	//nolint:staticcheck // SA1012: this explicitly verifies the nil-context guard.
	nilResult, nilErr := service.Suggest(
		nil,
		validSuggestionRequest("index=main | he"),
	)
	if nilErr == nil || !reflect.DeepEqual(nilResult, Result{}) {
		t.Fatalf("Suggest(nil) = (%#v, %v)", nilResult, nilErr)
	}
	//nolint:staticcheck // SA1012: this explicitly verifies the nil-context guard.
	nilCloseErr := service.Close(nil)
	if nilCloseErr == nil {
		t.Fatal("Close(nil) unexpectedly succeeded")
	}
	var nilService *Service
	if err := nilService.Close(context.Background()); err != nil {
		t.Fatalf("nil Service.Close() error = %v", err)
	}
}

func TestCloseCanRetryAfterDeadlineWithUncooperativeDependency(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDependency := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseDependency()
	validator := &suggestionTestValidator{validate: func(
		_ context.Context,
		request searchjobs.ValidateRequest,
	) (searchjobs.ValidationResult, error) {
		close(started)
		<-release
		return validSuggestionValidationRequest(request), nil
	}}
	service := mustSuggestionService(t, Config{
		Validator: validator,
		Scopes:    &suggestionTestScopes{failOnCall: true},
		Compiler:  &suggestionTestCompiler{failOnCall: true},
		Executor:  &suggestionTestExecutor{failOnCall: true},
	})
	suggestDone := make(chan error, 1)
	go func() {
		_, err := service.Suggest(context.Background(), validSuggestionRequest("index=main | he"))
		suggestDone <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("Suggest() did not enter validator")
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelClose()
	if err := service.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close(short deadline) error = %v, want deadline", err)
	}
	releaseDependency()
	select {
	case err := <-suggestDone:
		if !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("Suggest() error = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Suggest() did not finish after dependency release")
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("Close(retry) error = %v", err)
	}
}

type suggestionTestValidator struct {
	mu         sync.Mutex
	result     searchjobs.ValidationResult
	err        error
	calls      int
	failOnCall bool
	validate   func(context.Context, searchjobs.ValidateRequest) (searchjobs.ValidationResult, error)
}

func (validator *suggestionTestValidator) Validate(
	ctx context.Context,
	request searchjobs.ValidateRequest,
) (searchjobs.ValidationResult, error) {
	validator.mu.Lock()
	validator.calls++
	failOnCall := validator.failOnCall
	validate := validator.validate
	result := validator.result
	err := validator.err
	validator.mu.Unlock()
	if failOnCall {
		panic("unexpected validator call")
	}
	if validate != nil {
		return validate(ctx, request)
	}
	return result, err
}

type suggestionTestScopes struct {
	mu         sync.Mutex
	snapshot   searchjobs.AnalysisScopeSnapshot
	err        error
	calls      int
	failOnCall bool
	request    searchjobs.AnalysisScopeRequest
}

func (scopes *suggestionTestScopes) SnapshotAnalysisScope(
	_ context.Context,
	request searchjobs.AnalysisScopeRequest,
) (searchjobs.AnalysisScopeSnapshot, error) {
	scopes.mu.Lock()
	defer scopes.mu.Unlock()
	scopes.calls++
	if scopes.failOnCall {
		panic("unexpected scope snapshot call")
	}
	scopes.request = request
	return scopes.snapshot, scopes.err
}

type suggestionTestCompiler struct {
	mu         sync.Mutex
	result     clickhouse.CompiledFieldSuggestions
	err        error
	calls      int
	failOnCall bool
	compile    func(*plan.Query, clickhouse.FieldSuggestionSpec) (clickhouse.CompiledFieldSuggestions, error)
}

func (compiler *suggestionTestCompiler) CompileFieldSuggestions(
	logical *plan.Query,
	spec clickhouse.FieldSuggestionSpec,
) (clickhouse.CompiledFieldSuggestions, error) {
	compiler.mu.Lock()
	compiler.calls++
	failOnCall := compiler.failOnCall
	compile := compiler.compile
	result := compiler.result
	err := compiler.err
	compiler.mu.Unlock()
	if failOnCall {
		panic("unexpected compiler call")
	}
	if compile != nil {
		return compile(logical, spec)
	}
	if result.SQL == "" {
		result = clickhouse.CompiledFieldSuggestions{SQL: "SELECT names", Spec: spec}
	}
	return result, err
}

type suggestionTestExecutor struct {
	mu         sync.Mutex
	result     queryexec.FieldSuggestionResult
	err        error
	calls      int
	failOnCall bool
	execute    func(
		context.Context,
		clickhouse.CompiledFieldSuggestions,
	) (queryexec.FieldSuggestionResult, error)
}

func (executor *suggestionTestExecutor) ExecuteFieldSuggestions(
	ctx context.Context,
	compiled clickhouse.CompiledFieldSuggestions,
) (queryexec.FieldSuggestionResult, error) {
	executor.mu.Lock()
	executor.calls++
	failOnCall := executor.failOnCall
	execute := executor.execute
	result := executor.result
	err := executor.err
	executor.mu.Unlock()
	if failOnCall {
		panic("unexpected executor call")
	}
	if execute != nil {
		return execute(ctx, compiled)
	}
	return result, err
}

func testConfig() Config {
	return Config{
		Validator: &suggestionTestValidator{result: searchjobs.ValidationResult{
			Diagnostics: []searchjobs.Diagnostic{{
				Code: "SPL_EXPECTED", Message: "keep editing",
				ByteOffset: 0, EndByteOffset: 0, Line: 1, Column: 1, EndLine: 1, EndColumn: 1,
			}},
		}},
		Scopes:   &suggestionTestScopes{failOnCall: true},
		Compiler: &suggestionTestCompiler{failOnCall: true},
		Executor: &suggestionTestExecutor{failOnCall: true},
	}
}

func mustSuggestionService(t *testing.T, config Config) *Service {
	t.Helper()
	service, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return service
}

func validSuggestionRequest(source string) Request {
	resolved, err := searchtime.NewAbsoluteRange(
		time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.July, 2, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		panic(err)
	}
	return Request{
		SPL:                       source,
		CursorByteOffset:          len(source),
		TenantID:                  "tenant-a",
		AuthorizedIndexes:         []string{"main", "metrics"},
		AuthorizedIndexCandidates: []string{"main", "metrics"},
		TimeRange:                 resolved,
	}
}

func validSuggestionValidator() *suggestionTestValidator {
	return &suggestionTestValidator{validate: func(
		_ context.Context,
		request searchjobs.ValidateRequest,
	) (searchjobs.ValidationResult, error) {
		return validSuggestionValidationRequest(request), nil
	}}
}

func validSuggestionValidation(request Request) searchjobs.ValidationResult {
	return validSuggestionValidationRequest(searchjobs.ValidateRequest{
		SPL:               request.SPL,
		TenantID:          request.TenantID,
		AuthorizedIndexes: request.AuthorizedIndexes,
		RequestedIndexes:  request.RequestedIndexes,
		TimeRange:         request.TimeRange,
	})
}

func validSuggestionValidationRequest(
	request searchjobs.ValidateRequest,
) searchjobs.ValidationResult {
	indexes := request.AuthorizedIndexes
	if len(request.RequestedIndexes) != 0 {
		indexes = request.RequestedIndexes
	}
	indexes = slices.Clone(indexes)
	slices.Sort(indexes)
	indexes = slices.Compact(indexes)
	return searchjobs.ValidationResult{
		Valid:               true,
		NormalizedSPL:       strings.TrimSpace(request.SPL),
		ReferencedIndexes:   indexes,
		PredictedResultKind: searchjobs.ValidationResultKindEvents,
	}
}

func validSuggestionSnapshot(
	t *testing.T,
	request Request,
) searchjobs.AnalysisScopeSnapshot {
	t.Helper()
	anchor := time.Date(2026, time.July, 2, 1, 2, 3, 4, time.UTC)
	return searchjobs.AnalysisScopeSnapshot{
		TenantID:          request.TenantID,
		AuthorizedIndexes: slices.Clone(request.AuthorizedIndexes),
		RequestedIndexes:  slices.Clone(request.RequestedIndexes),
		TimeRange:         request.TimeRange,
		SearchStart:       anchor,
		IndexTimeCutoff:   anchor,
		VisibilityCutoff:  73,
	}
}

func activeDiagnostic(source, code string) searchjobs.Diagnostic {
	start := strings.LastIndex(source, "ho")
	if start < 0 {
		start = len(source)
	}
	return searchjobs.Diagnostic{
		Code: code, Message: "keep editing",
		ByteOffset: start, EndByteOffset: len(source),
		Line: 1, Column: start + 1, EndLine: 1, EndColumn: len(source) + 1,
	}
}

func hasSuggestion(suggestions []spl.Suggestion, kind spl.SuggestionKind, label string) bool {
	for _, suggestion := range suggestions {
		if suggestion.Kind == kind && suggestion.Label == label {
			return true
		}
	}
	return false
}

func fmtError(sentinel error, detail string) error {
	return errors.Join(sentinel, errors.New(detail))
}

func repeatedFields(count int) []string {
	fields := make([]string, count)
	for index := range fields {
		fields[index] = "ho" + strings.Repeat("x", index+1)
	}
	return fields
}
