package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const slowQueryClassificationSPL = `index=shubuhat "GORM slow query"
| eval stmt=case(
    match(sql,"FROM \"crawl_jobs\".*FOR UPDATE"),      "job row lock (FOR UPDATE)",
    match(sql,"FOR SHARE"),                           "claim job lock (FOR SHARE)",
    match(sql,"parent_count=\(SELECT count"),         "parent_count recount",
    match(sql,"INSERT INTO scrape_nodes"),            "discovery node upsert",
    match(sql,"seed_expanded"),                       "seed expansion",
    match(sql,"count\(\*\)|COUNT"),                   "progress counting",
    true(), substr(sql,1,80))
| stats count, avg(elapsed_ms) as avg_ms, perc95(elapsed_ms) as p95_ms, max(elapsed_ms) as max_ms, sum(elapsed_ms) as total_ms by stmt
| sort - total_ms`

func TestCompileSlowQueryClassificationWithTrueFallback(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.AuthorizedIndexes = []string{"shubuhat"}
	compiled := compileSPLWithScope(t, slowQueryClassificationSPL, scope)
	if !strings.Contains(compiled.SQL, "multiIf(") {
		t.Fatalf("classification SQL lacks case branches: %s", compiled.SQL)
	}
	if countArgument(compiled.Args, true) == 0 {
		t.Fatalf("classification lacks Boolean fallback: %#v", compiled.Args)
	}
	if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
		t.Fatalf("placeholder count does not match arguments: %s", compiled.SQL)
	}
}

func TestCompileBooleanConstantConsumersAndForgedArguments(t *testing.T) {
	t.Parallel()

	for _, pipeline := range []string{
		`where true() AND NOT false()`,
		`eval label=case(false(), "never", true(), "fallback")`,
		`eval label=if(true(), "yes", "no"), kind=typeof(false()), text=tostring(true())`,
		`stats count(eval(true())) AS all, count(eval(false())) AS none`,
		`eventstats count(eval(true())) AS all | eventstats count(eval(false())) AS none`,
		`streamstats count(eval(true())) AS all | streamstats count(eval(false())) AS none`,
	} {
		compiled := compileSPL(t, "index=gradethis | "+pipeline)
		if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
			t.Fatalf("unbalanced parameters for %q", pipeline)
		}
	}
	for _, function := range []plan.ScalarFunction{plan.ScalarFunctionTrue, plan.ScalarFunctionFalse} {
		logical := buildPlan(t, `index=gradethis | where isnull(host)`)
		filter := logical.Operators[len(logical.Operators)-1].(*plan.Filter)
		filter.Expression = &plan.ScalarPredicateExpression{Value: &plan.ScalarCallExpression{
			Function: function,
			Arguments: []plan.ScalarExpression{&plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindBool, Bool: true},
			}},
		}}
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatal("compiler accepted a Boolean constant with an argument")
		}
	}
}
