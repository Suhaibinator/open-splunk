package clickhouse

import (
	"reflect"
	"testing"
)

func TestCompileSearchMembershipMatchesSearchComparisons(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ membership, comparison string }{
		{`index="gradethis" build_id="v1.1.3" (level IN ("WARN")) message="Request summary statistics"`, `index="gradethis" build_id="v1.1.3" level="WARN" message="Request summary statistics"`},
		{`index=gradethis level IN (WARN, "error", "W*")`, `index=gradethis (level=WARN OR level="error" OR level="W*")`},
		{`index=gradethis NOT level IN (WARN, ERROR)`, `index=gradethis NOT (level=WARN OR level=ERROR)`},
		{`index=gradethis status IN (400, 5*)`, `index=gradethis (status=400 OR status=5*)`},
		{`index IN (gradethis)`, `index=gradethis`},
		{`index=gradethis | search level IN (WARN, ERROR)`, `index=gradethis | search (level=WARN OR level=ERROR)`},
		{`index=gradethis level IN (WARN, ERROR) OR status=500 host=api`, `index=gradethis (level=WARN OR level=ERROR) OR status=500 host=api`},
		{`index=gradethis custom IN ("'); DROP TABLE events; --", "a,b")`, `index=gradethis (custom="'); DROP TABLE events; --" OR custom="a,b")`},
	} {
		t.Run(test.membership, func(t *testing.T) {
			got, want := compileSPL(t, test.membership), compileSPL(t, test.comparison)
			if got.SQL != want.SQL || !reflect.DeepEqual(got.Args, want.Args) {
				t.Fatalf("membership differs from comparisons:\n%s\n%#v\nwant:\n%s\n%#v", got.SQL, got.Args, want.SQL, want.Args)
			}
		})
	}
}
