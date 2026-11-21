package spl

import (
	"errors"
	"testing"
)

func TestJoinRejectsUnsupportedSyntaxAtSource(t *testing.T) {
	for _, source := range []string{
		"| join host [ search * | fields host",
		"| join host [ search * | fields host, ]",
		"| join host [ search * | fields - host ]",
		"| join host [ search * | fields host, * ]",
		"| join host [ search * | fields level ]",
		"| join host [ search * | fields host | head 1 ]",
		"| join max=0 host [ search * | fields host ]",
		"| join type=right host [ search * | fields host ]",
		"| join [ search * | fields host ]",
		"| join host [ search * | fields __os_join_events, host ]",
	} {
		query, err := Parse(source)
		var diagnostic *Diagnostic
		if query != nil || !errors.As(err, &diagnostic) || diagnostic.Range.Start.Offset == 0 {
			t.Errorf("Parse(%q) = %v, %v", source, query, err)
		}
	}
}

func TestJoinBracketBoundaries(t *testing.T) {
	query, err := Parse(`[ordinary] | join type=OUTER max=1 host source[search message="a] | b" | fields host,source,service]| head 2`)
	if err != nil {
		t.Fatal(err)
	}
	join := query.Commands[0].(*JoinCommand)
	if !join.Left || len(join.Keys) != 2 || len(join.Fields) != 3 || len(query.Commands) != 2 {
		t.Fatalf("unexpected join: %+v", join)
	}
}
