package spl

import "testing"

// SPL bin timescales: https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.1/search-commands/bin
func TestParseBinSupportedUnitAliases(t *testing.T) {
	t.Parallel()
	for _, unit := range []struct {
		canonical string
		aliases   []string
	}{
		{"s", []string{"sec", "secs", "second", "seconds"}},
		{"m", []string{"min", "mins", "minute", "minutes"}},
		{"h", []string{"hr", "hrs", "hour", "hours"}},
		{"d", []string{"day", "days"}},
	} {
		for _, command := range []string{"bin", "bucket"} {
			canonical, err := Parse(`index=main | ` + command + ` _time span=1` + unit.canonical)
			if err != nil {
				t.Fatal(err)
			}
			want := canonical.Commands[0].(*BinCommand).Span
			for _, alias := range unit.aliases {
				t.Run(command+"/"+alias, func(t *testing.T) {
					source := `index=main | ` + command + ` _time span=1` + alias
					query, err := Parse(source)
					if err != nil {
						t.Fatal(err)
					}
					got := query.Commands[0].(*BinCommand).Span
					if got.Kind != want.Kind || got.Unit != want.Unit || got.Magnitude != want.Magnitude {
						t.Fatalf("span=%#v want %#v", got, want)
					}
					if text := source[got.Range.Start.Offset:got.Range.End.Offset]; text != "1"+alias {
						t.Fatalf("source span=%q", text)
					}
				})
			}
		}
	}
}
