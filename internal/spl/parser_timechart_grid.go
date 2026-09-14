package spl

import "fmt"

func (p *parser) parseTimechartGridOption(axis *TimechartAxisOptions, name string, option token) error {
	var value *bool
	var specified *bool
	var sourceRange *Range
	switch name {
	case "cont":
		value, specified, sourceRange = &axis.Cont, &axis.ContSpecified, &axis.ContRange
	case "partial":
		value, specified, sourceRange = &axis.Partial, &axis.PartialSpecified, &axis.PartialRange
	case "fixedrange":
		value, specified, sourceRange = &axis.FixedRange, &axis.FixedRangeSpecified, &axis.FixedRangeRange
	case "aligntime":
		specified, sourceRange = &axis.AlignTimeSpecified, &axis.AlignTimeRange
	}
	if *specified {
		return p.unsupportedTimechartSyntax(option, fmt.Sprintf("timechart option %q is repeated", name))
	}
	p.advance()
	p.advance()
	tok := p.current()
	if name == "aligntime" {
		if tok.kind != tokenWord && tok.kind != tokenString {
			return p.unsupportedTimechartSyntax(tok, "timechart aligntime requires earliest, latest, an epoch timestamp, or a relative-time specifier")
		}
		axis.AlignTime = tok.text
	} else {
		parsed, ok := parseStrictBool(tok.text)
		if tok.kind != tokenWord || !ok {
			return p.unsupportedTimechartSyntax(tok, "timechart "+name+" must be true or false")
		}
		*value = parsed
	}
	*specified = true
	*sourceRange = Range{Start: option.sourceRange.Start, End: tok.sourceRange.End}
	p.advance()
	return nil
}
