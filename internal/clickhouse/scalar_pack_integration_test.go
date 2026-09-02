package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// scanScalarPackRow executes one compiled query that returns exactly one row
// and scans it into the supplied destinations.
func scanScalarPackRow(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	label string,
	compiled CompiledQuery,
	destinations ...any,
) {
	t.Helper()
	if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(destinations...); err != nil {
		t.Fatalf("execute %s: %v\nSQL: %s\nargs: %#v", label, err, compiled.SQL, compiled.Args)
	}
}

func scalarPackText(value *string) string {
	if value == nil {
		return "<null>"
	}
	return *value
}

func requireScalarPackTexts(t *testing.T, label string, got []*string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s returned %d columns, want %d", label, len(got), len(want))
	}
	for index := range want {
		if scalarPackText(got[index]) != want[index] {
			rendered := make([]string, len(got))
			for column := range got {
				rendered[column] = scalarPackText(got[column])
			}
			t.Fatalf("%s column %d = %q, want %q\nall: %q", label, index, scalarPackText(got[index]), want[index], rendered)
		}
	}
}

func testScalarPackAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("scalar-pack-values", "scalarpack", indexTime)
	event.Event.Host = "  padded\t"
	event.Event.Raw = []byte("raw text")
	event.Event.Fields = typedObjectValue(
		typedField("negative", typedSint(-16)),
		typedField("positive", typedDouble(1000)),
		typedField("numeric_text", typedString("100")),
		typedField("zero", typedSint(0)),
		typedField("decimal", typedDecimal("8.0000")),
		typedField("amount", typedDouble(12345.6789)),
		typedField("half", typedDouble(0.5)),
		typedField("big", typedSint(1234567)),
		typedField("seconds", typedSint(615)),
		typedField("long_seconds", typedSint(100000)),
		typedField("wrapped", typedString("--name__")),
		typedField("encoded", typedString("a%20b+c%C3%BC")),
		typedField("broken", typedString("%FF")),
		typedField("word", typedString("abc")),
		typedField("csv", typedString(" a , b ")),
		typedField("multi", typedList(typedString("abc"), typedString(""))),
		typedField("yes", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField("ip4", typedString("10.1.2.3")),
		typedField("ip6", typedString("2001:db8::1")),
		typedField("junk", typedString("not-an-ip")),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("unchanged"))),
		),
	)
	other := testStoredEvent("scalar-pack-other", "scalarpack", indexTime)
	other.Event.Fields = typedObjectValue(
		typedField("ip4", typedString("172.16.0.1")),
		typedField("ip6", typedString("2001:db9::1")),
	)

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"scalarpack",
		"scalar-pack-batch",
		119,
		event,
		other,
	)

	numeric := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval absolute=abs(negative), root=sqrt(negative), text_root=sqrt(numeric_text), unit=exp(zero), ln_zero=ln(zero), log_ten=round(log(positive), 6), log_base=round(log(decimal, 2), 6), log_unit_base=log(positive, 1), complex=pow(negative, 0.5), square=pow(negative, 2), circle=round(pi(), 5)` +
			` | table absolute,root,text_root,unit,ln_zero,log_ten,log_base,log_unit_base,complex,square,circle`,
	)
	var absolute, root, textRoot, unit, lnZero, logTen, logBase, logUnitBase, complexPower, square, circle *float64
	scanScalarPackRow(
		t, queryContext, connection, "scalar pack math", numeric,
		&absolute, &root, &textRoot, &unit, &lnZero, &logTen, &logBase, &logUnitBase, &complexPower, &square, &circle,
	)
	for name, got := range map[string]*float64{
		"root": root, "ln_zero": lnZero, "log_unit_base": logUnitBase, "complex": complexPower,
	} {
		if got != nil {
			t.Fatalf("%s = %v, want null outside the function domain", name, *got)
		}
	}
	for name, test := range map[string]struct {
		got  *float64
		want float64
	}{
		"absolute":  {absolute, 16},
		"text_root": {textRoot, 10},
		"unit":      {unit, 1},
		"log_ten":   {logTen, 3},
		"log_base":  {logBase, 3},
		"square":    {square, 256},
		"circle":    {circle, 3.14159},
	} {
		if test.got == nil || *test.got != test.want {
			t.Fatalf("%s = %v, want %v", name, test.got, test.want)
		}
	}

	text := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval both=trim(host), left=ltrim(host), right=rtrim(host), custom=trim(wrapped, "-_"), raw_trimmed=trim(_raw, "rt"), decoded=urldecode(encoded), kept=urldecode(broken), digest_md5=md5(word), digest_sha1=sha1(word), digest_sha256=sha256(word), digest_sha512=sha512(word), missing=md5(absent), unchanged=nullif(tostring(word), "zzz"), cleared=nullif(tostring(word), "abc")` +
			` | table both,left,right,custom,raw_trimmed,decoded,kept,digest_md5,digest_sha1,digest_sha256,digest_sha512,missing,unchanged,cleared`,
	)
	texts := make([]*string, 14)
	destinations := make([]any, len(texts))
	for index := range texts {
		destinations[index] = &texts[index]
	}
	scanScalarPackRow(t, queryContext, connection, "scalar pack text", text, destinations...)
	requireScalarPackTexts(t, "scalar pack text", texts, []string{
		"padded",
		"padded\t",
		"  padded",
		"name",
		"aw tex",
		"a b+cü",
		"%FF",
		"900150983cd24fb0d6963f7d28e17f72",
		"a9993e364706816aba3e25717850c26c9cd0d89d",
		"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
		"<null>",
		"abc",
		"<null>",
	})

	multivalue := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval parts=trim(split(csv, ",")), digests=md5(multi)` +
			` | table parts,digests`,
	)
	multivalueControl := "SELECT parts, dynamicType(digests), " +
		"dynamicElement(digests, 'Array(String)') FROM (" + multivalue.SQL + ")"
	var parts, digests []string
	var digestType string
	if err := connection.QueryRow(
		queryContext,
		multivalueControl,
		multivalue.Args...,
	).Scan(&parts, &digestType, &digests); err != nil {
		t.Fatalf(
			"execute scalar pack multivalue: %v\nSQL: %s\nargs: %#v",
			err,
			multivalueControl,
			multivalue.Args,
		)
	}
	if !slices.Equal(parts, []string{"a", "b"}) || digestType != "Array(String)" ||
		!slices.Equal(digests, []string{
			"900150983cd24fb0d6963f7d28e17f72",
			"d41d8cd98f00b204e9800998ecf8427e",
		}) {
		t.Fatalf("scalar pack multivalue = %#v %q/%#v", parts, digestType, digests)
	}

	kinds := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval k_word=typeof(word), k_number=typeof(negative), k_decimal=typeof(decimal), k_text=typeof(numeric_text), k_bool=typeof(yes), k_null=typeof(nothing), k_absent=typeof(absent), k_time=typeof(_time), k_host=typeof(host), k_predicate=typeof(isnull(absent)), k_object=typeof(object_value), k_list=typeof(multi), k_literal=typeof(2.5)` +
			` | table k_word,k_number,k_decimal,k_text,k_bool,k_null,k_absent,k_time,k_host,k_predicate,k_object,k_list,k_literal`,
	)
	kindTexts := make([]*string, 13)
	kindDestinations := make([]any, len(kindTexts))
	for index := range kindTexts {
		kindDestinations[index] = &kindTexts[index]
	}
	scanScalarPackRow(t, queryContext, connection, "scalar pack typeof", kinds, kindDestinations...)
	requireScalarPackTexts(t, "scalar pack typeof", kindTexts, []string{
		// A flattened object leaves no scalar at its own path, so it is absent
		// like any missing field; a stored list is the unsupported runtime type
		// that yields null.
		"String", "Number", "Number", "Number", "Boolean", "Invalid", "Invalid",
		"Number", "String", "Boolean", "Invalid", "<null>", "Number",
	})

	cidr := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval private=if(cidrmatch("10.1.2.3/8", ip4), "in", "out"), other_block=if(cidrmatch("192.168.0.0/16", ip4), "in", "out"), lab=if(cidrmatch("2001:db8::/32", ip6), "in", "out"), family=if(cidrmatch("2001:db8::/32", ip4), "in", "out"), text=if(cidrmatch("0.0.0.0/0", junk), "in", "out"), absent=if(cidrmatch("10.0.0.0/8", nothing), "in", "out"), missing=if(cidrmatch("10.0.0.0/8", absent), "in", "out")` +
			` | table private,other_block,lab,family,text,absent,missing`,
	)
	cidrTexts := make([]*string, 7)
	cidrDestinations := make([]any, len(cidrTexts))
	for index := range cidrTexts {
		cidrDestinations[index] = &cidrTexts[index]
	}
	scanScalarPackRow(t, queryContext, connection, "scalar pack cidrmatch", cidr, cidrDestinations...)
	requireScalarPackTexts(t, "scalar pack cidrmatch", cidrTexts, []string{
		"in", "out", "in", "out", "out", "out", "out",
	})
	for _, test := range []struct {
		source string
		want   uint64
	}{
		{source: `index=scalarpack | where cidrmatch("10.0.0.0/8", ip4) | stats count`, want: 1},
		{source: `index=scalarpack | where cidrmatch("172.16.0.0/12", ip4) | stats count`, want: 1},
		{source: `index=scalarpack | where cidrmatch("2001:db8::/31", ip6) | stats count`, want: 2},
		{source: `index=scalarpack | where cidrmatch("10.0.0.0/8", nothing) | stats count`, want: 0},
		{source: `index=scalarpack | where NOT cidrmatch("10.0.0.0/8", ip4) | stats count`, want: 1},
	} {
		predicate := compile(test.source)
		var count uint64
		scanScalarPackRow(t, queryContext, connection, "scalar pack cidrmatch predicate", predicate, &count)
		if count != test.want {
			t.Fatalf("%s = %d, want %d", test.source, count, test.want)
		}
	}

	formats := compile(
		`index=scalarpack event_id="scalar-pack-values"` +
			` | eval grouped=tostring(amount, "commas"), signed=tostring(negative, "commas"), millions=tostring(big, "commas"), from_text=tostring(numeric_text, "commas"), fraction=tostring(half, "commas"), stored_bool=tostring(yes, "commas"), predicate=tostring(isnull(absent), "commas"), empty=tostring(nothing, "commas"), short=tostring(seconds, "duration"), long=tostring(long_seconds, "duration"), invalid=tostring(negative, "duration"), decimal_duration=tostring(decimal, "duration")` +
			` | table grouped,signed,millions,from_text,fraction,stored_bool,predicate,empty,short,long,invalid,decimal_duration`,
	)
	formatTexts := make([]*string, 12)
	formatDestinations := make([]any, len(formatTexts))
	for index := range formatTexts {
		formatDestinations[index] = &formatTexts[index]
	}
	scanScalarPackRow(t, queryContext, connection, "scalar pack tostring formats", formats, formatDestinations...)
	requireScalarPackTexts(t, "scalar pack tostring formats", formatTexts, []string{
		// A stored Bool is an ineligible arithmetic operand and stays null; a
		// Boolean expression keeps the default True/False rendering.
		"12,345.68", "-16", "1,234,567", "100", "0.50", "<null>", "True", "<null>",
		"00:10:15", "1+03:46:40", "<null>", "00:00:08",
	})

	actions := explainCompiledQuery(t, queryContext, connection, explainActionsPrefix, text)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("scalar pack lowering expands event rows:\n%s", actions)
	}
}
