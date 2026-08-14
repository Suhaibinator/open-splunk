package clickhouse

import (
	"strings"
	"testing"
)

func TestCompileSemanticBytesStrcatUsesDynamicEnvelopeOnly(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{
		` | table output`,
		` | fillnull value="x" output | table output`,
	} {
		compiled := compileSPL(
			t,
			`index=gradethis | strcat "<" _raw ">" output`+suffix,
		)
		if len(compiled.StringOrBytesOutputs) != 0 {
			t.Fatalf(
				"Dynamic bytes/v1 strcat retained fixed-String descriptors: %#v",
				compiled.StringOrBytesOutputs,
			)
		}
		if !strings.Contains(compiled.SQL, "__os_strcat_published_value") ||
			!strings.Contains(compiled.SQL, "base64Encode") {
			t.Fatalf("strcat omitted semantic Bytes Dynamic envelope:\n%s", compiled.SQL)
		}
		if strings.Contains(compiled.SQL, "ifNull(, 0)") {
			t.Fatalf("strcat retained an empty fixed-String text proof:\n%s", compiled.SQL)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("strcat placeholders = %d, args = %d", got, want)
		}
	}
}

func TestCompileSemanticBytesConcatUsesNullableTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval lowered=case(raw_encoding=1,"x")`+
			` | eval output=_raw . lowered | table output`,
	)
	outputs, valid := compiled.ValidatedResultStringOrBytesOutputs()
	if !valid || len(outputs) != 1 || outputs[0].OutputIndex != 0 ||
		!outputs[0].Nullable {
		t.Fatalf("String-or-Bytes outputs = %#v, valid=%t, want one nullable output", outputs, valid)
	}
	if !strings.Contains(compiled.SQL, "AS Nullable(String)") {
		t.Fatalf("byte-capable concat was not normalized to Nullable(String):\n%s", compiled.SQL)
	}
}

func TestCompileSemanticBytesModeCarriesWinnerTypeAndTextGuard(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats mode(_raw) AS modal BY host`+
			` | rename modal AS renamed | eval lowered=lower(renamed)`+
			` | table renamed lowered`,
	)
	outputs, valid := compiled.ValidatedResultStringOrBytesOutputs()
	if !valid || len(outputs) != 1 || outputs[0].OutputIndex != 0 ||
		!outputs[0].Nullable {
		t.Fatalf("mode String-or-Bytes outputs = %#v, valid=%t", outputs, valid)
	}
	for _, required := range []string{
		`__os_measure_mode_values_`,
		`__os_measure_semantic_bytes_`,
		`__os_mode_semantic_bytes_`,
		`hex(`,
		`unhex(`,
		`isValidUTF8(assumeNotNull("renamed"))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("semantic mode SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("mode placeholders = %d, args = %d", got, want)
	}
}

func TestCompileSemanticBytesNullableModeSurvivesStatsByRegroup(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats mode(_raw) AS modal BY service`+
			` | stats count BY modal | table modal count`,
	)
	outputs, valid := compiled.ValidatedResultStringOrBytesOutputs()
	if !valid || len(outputs) != 1 || outputs[0].OutputIndex != 0 ||
		!outputs[0].Nullable {
		t.Fatalf("regrouped mode outputs = %#v, valid=%t, want nullable modal", outputs, valid)
	}
}

func TestCompileSemanticBytesConditionalProofRebindsAcrossProjection(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`if(service=service,modal,null)`,
		`case(service=service,modal)`,
		`coalesce(null,modal)`,
		`modal . ""`,
	} {
		compiled := compileSPL(
			t,
			`index=gradethis | stats mode(_raw) AS modal BY service`+
				` | eval selected=`+expression+
				` | table selected | eval lowered=lower(selected)`+
				` | table selected lowered`,
		)
		outputs, valid := compiled.ValidatedResultStringOrBytesOutputs()
		if !valid || len(outputs) != 1 || outputs[0].OutputIndex != 0 {
			t.Fatalf("%s outputs = %#v, valid=%t", expression, outputs, valid)
		}
		if !strings.Contains(
			compiled.SQL,
			`isValidUTF8(assumeNotNull("selected"))`,
		) {
			t.Fatalf("%s did not rebind its text proof to selected:\n%s", expression, compiled.SQL)
		}
		if strings.Contains(
			compiled.SQL,
			`isValidUTF8(assumeNotNull("modal"))`,
		) {
			t.Fatalf("%s retained an out-of-scope modal text proof:\n%s", expression, compiled.SQL)
		}
	}
}

func TestCompileChronologicalChartRetainsSemanticRowSidecar(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats count AS peers` +
			` | chart count OVER _raw BY level`,
		`index=gradethis | eventstats count AS peers` +
			` | chart avg(peers) OVER _raw BY level`,
	} {
		compiled := compileSPL(t, source)
		if compiled.Chart == nil || !compiled.Chart.RowSemanticBytes {
			t.Fatalf("chart = %#v, want semantic row sidecar", compiled.Chart)
		}
		if !strings.Contains(compiled.SQL, quoteIdentifier(ChartRowSemanticBytesColumn)) {
			t.Fatalf("chronological wrapper dropped semantic row sidecar:\n%s", compiled.SQL)
		}
	}
}
