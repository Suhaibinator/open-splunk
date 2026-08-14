package queryexec

import (
	"bytes"
	"os"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse
// composes every v0.3 command while an unrelated semantic-BINARY _raw value is
// carried through makemv/mvexpand row copying. Both expanded rows must retain
// Bytes identity even though the payload is valid UTF-8 at publication time.
func TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	earliest := time.Date(2026, time.August, 12, 23, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	wantRaw := []byte("binary-valid-utf8-界")
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, []semanticBytesLineageEvent{{
		id:       "sdet-v03-binary",
		at:       earliest.Add(time.Minute),
		raw:      wantRaw,
		encoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY,
		source:   "lineage",
	}})
	page := semanticBytesLineageRunManagerSearch(
		t,
		ctx,
		executor,
		indexTime,
		"sdet-v03-all-ten-bytes",
		`index=`+semanticBytesLineageIndex+` event_id="sdet-v03-binary"`+
			` | regex source="lineage"`+
			` | sort 0 +event_id | reverse`+
			` | eval n=1 | accum n AS running`+
			` | strcat source ":" source endpoint`+
			` | addinfo | fillnull value="x" optional`+
			` | addtotals fieldname=total running`+
			` | delta running AS step`+
			` | eval tags="a,b" | makemv delim="," tags`+
			` | mvexpand tags | reverse | table event_id tags _raw`,
		earliest,
		latest,
	)

	semanticBytesLineageRequireSchema(t, page, []string{"event_id", "tags", "_raw"})
	if page.Schema.Columns[2].Kind != searchjobs.ValueKindMixed ||
		!page.Schema.Columns[2].Nullable {
		t.Fatalf("_raw schema = %#v, want nullable Mixed String/Bytes", page.Schema.Columns[2])
	}
	if len(page.Rows) != 2 || !page.Complete || page.TotalRows != 2 {
		t.Fatalf(
			"page = rows %d total %d complete=%t, want two complete rows",
			len(page.Rows),
			page.TotalRows,
			page.Complete,
		)
	}
	for index, wantTag := range []string{"b", "a"} {
		row := page.Rows[index]
		if row.Ordinal != uint64(index) {
			t.Fatalf("row %d ordinal = %d, want %d", index, row.Ordinal, index)
		}
		id, idOK := row.Values[0].String()
		tag, tagOK := row.Values[1].String()
		gotRaw, rawOK := row.Values[2].Bytes()
		if !idOK || id != "sdet-v03-binary" || !tagOK || tag != wantTag ||
			!rawOK || !bytes.Equal(gotRaw, wantRaw) {
			t.Fatalf("row %d public values did not preserve all-ten order and Bytes identity: %#v", index, row)
		}
	}
}
