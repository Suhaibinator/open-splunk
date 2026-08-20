package gradethiscorpus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector"
)

func TestMigrationFixtureIsDeterministicSanitizedAndPinned(t *testing.T) {
	t.Parallel()

	first := MigrationFixture()
	second := MigrationFixture()
	if err := ValidateMigration(first); err != nil {
		t.Fatalf("ValidateMigration(MigrationFixture()): %v", err)
	}
	if !bytes.Equal(first.NDJSON, second.NDJSON) {
		t.Fatal("MigrationFixture() did not return byte-identical NDJSON")
	}
	sum := sha256.Sum256(first.NDJSON)
	const wantSHA256 = "a2aa07d18a75b2a2624ee057a1dc9f7f2c207c895235346541600eab541b505a"
	if got := hex.EncodeToString(sum[:]); got != wantSHA256 {
		t.Fatalf("migration fixture SHA-256 = %s, want %s", got, wantSHA256)
	}

	for lineNumber, line := range bytes.Split(bytes.TrimSuffix(first.NDJSON, []byte{'\n'}), []byte{'\n'}) {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			t.Fatalf("decode migration line %d: %v", lineNumber+1, err)
		}
		for _, collectorOwned := range []string{
			"index", "index_name", "host", "source", "sourcetype", "service", "environment",
		} {
			if _, exists := raw[collectorOwned]; exists {
				t.Fatalf("migration line %d embeds collector-owned field %q", lineNumber+1, collectorOwned)
			}
		}
	}
}

func TestMigrationFixtureAtRebasesPinnedSemantics(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2031, time.February, 3, 4, 5, 6, 789_000_000, time.FixedZone("caller", 2*60*60))
	first := MigrationFixtureAt(baseTime)
	second := MigrationFixtureAt(baseTime)
	if err := ValidateMigration(first); err != nil {
		t.Fatalf("ValidateMigration(MigrationFixtureAt()): %v", err)
	}
	if !first.BaseTime.Equal(baseTime) || first.BaseTime.Location() != time.UTC {
		t.Fatalf("rebased migration base time = %s (%s), want instant %s in UTC", first.BaseTime, first.BaseTime.Location(), baseTime)
	}
	if !bytes.Equal(first.NDJSON, second.NDJSON) {
		t.Fatal("MigrationFixtureAt returned different bytes for the same instant")
	}
	if bytes.Equal(first.NDJSON, MigrationFixture().NDJSON) {
		t.Fatal("rebased migration fixture retained the pinned fixture timestamps")
	}
	for _, event := range first.Events {
		if !event.Timestamp.Equal(first.BaseTime.Add(event.Offset)) {
			t.Fatalf("rebased migration event %q timestamp = %s, want offset %s", event.ID, event.Timestamp, event.Offset)
		}
	}
}

func TestMigrationSearchManifestRendersCurrentSourceInvestigations(t *testing.T) {
	t.Parallel()

	fixture := MigrationFixture()
	searches := MigrationSearches()
	wantIDs := []MigrationSearchID{
		MigrationSearchFollowTrace,
		MigrationSearchSeverityCounts,
		MigrationSearchFailedRequests,
		MigrationSearchPathStatus,
		MigrationSearchDurationUnits,
		MigrationSearchTopMessages,
	}
	if len(searches) != len(wantIDs) {
		t.Fatalf("migration searches = %d, want %d", len(searches), len(wantIDs))
	}
	seen := make(map[MigrationSearchID]struct{}, len(searches))
	for index, search := range searches {
		if search.ID != wantIDs[index] {
			t.Fatalf("migration search %d ID = %q, want %q", index, search.ID, wantIDs[index])
		}
		if _, duplicate := seen[search.ID]; duplicate {
			t.Fatalf("duplicate migration search ID %q", search.ID)
		}
		if search.ExpectedRows == 0 {
			t.Fatalf("migration search %q has no expected row count", search.ID)
		}
		seen[search.ID] = struct{}{}
		source, err := search.Render(fixture.TraceID)
		if err != nil {
			t.Fatalf("Render(%q): %v", search.ID, err)
		}
		if source == "" || strings.Contains(source, migrationTracePlaceholder) {
			t.Fatalf("rendered migration search %q = %q", search.ID, source)
		}
	}
	if _, err := searches[0].Render(`bad" | head 0`); err == nil {
		t.Fatal("migration Render accepted an unsafe trace identifier")
	}
	searches[0].Template = "mutated"
	if MigrationSearches()[0].Template == "mutated" {
		t.Fatal("MigrationSearches returned storage aliased to its caller")
	}
}

func TestMigrationFixtureDecodesAsCurrentGradeThisSource(t *testing.T) {
	t.Parallel()

	fixture := MigrationFixture()
	decoder, err := collector.NewDecoder(collector.DecodeConfig{
		Format: collector.InputFormatNDJSON, InputID: "gradethis-migration",
		IndexName: MigrationIndexName, Source: MigrationSource,
		Sourcetype: MigrationSourcetype, Host: "gradethis-test-host",
		Service: MigrationService,
	})
	if err != nil {
		t.Fatal(err)
	}

	offset := uint64(0)
	var sawRoot, sawRequest bool
	for index, expected := range fixture.Events {
		end := offset + uint64(len(expected.RawLine))
		event, decodeErr := decoder.Decode(expected.RawLine, collector.SourcePosition{
			FileIdentity: "gradethis-migration", SourcePath: "gradethis.log",
			FileFingerprintLength: 4096, StartOffset: offset, EndOffset: end,
			LineNumber: uint64(index + 1), NextLineNumber: uint64(index + 2),
		}, fixture.BaseTime)
		if decodeErr != nil {
			t.Fatalf("Decode(%q): %v", expected.ID, decodeErr)
		}
		if !event.GetEventTime().AsTime().Equal(fixture.BaseTime.Add(expected.Offset)) ||
			event.GetLevel() != expected.Level ||
			event.GetMessage() != expected.Message ||
			event.GetTraceId() != expected.TraceID ||
			event.GetIndexName() != MigrationIndexName ||
			event.GetSource() != MigrationSource ||
			event.GetSourcetype() != MigrationSourcetype ||
			event.GetService() != MigrationService ||
			!bytes.Equal(event.GetRaw(), expected.RawLine) {
			t.Fatalf("%q decoded canonical event = %+v", expected.ID, event)
		}

		fields := make(map[string]*opensplunk.TypedValue, len(event.GetFields().GetFields()))
		for _, field := range event.GetFields().GetFields() {
			fields[field.GetName()] = field.GetValue()
		}
		for _, collectorOwned := range []string{
			"index", "index_name", "host", "source", "sourcetype", "service", "environment",
		} {
			if _, exists := fields[collectorOwned]; exists {
				t.Fatalf("%q decoded payload retained collector-owned field %q", expected.ID, collectorOwned)
			}
		}
		if expected.Request {
			sawRequest = true
			if fields["path"].GetStringValue() != expected.Path ||
				fields["status"].GetSint64Value() != expected.Status ||
				fields["duration"].GetStringValue() != expected.Duration ||
				fields["bytes"].GetSint64Value() != int64(expected.Bytes) {
				t.Fatalf("%q decoded request fields = %+v", expected.ID, fields)
			}
		}
		if expected.Logger == "" && expected.Layer == "" {
			sawRoot = true
			if _, exists := fields["logger"]; exists {
				t.Fatalf("%q sparse root unexpectedly has logger", expected.ID)
			}
			if _, exists := fields["layer"]; exists {
				t.Fatalf("%q sparse root unexpectedly has layer", expected.ID)
			}
			if !fields["healthy"].GetBoolValue() ||
				fields["optional_note"].GetNullValue() != opensplunk.NullValue_NULL_VALUE_NULL ||
				fields["details"].GetObjectValue() == nil {
				t.Fatalf("%q sparse root typed fields = %+v", expected.ID, fields)
			}
		}
		offset = end + 1
	}
	if !sawRequest || !sawRoot {
		t.Fatalf("migration decoder coverage = request:%t sparse-root:%t", sawRequest, sawRoot)
	}
}
