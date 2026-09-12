package searchjobs

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

const (
	// NearbyEventProvenanceVersion identifies the persisted ordinal layout.
	NearbyEventProvenanceVersion = uint32(1)
	nearbyContextRadius          = 5 * time.Minute
)

var ErrNearbyContextUnavailable = errors.New("nearby context is unavailable for this result; rerun the search and try again")

// NearbyEventProvenance binds unchanged physical event fields to a persisted
// result schema. The compiler descriptor is validated before this value is
// installed on a completed Job.
type NearbyEventProvenance struct {
	Version     uint32
	TimeIndex   uint16
	IndexIndex  uint16
	HostIndex   uint16
	SourceIndex uint16
}

// NearbyContextField is one retained scalar that can be used as an exact
// comparison in the nearby-search editor.
type NearbyContextField struct {
	Name      string
	Value     Value
	Suggested bool
}

// NearbyContext contains detached source identity and a clipped half-open
// search interval around one retained event.
type NearbyContext struct {
	AnchorTime time.Time
	Earliest   time.Time
	Latest     time.Time
	Index      string
	Host       string
	Source     string
	Clipped    bool
	Fields     []NearbyContextField
}

func nearbyEventProvenance(output clickhouse.NearbyEventOutput, schema Schema) (*NearbyEventProvenance, bool) {
	provenance := &NearbyEventProvenance{
		Version: NearbyEventProvenanceVersion, TimeIndex: output.TimeIndex,
		IndexIndex: output.IndexIndex, HostIndex: output.HostIndex, SourceIndex: output.SourceIndex,
	}
	if !validNearbyEventProvenance(provenance, schema) {
		return nil, false
	}
	return provenance, true
}

func validNearbyEventProvenance(provenance *NearbyEventProvenance, schema Schema) bool {
	if provenance == nil || provenance.Version != NearbyEventProvenanceVersion {
		return false
	}
	indexes := [...]uint16{
		provenance.TimeIndex, provenance.IndexIndex,
		provenance.HostIndex, provenance.SourceIndex,
	}
	names := [...]string{"_time", "index", "host", "source"}
	kinds := [...]ValueKind{ValueKindTime, ValueKindString, ValueKindString, ValueKindString}
	seen := make(map[uint16]struct{}, len(indexes))
	for index, ordinal := range indexes {
		if int(ordinal) >= len(schema.Columns) {
			return false
		}
		column := schema.Columns[ordinal]
		if column.Name != names[index] || column.Kind != kinds[index] || column.Multivalue {
			return false
		}
		if _, duplicate := seen[ordinal]; duplicate {
			return false
		}
		seen[ordinal] = struct{}{}
	}
	return true
}

// PrepareNearbyContext validates compiler provenance against the exact final
// schema and derives context from one retained row without executing SPL.
func PrepareNearbyContext(
	provenance *NearbyEventProvenance,
	schema Schema,
	row ResultRow,
) (NearbyContext, error) {
	if !validNearbyEventProvenance(provenance, schema) || len(row.Values) != len(schema.Columns) {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	anchor, ok := row.Values[provenance.TimeIndex].Time()
	if !ok || anchor.IsZero() {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	index, indexOK := row.Values[provenance.IndexIndex].String()
	host, hostOK := row.Values[provenance.HostIndex].String()
	source, sourceOK := row.Values[provenance.SourceIndex].String()
	if !indexOK || !hostOK || !sourceOK || index == "" ||
		!utf8.ValidString(index) || !utf8.ValidString(host) || !utf8.ValidString(source) {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	anchor = anchor.Round(0).UTC()
	minimum := clickhouse.MinimumSearchTime()
	maximum := clickhouse.MaximumSearchTime()
	if anchor.Before(minimum) || anchor.After(maximum) {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	earliest := anchor.Add(-nearbyContextRadius)
	latest := anchor.Add(nearbyContextRadius)
	clipped := false
	if earliest.Before(minimum) {
		earliest = minimum
		clipped = true
	}
	if latest.After(maximum) {
		latest = maximum
		clipped = true
	}
	fields := make([]NearbyContextField, 0, len(row.Values))
	for position, value := range row.Values {
		if !utf8.ValidString(schema.Columns[position].Name) || !nearbyComparableScalar(value) {
			continue
		}
		name := schema.Columns[position].Name
		fields = append(fields, NearbyContextField{
			Name: name, Value: cloneValue(value), Suggested: slices.Contains(
				[]string{"trace_id", "span_id", "request_id"}, name,
			),
		})
	}
	return NearbyContext{
		AnchorTime: anchor, Earliest: earliest, Latest: latest,
		Index: index, Host: host, Source: source, Clipped: clipped, Fields: fields,
	}, nil
}

func nearbyComparableScalar(value Value) bool {
	switch value.Kind() {
	case ValueKindString:
		text, ok := value.String()
		return ok && utf8.ValidString(text)
	case ValueKindSigned, ValueKindUnsigned, ValueKindBool, ValueKindDecimal:
		return true
	case ValueKindDouble:
		number, ok := value.Double()
		return ok && !math.IsNaN(number) && !math.IsInf(number, 0)
	default:
		return false
	}
}

// NearbyContextFor reads one exact row from a live immutable result generation
// after applying the manager's current owner and expiry checks.
func (manager *Manager) NearbyContextFor(
	ctx context.Context,
	access AccessScope,
	id string,
	generation uint64,
	ordinal uint64,
) (NearbyContext, error) {
	if ctx == nil {
		return NearbyContext{}, errors.New("prepare nearby context: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return NearbyContext{}, err
	}
	if !validAccessScope(access) || generation == 0 {
		return NearbyContext{}, ErrNotFound
	}
	entry, err := manager.lockEntryForAccess(access, id, true, func() error { return ctx.Err() })
	if err != nil {
		return NearbyContext{}, err
	}
	defer entry.mu.Unlock()
	switch entry.job.State {
	case StateExpired:
		return NearbyContext{}, ErrExpired
	case StateCompleted:
		// Continue below.
	case StateFailed, StateCanceled:
		return NearbyContext{}, ErrResultsUnavailable
	default:
		return NearbyContext{}, ErrResultsNotReady
	}
	if entry.resultGeneration != generation || entry.resultSchema == nil ||
		entry.job.NearbyEventProvenance == nil || ordinal > uint64(math.MaxInt) ||
		ordinal >= uint64(len(entry.rows)) {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	row := entry.rows[int(ordinal)]
	if row.Ordinal != ordinal {
		return NearbyContext{}, ErrNearbyContextUnavailable
	}
	return PrepareNearbyContext(entry.job.NearbyEventProvenance, *entry.resultSchema, row)
}
