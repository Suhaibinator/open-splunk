package gradethiscorpus

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// StoredFixture is the canonical decoded corpus after it has traversed
// the production collector decoder and an EventStore.
type StoredFixture struct {
	Profile    Profile
	EventsByID map[string]*opensplunk.LogEvent
}

// StoreCanonical decodes and stores the exact canonical twenty-event batch.
// It is the shared semantic target for query execution and physical-plan
// inspection; poison load cohorts are inserted separately.
func StoreCanonical(
	ctx context.Context,
	store ingest.EventStore,
	targetTenant string,
) (StoredFixture, error) {
	if ctx == nil || store == nil {
		return StoredFixture{}, errors.New(
			"GradeThis store context and event store are required",
		)
	}
	if !validInspectionTenant(targetTenant) {
		return StoredFixture{}, errors.New(
			"GradeThis target tenant is invalid",
		)
	}
	profile := Fixture()
	if err := Validate(profile); err != nil {
		return StoredFixture{}, err
	}
	if len(profile.Events) > math.MaxUint32 {
		return StoredFixture{}, errors.New(
			"GradeThis corpus event count exceeds the storage contract",
		)
	}
	// #nosec G115 -- the explicit math.MaxUint32 guard above proves this safe.
	eventCount := uint32(len(profile.Events))
	decoder, err := collector.NewDecoder(collector.DecodeConfig{
		Format:     collector.InputFormatNDJSON,
		InputID:    "gradethis-corpus-input",
		IndexName:  IndexName,
		Source:     Source,
		Sourcetype: Sourcetype,
		Host:       Host,
		Service:    Service,
	})
	if err != nil {
		return StoredFixture{}, fmt.Errorf(
			"create GradeThis corpus decoder: %w",
			err,
		)
	}

	events := make([]*ingest.StoredEvent, 0, len(profile.Events))
	eventsByID := make(
		map[string]*opensplunk.LogEvent,
		len(profile.Events),
	)
	var offset uint64
	for index, expected := range profile.Events {
		if uint64(len(expected.RawLine)) > ^uint64(0)-offset {
			return StoredFixture{}, errors.New(
				"GradeThis corpus source offset overflowed",
			)
		}
		end := offset + uint64(len(expected.RawLine))
		event, decodeErr := decoder.Decode(
			expected.RawLine,
			collector.SourcePosition{
				FileIdentity:          "gradethis-corpus-file",
				SourcePath:            Source,
				FileFingerprintLength: 4096,
				StartOffset:           offset,
				EndOffset:             end,
				LineNumber:            uint64(index + 1),
			},
			profile.IndexTime,
		)
		if decodeErr != nil {
			return StoredFixture{}, fmt.Errorf(
				"decode GradeThis fixture event %q: %w",
				expected.ID,
				decodeErr,
			)
		}
		events = append(events, &ingest.StoredEvent{
			Event:       event,
			TenantID:    targetTenant,
			CollectorID: "gradethis-corpus",
			BatchID:     "gradethis-corpus-batch",
			IndexTime:   profile.IndexTime,
		})
		eventsByID[expected.ID] = event
		if end == ^uint64(0) {
			return StoredFixture{}, errors.New(
				"GradeThis corpus source offset overflowed",
			)
		}
		offset = end + 1
	}

	digest := sha256.Sum256(profile.NDJSON)
	result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           targetTenant,
		CollectorID:        "gradethis-corpus",
		BatchID:            "gradethis-corpus-batch",
		BatchSequence:      1,
		OriginalEventCount: eventCount,
		SourceBatchSHA256:  digest,
		ReceivedAt:         profile.IndexTime,
		Events:             events,
	})
	if err != nil {
		return StoredFixture{}, fmt.Errorf(
			"store GradeThis corpus: %w",
			err,
		)
	}
	if result.Accepted != eventCount ||
		result.Duplicate != 0 ||
		result.OriginalEventCount != eventCount ||
		len(result.RejectedEvents) != 0 {
		return StoredFixture{}, errors.New(
			"GradeThis corpus store result violated its exact batch contract",
		)
	}
	return StoredFixture{
		Profile:    profile,
		EventsByID: eventsByID,
	}, nil
}
