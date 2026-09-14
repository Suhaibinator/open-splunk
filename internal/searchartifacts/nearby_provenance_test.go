package searchartifacts

import (
	"bytes"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestJobEncodingPreservesVersionedNearbyEventProvenance(t *testing.T) {
	job := searchjobs.Job{
		ID: "durable-nearby",
		NearbyEventProvenance: &searchjobs.NearbyEventProvenance{
			Version:   searchjobs.NearbyEventProvenanceVersion,
			TimeIndex: 3, IndexIndex: 7, HostIndex: 9, SourceIndex: 11,
		},
	}
	encoded, err := encodeJob(job)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeJob(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.NearbyEventProvenance == nil ||
		*decoded.NearbyEventProvenance != *job.NearbyEventProvenance {
		t.Fatalf("decoded provenance = %+v", decoded.NearbyEventProvenance)
	}
	legacy, err := decodeJob([]byte(`{"job":{"ID":"legacy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if legacy.NearbyEventProvenance != nil {
		t.Fatalf("legacy provenance = %+v", legacy.NearbyEventProvenance)
	}
	if !bytes.Contains(encoded, []byte("NearbyEventProvenance")) {
		t.Fatalf("encoded job omitted versioned provenance: %s", encoded)
	}
}
