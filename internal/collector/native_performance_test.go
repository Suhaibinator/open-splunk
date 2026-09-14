package collector

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/parserconfig"
)

func TestNativeDecoderConcurrentIndependentOutputs(t *testing.T) {
	for _, fixture := range nativeBenchmarkFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			decoder, err := NewDecoder(nativeBenchmarkConfig(fixture))
			if err != nil {
				t.Fatal(err)
			}
			const workers, iterations = 8, 32
			failures := make(chan error, workers)
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					for iteration := range iterations {
						raw := []byte(fixture.raw)
						original := bytes.Clone(raw)
						position := SourcePosition{FileIdentity: fmt.Sprintf("worker-%d", worker), StartOffset: uint64(iteration * len(raw)), EndOffset: uint64((iteration + 1) * len(raw))}
						now := time.Date(2026, 9, 7, 12, 35, 0, 0, time.UTC)
						first, err := decoder.Decode(raw, position, now)
						if err != nil {
							failures <- err
							return
						}
						// All three caller-owned objects can change without changing the decoder
						// or a previously returned event, even while other workers are decoding.
						raw[0] ^= 1
						if !bytes.Equal(first.GetRaw(), original) || first.GetMessage() != fixture.message {
							failures <- fmt.Errorf("worker %d event aliases caller buffer or wrong projection", worker)
							return
						}
						id := first.GetEventId()
						first.Raw[0] ^= 1
						first.EventTime.Seconds = 0
						if len(first.Fields.Fields) > 0 {
							first.Fields.Fields[0].Name = "mutated"
						}
						second, err := decoder.Decode(original, position, now.Add(time.Second))
						if err != nil {
							failures <- err
							return
						}
						if !bytes.Equal(second.GetRaw(), original) || second.GetMessage() != fixture.message || second.GetEventId() != id || second.GetEventTime().GetSeconds() == 0 {
							failures <- fmt.Errorf("worker %d outputs share mutable state or unstable ID", worker)
							return
						}
						for _, field := range second.GetFields().GetFields() {
							if field.GetName() == "mutated" {
								failures <- fmt.Errorf("worker %d field aliases previous event", worker)
								return
							}
						}
					}
				})
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}

func TestNativeDecoderCopiesParserOptions(t *testing.T) {
	options := &parserconfig.Options{Fields: map[string]string{"message": "text"}}
	decoder, err := NewDecoder(nativeBenchmarkConfig(nativeBenchmarkFixture{format: "logfmt", options: options}))
	if err != nil {
		t.Fatal(err)
	}
	options.Fields["message"] = "other"
	options.Pattern = "%{message}"
	event, err := decoder.Decode([]byte("text=original other=replacement"), SourcePosition{FileIdentity: "ownership", EndOffset: 31}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if event.GetMessage() != "original" {
		t.Fatalf("compiled parser followed caller mutation: %q", event.GetMessage())
	}
}
