package searchjobs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestJobSourceUsesOriginSpecificIDBounds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    JobSource
		wantError bool
	}{
		{
			name: "history rerun exact limit",
			source: JobSource{
				Origin:   JobOriginHistoryRerun,
				ObjectID: strings.Repeat("h", MaximumJobIDBytes),
			},
		},
		{
			name: "history rerun over limit",
			source: JobSource{
				Origin:   JobOriginHistoryRerun,
				ObjectID: strings.Repeat("h", MaximumJobIDBytes+1),
			},
			wantError: true,
		},
		{
			name: "saved search exact limit",
			source: JobSource{
				Origin:   JobOriginSavedSearch,
				ObjectID: strings.Repeat("s", maximumJobSourceIDBytes),
			},
		},
		{
			name: "saved search over limit",
			source: JobSource{
				Origin:   JobOriginSavedSearch,
				ObjectID: strings.Repeat("s", maximumJobSourceIDBytes+1),
			},
			wantError: true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			canonical, canonicalErr := CanonicalJobSource(test.source)
			if test.wantError {
				if canonicalErr == nil {
					t.Fatalf("CanonicalJobSource(%+v) unexpectedly succeeded", test.source)
				}
			} else if canonicalErr != nil {
				t.Fatalf("CanonicalJobSource(%+v) error = %v", test.source, canonicalErr)
			} else if canonical != test.source {
				t.Fatalf("CanonicalJobSource() = %+v, want %+v", canonical, test.source)
			}

			manager := newTestManager(t, Config{
				Executor: executorFunc(func(
					context.Context,
					clickhouse.CompiledQuery,
					ResultSink,
				) error {
					return nil
				}),
				CleanupInterval: -1,
				NewID:           sequenceIDs("history-source-boundary"),
			})
			request := validRequest()
			request.Source = test.source

			created, err := manager.Create(context.Background(), request)
			if test.wantError {
				if !errors.Is(err, ErrRequestTooLarge) {
					t.Fatalf("Create() error = %v, want ErrRequestTooLarge", err)
				}
				if jobs := manager.List(); len(jobs) != 0 {
					t.Fatalf("rejected source retained %d jobs", len(jobs))
				}
				return
			}
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if created.Source != test.source {
				t.Fatalf("created source = %+v, want %+v", created.Source, test.source)
			}
		})
	}
}
