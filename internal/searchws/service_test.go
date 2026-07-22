package searchws

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type stubSearchSnapshots struct{}

func (stubSearchSnapshots) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

type stubExportSnapshots struct{}

func (stubExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

func TestNewNormalizesHardBoundsAndReportsBootstrapLimits(t *testing.T) {
	service, err := New(Config{
		Searches: stubSearchSnapshots{},
		Exports:  stubExportSnapshots{},
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})

	if service.MaximumSubscriptions() == 0 {
		t.Fatal("MaximumSubscriptions() = 0")
	}
	if service.MaximumFrameBytes() < minimumFrameBytes {
		t.Fatalf("MaximumFrameBytes() = %d, want at least %d", service.MaximumFrameBytes(), minimumFrameBytes)
	}
	limits := service.Limits()
	if limits.MaximumSubscriptions != service.MaximumSubscriptions() || limits.MaximumFrameBytes != service.MaximumFrameBytes() {
		t.Fatalf("Limits() = %+v, methods = (%d, %d)", limits, service.MaximumSubscriptions(), service.MaximumFrameBytes())
	}
}

func TestNewRejectsMissingDependenciesUnsafeScopeAndUnusableLimits(t *testing.T) {
	valid := Config{
		Searches: stubSearchSnapshots{},
		Exports:  stubExportSnapshots{},
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "search reader", mutate: func(config *Config) { config.Searches = nil }},
		{name: "export reader", mutate: func(config *Config) { config.Exports = nil }},
		{name: "tenant", mutate: func(config *Config) { config.Access.TenantID = "" }},
		{name: "owner", mutate: func(config *Config) { config.Access.OwnerID = "" }},
		{name: "frame too small", mutate: func(config *Config) { config.MaximumFrameBytes = minimumFrameBytes - 1 }},
		{name: "queue smaller than frame", mutate: func(config *Config) {
			config.MaximumFrameBytes = 4096
			config.MaximumQueuedBytes = 2048
		}},
		{name: "replay count", mutate: func(config *Config) { config.MaximumReplayEvents = maximumReplayEventsCeiling + 1 }},
		{name: "global replay", mutate: func(config *Config) { config.MaximumTotalReplayBytes = maximumTotalReplayBytesCeiling + 1 }},
		{name: "ping not below pong", mutate: func(config *Config) {
			config.PingInterval = time.Second
			config.PongTimeout = time.Second
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			service, err := New(config)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = service.Close(ctx)
				t.Fatal("New() succeeded")
			}
		})
	}
}

func TestServeHTTPRejectsBeforeUpgradeAfterClose(t *testing.T) {
	service, err := New(Config{
		Searches: stubSearchSnapshots{},
		Exports:  stubExportSnapshots{},
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequest(http.MethodGet, "http://example.test/api/v1/search/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := newRecordingResponse()
	service.ServeHTTP(response, request)
	if response.status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.status, http.StatusServiceUnavailable)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("second Close() = %v", err)
	}
}

type recordingResponse struct {
	header http.Header
	status int
}

func newRecordingResponse() *recordingResponse {
	return &recordingResponse{header: make(http.Header)}
}

func (response *recordingResponse) Header() http.Header { return response.header }

func (response *recordingResponse) WriteHeader(status int) { response.status = status }

func (response *recordingResponse) Write(payload []byte) (int, error) {
	if response.status == 0 {
		response.status = http.StatusOK
	}
	return len(payload), nil
}
