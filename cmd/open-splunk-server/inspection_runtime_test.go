package main

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestNewClickHouseOptionsAreSharedExplainerSafe(t *testing.T) {
	t.Setenv("OPEN_SPLUNK_CLICKHOUSE_PASSWORD", "test-password")

	result, err := newClickHouseOptions(options{
		clickhouseAddress:  "127.0.0.1:9000",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "runtime-user",
		clickhouseSecure:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Protocol != clickhousedriver.Native ||
		result.ConnOpenStrategy != clickhousedriver.ConnOpenInOrder {
		t.Fatalf(
			"ClickHouse protocol/strategy = (%v, %v)",
			result.Protocol,
			result.ConnOpenStrategy,
		)
	}
	if len(result.Addr) != 1 ||
		result.Addr[0] != "127.0.0.1:9000" ||
		result.Auth.Database != "default" ||
		result.Auth.Username != "runtime-user" ||
		result.Auth.Password != "test-password" {
		t.Fatalf("ClickHouse connection identity = %#v", result)
	}
	if result.TLS == nil ||
		result.TLS.MinVersion != tls.VersionTLS12 ||
		result.TLS.InsecureSkipVerify {
		t.Fatalf("ClickHouse TLS config = %#v", result.TLS)
	}
	if result.DialContext != nil ||
		result.DialStrategy != nil ||
		result.Settings != nil ||
		result.GetJWT != nil {
		t.Fatal("ClickHouse options contain an unsafe Explainer extension")
	}
}

func TestNewClickHouseOptionsRejectsPlaintextRemoteAddress(t *testing.T) {
	t.Parallel()

	result, err := newClickHouseOptions(options{
		clickhouseAddress:  "192.0.2.10:9000",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "runtime-user",
	})
	if err == nil || result != nil ||
		!strings.Contains(err.Error(), "loopback") {
		t.Fatalf(
			"newClickHouseOptions(remote plaintext) = (%#v, %v)",
			result,
			err,
		)
	}
}

func TestRuntimeSearchInspectionClosesServiceBeforeExplainer(t *testing.T) {
	t.Parallel()

	explainer := &runtimeInspectionExplainerFixture{}
	inspection, err := newRuntimeSearchInspection(
		runtimeSearchInspectionConfig{
			Searches: &runtimeInspectionSearchesFixture{},
			Compiler: runtimeInspectionCompilerFixture{},
			ClickHouseOptions: &clickhousedriver.Options{
				Addr: []string{"127.0.0.1:9000"},
			},
			NewExplainer: func(
				*clickhousedriver.Options,
				queryexec.Config,
			) (runtimeInspectionExplainer, error) {
				return explainer, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	explainer.onClose = func() {
		_, inspectErr := inspection.service.Inspect(
			context.Background(),
			searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
			searchinspection.Request{SearchJobID: "job"},
		)
		if !errors.Is(inspectErr, searchjobs.ErrClosed) {
			t.Errorf(
				"inspection during Explainer close error = %v, want ErrClosed",
				inspectErr,
			)
		}
	}

	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if explainer.closeCount() != 1 {
		t.Fatalf(
			"Explainer close count = %d, want 1",
			explainer.closeCount(),
		)
	}
}

func TestRuntimeSearchInspectionCloseCancelsActiveServiceBeforeExplainer(
	t *testing.T,
) {
	t.Parallel()

	searches := &runtimeBlockingInspectionSearches{
		started: make(chan struct{}),
	}
	explainer := &runtimeInspectionExplainerFixture{}
	inspection, err := newRuntimeSearchInspection(
		runtimeSearchInspectionConfig{
			Searches: searches,
			Compiler: runtimeInspectionCompilerFixture{},
			ClickHouseOptions: &clickhousedriver.Options{
				Addr: []string{"127.0.0.1:9000"},
			},
			NewExplainer: func(
				*clickhousedriver.Options,
				queryexec.Config,
			) (runtimeInspectionExplainer, error) {
				return explainer, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	explainer.onClose = func() {
		if searches.isActive() {
			t.Error("Explainer closed while snapshot dependency remained active")
		}
	}

	inspectDone := make(chan error, 1)
	go func() {
		_, inspectErr := inspection.service.Inspect(
			context.Background(),
			searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
			searchinspection.Request{SearchJobID: "job"},
		)
		inspectDone <- inspectErr
	}()
	<-searches.started

	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if inspectErr := <-inspectDone; !errors.Is(
		inspectErr,
		searchjobs.ErrClosed,
	) {
		t.Fatalf("active Inspect() error = %v, want ErrClosed", inspectErr)
	}
	if explainer.closeCount() != 1 {
		t.Fatalf(
			"Explainer close count = %d, want 1",
			explainer.closeCount(),
		)
	}
}

func TestRuntimeSearchInspectionValidatesDependenciesBeforeOpeningExplainer(
	t *testing.T,
) {
	t.Parallel()

	factoryCalls := 0
	inspection, err := newRuntimeSearchInspection(
		runtimeSearchInspectionConfig{
			ClickHouseOptions: &clickhousedriver.Options{
				Addr: []string{"127.0.0.1:9000"},
			},
			NewExplainer: func(
				*clickhousedriver.Options,
				queryexec.Config,
			) (runtimeInspectionExplainer, error) {
				factoryCalls++
				return &runtimeInspectionExplainerFixture{}, nil
			},
		},
	)
	if err == nil || inspection != nil ||
		!strings.Contains(err.Error(), "completed search snapshots") {
		t.Fatalf(
			"newRuntimeSearchInspection() = (%#v, %v), want dependency error",
			inspection,
			err,
		)
	}
	if factoryCalls != 0 {
		t.Fatalf(
			"Explainer factory calls = %d, want 0",
			factoryCalls,
		)
	}
}

func TestRuntimeSearchInspectionRejectsNilExplainerFactoryResult(t *testing.T) {
	t.Parallel()

	var typedNil *runtimeInspectionExplainerFixture
	for name, result := range map[string]runtimeInspectionExplainer{
		"nil":       nil,
		"typed nil": typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			inspection, err := newRuntimeSearchInspection(
				runtimeSearchInspectionConfig{
					Searches: &runtimeInspectionSearchesFixture{},
					Compiler: runtimeInspectionCompilerFixture{},
					ClickHouseOptions: &clickhousedriver.Options{
						Addr: []string{"127.0.0.1:9000"},
					},
					NewExplainer: func(
						*clickhousedriver.Options,
						queryexec.Config,
					) (runtimeInspectionExplainer, error) {
						return result, nil
					},
				},
			)
			if err == nil || inspection != nil ||
				!strings.Contains(err.Error(), "Explainer is required") {
				t.Fatalf(
					"newRuntimeSearchInspection() = (%#v, %v), want nil error",
					inspection,
					err,
				)
			}
		})
	}
}

func TestRuntimeSearchInspectionZeroValueCloseFailsWithoutPanicking(
	t *testing.T,
) {
	t.Parallel()

	if err := (&runtimeSearchInspection{}).Close(); err == nil ||
		!strings.Contains(err.Error(), "Explainer is required") {
		t.Fatalf("zero-value Close() error = %v", err)
	}
}

type runtimeInspectionSearchesFixture struct{}

func (*runtimeInspectionSearchesFixture) CompletedExecutionSnapshotFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ExecutionSnapshot, error) {
	return searchjobs.ExecutionSnapshot{}, errors.New("unexpected snapshot")
}

type runtimeBlockingInspectionSearches struct {
	mu      sync.Mutex
	active  bool
	started chan struct{}
	once    sync.Once
}

func (searches *runtimeBlockingInspectionSearches) CompletedExecutionSnapshotFor(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
) (searchjobs.ExecutionSnapshot, error) {
	searches.mu.Lock()
	searches.active = true
	searches.mu.Unlock()
	searches.once.Do(func() { close(searches.started) })
	defer func() {
		searches.mu.Lock()
		searches.active = false
		searches.mu.Unlock()
	}()
	<-ctx.Done()
	return searchjobs.ExecutionSnapshot{}, ctx.Err()
}

func (searches *runtimeBlockingInspectionSearches) isActive() bool {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.active
}

type runtimeInspectionCompilerFixture struct{}

func (runtimeInspectionCompilerFixture) Compile(
	*plan.Query,
) (clickhouse.CompiledQuery, error) {
	return clickhouse.CompiledQuery{}, errors.New("unexpected compile")
}

type runtimeInspectionExplainerFixture struct {
	mu       sync.Mutex
	closes   int
	onClose  func()
	closeErr error
}

func (*runtimeInspectionExplainerFixture) Explain(
	context.Context,
	clickhouse.CompiledQuery,
) (queryexec.ExplainResult, error) {
	return queryexec.ExplainResult{}, errors.New("unexpected Explain")
}

func (explainer *runtimeInspectionExplainerFixture) Close() error {
	explainer.mu.Lock()
	explainer.closes++
	onClose := explainer.onClose
	err := explainer.closeErr
	explainer.mu.Unlock()
	if onClose != nil {
		onClose()
	}
	return err
}

func (explainer *runtimeInspectionExplainerFixture) closeCount() int {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	return explainer.closes
}
