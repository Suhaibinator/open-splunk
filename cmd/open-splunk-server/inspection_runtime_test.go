package main

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestNewClickHouseConnectionOptionsUsesUnifiedPrincipal(t *testing.T) {
	t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")

	results, err := newClickHouseConnectionOptions(options{
		clickhouseAddress:  "clickhouse.internal:9440",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "shared-user",
		clickhouseSecure:   true,
	}, testClickHouseClientTLSProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	result := results.runtime
	if result.Protocol != clickhousedriver.Native ||
		result.ConnOpenStrategy != clickhousedriver.ConnOpenInOrder {
		t.Fatalf(
			"ClickHouse protocol/strategy = (%v, %v)",
			result.Protocol,
			result.ConnOpenStrategy,
		)
	}
	if len(result.Addr) != 1 ||
		result.Addr[0] != "clickhouse.internal:9440" ||
		result.Auth.Database != "open_splunk" ||
		result.Auth.Username != "shared-user" ||
		result.Auth.Password != "shared-password" {
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

	if results.deletion.Auth.Database != "open_splunk" ||
		results.deletion.Auth.Username != "shared-user" ||
		results.deletion.Auth.Password != "shared-password" ||
		results.deletion.MaxOpenConns != 1 ||
		results.deletion.MaxIdleConns != 1 {
		t.Fatalf("ClickHouse deletion options = %#v", results.deletion)
	}
	if results.migration.Auth.Database != "default" ||
		results.migration.Auth.Username != "shared-user" ||
		results.migration.Auth.Password != "shared-password" ||
		results.migration.MaxOpenConns != 1 ||
		results.migration.MaxIdleConns != 1 {
		t.Fatalf("ClickHouse migration options = %#v", results.migration)
	}
}

func TestNewClickHouseConnectionOptionsSkipsMigrations(t *testing.T) {
	t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")

	results, err := newClickHouseConnectionOptions(options{
		clickhouseAddress:        "127.0.0.1:9000",
		clickhouseDatabase:       "open_splunk",
		clickhouseUsername:       "shared-user",
		clickhouseSkipMigrations: true,
		clickhouseSecure:         true,
	}, testClickHouseClientTLSProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	if results.migration != nil {
		t.Fatalf("skipped migration options = %#v, want nil", results.migration)
	}
	if results.runtime == nil || results.deletion == nil {
		t.Fatalf(
			"runtime/deletion options = (%#v, %#v), want both configured",
			results.runtime,
			results.deletion,
		)
	}
	if results.runtime.Auth.Username != "shared-user" ||
		results.runtime.Auth.Password != "shared-password" ||
		results.deletion.Auth.Username != "shared-user" ||
		results.deletion.Auth.Password != "shared-password" ||
		results.runtime.TLS == nil || results.deletion.TLS == nil {
		t.Fatalf("skipped-migration ClickHouse options = %#v", results)
	}
}

func TestDiscardClickHouseConnectionCredentialsClearsEveryPrincipal(
	t *testing.T,
) {
	t.Parallel()

	migration := &clickhousedriver.Options{
		Auth: clickhousedriver.Auth{Password: "migration-secret"},
	}
	runtimeOptions := &clickhousedriver.Options{
		Auth: clickhousedriver.Auth{Password: "runtime-secret"},
	}
	deletion := &clickhousedriver.Options{
		Auth: clickhousedriver.Auth{Password: "deletion-secret"},
	}
	options := clickHouseConnectionOptions{
		migration: migration,
		runtime:   runtimeOptions,
		deletion:  deletion,
	}

	discardClickHouseConnectionCredentials(&options)

	if options != (clickHouseConnectionOptions{}) {
		t.Fatalf("discarded ClickHouse options = %#v, want empty", options)
	}
	for name, connectionOptions := range map[string]*clickhousedriver.Options{
		"migration": migration,
		"runtime":   runtimeOptions,
		"deletion":  deletion,
	} {
		if connectionOptions.Auth.Password != "" {
			t.Fatalf("%s password was not cleared", name)
		}
	}
}

func TestConfigureClickHouseConnectionOptionsUnsetsPasswordEnvironment(
	t *testing.T,
) {
	const secret = "clickhouse-secret-must-not-leak"
	t.Setenv(clickHousePasswordEnvironmentVariable, secret)

	result, err := configureClickHouseConnectionOptions(options{
		clickhouseAddress:  "per-clickhouse:9000",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "clickhouse",
	}, nil)
	if err != nil || result.runtime == nil || result.deletion == nil ||
		result.migration == nil {
		t.Fatalf(
			"configureClickHouseConnectionOptions() = (%#v, %v)",
			result,
			err,
		)
	}
	if _, exists := os.LookupEnv(clickHousePasswordEnvironmentVariable); exists {
		t.Fatal("ClickHouse password remained in the process environment")
	}
}

func TestNewClickHouseConnectionOptionsRejectsUnsafeConfiguration(t *testing.T) {
	t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")

	valid := options{
		clickhouseAddress:  "per-clickhouse:9000",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "clickhouse",
	}
	for name, mutate := range map[string]func(*options){
		"missing address": func(config *options) {
			config.clickhouseAddress = ""
		},
		"wrong database": func(config *options) {
			config.clickhouseDatabase = "logs"
		},
		"missing username": func(config *options) {
			config.clickhouseUsername = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			result, err := newClickHouseConnectionOptions(config, nil)
			if err == nil || result.runtime != nil ||
				result.deletion != nil || result.migration != nil {
				t.Fatalf(
					"newClickHouseConnectionOptions(%s) = (%#v, %v)",
					name,
					result,
					err,
				)
			}
		})
	}

	t.Run("missing password", func(t *testing.T) {
		t.Setenv(clickHousePasswordEnvironmentVariable, "")
		result, err := newClickHouseConnectionOptions(valid, nil)
		if err == nil || result.deletion != nil {
			t.Fatalf(
				"newClickHouseConnectionOptions(missing password) = (%#v, %v)",
				result,
				err,
			)
		}
	})

	t.Run("remote plaintext", func(t *testing.T) {
		t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")
		result, err := newClickHouseConnectionOptions(valid, nil)
		if err != nil || result.runtime == nil || result.runtime.TLS != nil {
			t.Fatalf("remote plaintext options = (%#v, %v)", result, err)
		}
	})
}

func TestDiscardClickHouseMigrationCredentials(t *testing.T) {
	t.Parallel()

	migration := &clickhousedriver.Options{
		Auth: clickhousedriver.Auth{Password: "migration-secret"},
	}
	options := clickHouseConnectionOptions{migration: migration}
	discardClickHouseMigrationCredentials(&options)
	if options.migration != nil {
		t.Fatal("migration options were retained")
	}
	if migration.Auth.Password != "" {
		t.Fatal("migration password was retained")
	}
	discardClickHouseMigrationCredentials(&options)
	discardClickHouseMigrationCredentials(nil)
}

func TestRuntimeSearchInspectionClosesServiceBeforeExplainer(t *testing.T) {
	t.Parallel()

	explainer := &runtimeInspectionExplainerFixture{}
	inspection, err := newRuntimeSearchInspection(
		runtimeSearchInspectionConfig{
			Searches: &runtimeInspectionSearchesFixture{},
			Compiler: clickhouse.Compiler{},
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
			Compiler: clickhouse.Compiler{},
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
					Compiler: clickhouse.Compiler{},
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
