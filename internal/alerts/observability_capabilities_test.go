package alerts

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

type observedCapabilityRepository struct {
	*memoryRepository
	owners     func(context.Context, string, int) ([]string, error)
	source     func(context.Context, AccessScope) (AccessScope, error)
	page       func(context.Context, string, ListPageRequest) (ListPage, error)
	readSecret func(context.Context)
}

func (repository *observedCapabilityRepository) ListRuntimeOwners(ctx context.Context, after string, limit int) ([]string, error) {
	return repository.owners(ctx, after, limit)
}
func (repository *observedCapabilityRepository) ResolveSourceScope(ctx context.Context, scope AccessScope) (AccessScope, error) {
	return repository.source(ctx, scope)
}
func (repository *observedCapabilityRepository) ListPage(ctx context.Context, owner string, request ListPageRequest) (ListPage, error) {
	return repository.page(ctx, owner, request)
}
func (repository *observedCapabilityRepository) GetSecretBearing(ctx context.Context, owner, id string) (Alert, error) {
	repository.readSecret(ctx)
	return repository.memoryRepository.GetSecretBearing(ctx, owner, id)
}

func TestObservedRepositoryPreservesStartupValidation(t *testing.T) {
	t.Parallel()
	queryErr := errors.New("owner enumeration failed")
	for _, test := range []struct {
		name     string
		owners   []string
		baseURL  string
		queryErr error
		wantErr  bool
	}{
		{name: "empty catalog"},
		{name: "enabled owner requires public URL", owners: []string{"another-owner"}, wantErr: true},
		{name: "enabled owner has public URL", owners: []string{"another-owner"}, baseURL: "https://open-splunk.example.test"},
		{name: "query failure", queryErr: queryErr, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			metrics := featureops.NewMetrics()
			calls := 0
			base := &observedCapabilityRepository{memoryRepository: &memoryRepository{}}
			base.owners = func(gotCtx context.Context, after string, limit int) ([]string, error) {
				calls++
				if gotCtx != ctx || after != "" || limit != 1 {
					t.Fatalf("startup enumeration arguments = %v, %q, %d", gotCtx, after, limit)
				}
				return test.owners, test.queryErr
			}
			service := newTestService(t, ObserveRepository(base, ObservabilityOptions{Observer: metrics}), test.baseURL)
			err := service.ValidateEnabledRuntimeAll(ctx)
			if (err != nil) != test.wantErr || calls != 1 {
				t.Fatalf("startup error = %v, enumeration calls = %d", err, calls)
			}
			if test.queryErr != nil && !errors.Is(err, test.queryErr) {
				t.Fatalf("startup lost underlying error: %v", err)
			}
			if metrics.Snapshot() != (featureops.Snapshot{}) {
				t.Fatal("startup reads emitted mutation telemetry")
			}
		})
	}
}

func TestObservedRepositoryRechecksSourceBeforeDeliverySecrets(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	metrics := featureops.NewMetrics()
	base := &observedCapabilityRepository{memoryRepository: &memoryRepository{}}
	wantScope := AccessScope{OwnerID: "owner-1", OrganizationID: "1"}
	freshScope := wantScope
	freshScope.SiteAdministrator = true
	var sourceErr error
	sourceCalls, secretReads, generationCalls := 0, 0, 0
	base.source = func(gotCtx context.Context, scope AccessScope) (AccessScope, error) {
		sourceCalls++
		if gotCtx != ctx || scope != wantScope {
			t.Fatalf("source arguments = %v, %#v", gotCtx, scope)
		}
		return freshScope, sourceErr
	}
	base.readSecret = func(gotCtx context.Context) {
		secretReads++
		if scope, ok := AccessScopeFromContext(gotCtx); !ok || scope != freshScope {
			t.Fatalf("secret lookup lost fresh source scope: %#v, %t", scope, ok)
		}
	}
	service := newTestService(t, ObserveRepository(base, ObservabilityOptions{Observer: metrics}), "")
	created, err := service.Create(ctx, CreateInput{OwnerID: wantScope.OwnerID, OrganizationID: wantScope.OrganizationID, Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := RunSnapshot{
		AlertID: created.Alert.ID, AlertRunID: "run-1", OwnerID: wantScope.OwnerID, OrganizationID: wantScope.OrganizationID,
		Endpoint: created.Alert.Endpoint, EndpointGeneration: created.Alert.EndpointGeneration, SecretGeneration: created.Alert.SecretGeneration,
	}
	before := metrics.Snapshot()
	generate := func() (string, error) { generationCalls++; return "delivery-1", nil }
	opened, err := service.AuthorizeAndOpenDelivery(ctx, snapshot, generate)
	if err != nil || opened.Endpoint != "https://hooks.example.com/notify" || len(opened.Secret) != SecretBytes {
		t.Fatalf("authorized delivery = %#v, %v", opened, err)
	}
	sourceErr = errors.New("run-as membership revoked")
	snapshot.AlertRunID = "run-2"
	denied, err := service.AuthorizeAndOpenDelivery(ctx, snapshot, generate)
	if !errors.Is(err, sourceErr) || !reflect.DeepEqual(denied, OpenedDeliverySecrets{}) {
		t.Fatalf("revoked delivery = %#v, %v", denied, err)
	}
	if sourceCalls != 2 || secretReads != 1 || generationCalls != 1 {
		t.Fatalf("source/secret/generator calls = %d/%d/%d", sourceCalls, secretReads, generationCalls)
	}
	if metrics.Snapshot() != before {
		t.Fatal("delivery authorization emitted mutation telemetry")
	}
}

func TestObservedRepositoryPreservesNativeSitePagination(t *testing.T) {
	t.Parallel()
	metrics := featureops.NewMetrics()
	scope := AccessScope{OwnerID: "site-admin", OrganizationID: "2", SiteAdministrator: true}
	total := uint64(MaximumAlertsPerOwner + 20)
	want := ListPage{Alerts: []AlertSummary{{ID: "other-owner-alert", OwnerID: "other-owner", OrganizationID: "2"}}, NextAlertID: "next-alert", TotalSize: &total}
	pageErr := errors.New("catalog unavailable")
	var underlyingErr error
	base := &observedCapabilityRepository{memoryRepository: &memoryRepository{}}
	base.page = func(ctx context.Context, owner string, request ListPageRequest) (ListPage, error) {
		gotScope, ok := AccessScopeFromContext(ctx)
		if !ok || gotScope != scope || owner != scope.OwnerID || request != (ListPageRequest{Limit: 2, AfterAlertID: "previous-alert", Application: "search", Text: "errors", IncludeTotal: true}) {
			t.Fatalf("page forwarding = %#v, %q, %#v", gotScope, owner, request)
		}
		return want, underlyingErr
	}
	service := newTestService(t, ObserveRepository(base, ObservabilityOptions{Observer: metrics}), "")
	request := ListPageRequest{Limit: 2, AfterAlertID: "previous-alert", Application: " search ", Text: " ERRORS ", IncludeTotal: true}
	got, err := service.ListPage(context.Background(), scope, request)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("site page = %#v, %v", got, err)
	}
	underlyingErr = pageErr
	if _, err := service.ListPage(context.Background(), scope, request); !errors.Is(err, pageErr) {
		t.Fatalf("site page lost underlying error: %v", err)
	}
	if metrics.Snapshot() != (featureops.Snapshot{}) {
		t.Fatal("catalog reads emitted mutation telemetry")
	}
}

func TestObservedRepositoryMissingCapabilitiesFailClosed(t *testing.T) {
	t.Parallel()
	service := newTestService(t, ObserveRepository(&memoryRepository{}, ObservabilityOptions{Observer: featureops.NewMetrics()}), "")
	ctx := context.Background()
	if err := service.ValidateEnabledRuntimeAll(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing owner enumeration error = %v", err)
	}
	if _, err := service.resolveSourceScope(ctx, AccessScope{OwnerID: "owner-1", OrganizationID: "1"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing source resolver error = %v", err)
	}
	generated := false
	if _, err := service.AuthorizeAndOpenDelivery(ctx, RunSnapshot{OwnerID: "owner-1", OrganizationID: "1", AlertRunID: "run-1"}, func() (string, error) {
		generated = true
		return "delivery-1", nil
	}); !errors.Is(err, ErrInvalidArgument) || generated {
		t.Fatalf("missing source resolver allowed delivery: error=%v, generated=%t", err, generated)
	}
	if _, err := service.ListPage(ctx, AccessScope{OwnerID: "admin", SiteAdministrator: true}, ListPageRequest{Limit: 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing native pagination error = %v", err)
	}
}
