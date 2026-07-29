package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeCollectorAdministrationHTTPGRPCSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")
	const (
		tenantID           = "tenant-collector-admin-acceptance"
		ownerID            = "collector-admin"
		onlineCollectorID  = "collector-a-online"
		offlineCollectorID = "collector-z-offline"
		onlineInstanceID   = "instance-admin-online"
		onlineBootEpoch    = "boot-admin-online"
		onlineStreamID     = "stream-admin-online"
		inputID            = "input-admin-online"
	)

	administratorToken := bytes.Repeat(
		[]byte("a"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	authenticator, err := auth.NewBearerTokenAuthenticator(
		administratorToken,
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}

	firstDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	firstDatabaseOpen := true
	t.Cleanup(func() {
		if firstDatabaseOpen {
			if closeErr := firstDatabase.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
	})
	if firstDatabase.GORMDB() == nil {
		t.Fatal("migrated control database did not expose its GORM handle")
	}
	if _, err := firstDatabase.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatal(err)
	}
	_, tokenStore, err := openSecurityStores(
		ctx,
		firstDatabase,
		masterKeyPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := tokenStore.CreateCollectorToken(
		ctx,
		auth.CreateCollectorTokenRequest{
			Name:              "collector administration acceptance",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  onlineCollectorID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := collectorfleet.New(firstDatabase)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := collectoradmission.New(
		firstDatabase,
		tokenStore,
		tenantID,
	)
	if err != nil {
		t.Fatal(err)
	}

	connectedAt := issued.Token.CreatedAt.
		Add(time.Second).
		Truncate(time.Microsecond).
		UTC()
	heartbeatAt := connectedAt.Add(time.Second)
	offlineConnectedAt := connectedAt.Add(2 * time.Second)
	offlineDisconnectedAt := connectedAt.Add(3 * time.Second)
	displayUpdatedAt := connectedAt.Add(4 * time.Second)
	disabledAt := connectedAt.Add(5 * time.Second)

	var collectorClockUnixMicro atomic.Int64
	collectorClockUnixMicro.Store(connectedAt.UnixMicro())
	ingestConfig := ingest.DefaultConfig()
	ingestConfig.Clock = func() time.Time {
		return time.UnixMicro(collectorClockUnixMicro.Load()).UTC()
	}
	ingestConfig.NewStreamID = func() string { return onlineStreamID }
	ingestConfig.ServerInstanceID = onlineBootEpoch
	ingestConfig.ServerVersion = "collector-admin-acceptance"
	heartbeatRuntime, err := collectorfleet.NewHeartbeatRuntime(
		fleet,
		collectorfleet.HeartbeatRuntimeConfig{
			MaxCollectors:     collectorfleet.MaximumActiveCollectors,
			HeartbeatInterval: ingestConfig.HeartbeatInterval,
			StaleGrace:        2 * ingestConfig.HeartbeatInterval,
			FlushInterval:     5 * time.Millisecond,
			WriteTimeout:      time.Second,
			MonotonicNow:      time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var closeHeartbeatOnce sync.Once
	closeHeartbeat := func() {
		closeHeartbeatOnce.Do(func() {
			closeContext, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			defer cancel()
			if closeErr := heartbeatRuntime.Close(closeContext); closeErr != nil {
				t.Errorf("close heartbeat runtime: %v", closeErr)
			}
		})
	}
	t.Cleanup(closeHeartbeat)
	ingestConfig.SessionManager = collectorSessionManager{
		admission:  admissions,
		fleet:      fleet,
		heartbeats: heartbeatRuntime,
	}
	ingestService, err := ingest.NewService(
		ingestConfig,
		collectorAuthorizer{store: tokenStore, tenantID: tenantID},
		ingest.EventStoreFunc(func(
			context.Context,
			ingest.StoreBatch,
		) (ingest.StoreResult, error) {
			return ingest.StoreResult{}, errors.New(
				"event storage is unexpected in collector administration acceptance",
			)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(
		grpcServer,
		ingestService,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	var connection *grpc.ClientConn
	var cancelCollectorStream context.CancelFunc
	var stopGRPCOnce sync.Once
	stopGRPC := func() {
		stopGRPCOnce.Do(func() {
			if cancelCollectorStream != nil {
				cancelCollectorStream()
			}
			if connection != nil {
				if closeErr := connection.Close(); closeErr != nil {
					t.Errorf("close collector connection: %v", closeErr)
				}
			}
			grpcServer.Stop()
			if closeErr := listener.Close(); closeErr != nil {
				t.Errorf("close collector listener: %v", closeErr)
			}
			select {
			case serveErr := <-serveDone:
				if serveErr != nil &&
					!errors.Is(serveErr, grpc.ErrServerStopped) {
					t.Errorf("serve collector: %v", serveErr)
				}
			case <-time.After(time.Second):
				t.Error("collector gRPC server did not stop")
			}
		})
	}
	t.Cleanup(stopGRPC)

	connection, err = grpc.NewClient(
		"passthrough:///collector-admin-acceptance",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	streamContext, cancel := context.WithTimeout(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer "+issued.Secret.Plaintext(),
			),
		),
		10*time.Second,
	)
	cancelCollectorStream = cancel
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(connectedAt),
		Payload: &opensplunkv1.CollectRequest_Hello{
			Hello: &opensplunkv1.CollectorHello{
				CollectorId:      onlineCollectorID,
				InstanceId:       onlineInstanceID,
				ProtocolMajor:    1,
				ProtocolMinor:    0,
				CollectorVersion: "collector-admin-acceptance",
				Hostname:         "a-online.example",
				OperatingSystem:  "linux",
				Architecture:     "amd64",
				StartedAt:        timestamppb.New(connectedAt.Add(-time.Minute)),
				Capabilities: []opensplunkv1.CollectorCapability{
					opensplunkv1.
						CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
				},
				Inputs: []*opensplunkv1.CollectorInputRegistration{{
					InputId: inputID,
					InputType: opensplunkv1.
						CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
					IndexName: "main",
				}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if readyResponse.GetReady() == nil ||
		readyResponse.GetReady().GetStreamId() != onlineStreamID ||
		readyResponse.GetReady().GetServerInstanceId() != onlineBootEpoch ||
		!slices.Equal(
			readyResponse.GetReady().GetAuthorizedIndexes(),
			[]string{"main"},
		) {
		t.Fatalf("collector Ready = %#v", readyResponse)
	}

	collectorClockUnixMicro.Store(heartbeatAt.UnixMicro())
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(heartbeatAt),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{
			Heartbeat: &opensplunkv1.CollectorHeartbeat{
				CollectorId: onlineCollectorID,
				InstanceId:  onlineInstanceID,
				ObservedAt:  timestamppb.New(heartbeatAt),
				Queue: &opensplunkv1.CollectorQueueStats{
					QueuedEvents:            7,
					QueuedBytes:             4096,
					OldestEventAge:          durationpb.New(2 * time.Second),
					SentEventsTotal:         11,
					AcknowledgedEventsTotal: 9,
					RetriedBatchesTotal:     2,
					RejectedEventsTotal:     1,
				},
				Inputs: []*opensplunkv1.CollectorInputHealth{{
					InputId: inputID,
					State: opensplunkv1.
						CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY,
					StatusMessage:     "healthy",
					DiscoveredSources: 3,
					ActiveSources:     2,
					EventsReadTotal:   11,
					BytesReadTotal:    2048,
					LastEventAt:       timestamppb.New(heartbeatAt),
				}},
				ProcessResidentMemoryBytes: 64 << 20,
				ProcessCpuPercent:          7.5,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	persistedOnline := waitForCollectorAdminAcceptanceObservation(
		t,
		fleet,
		collectorfleet.Scope{TenantID: tenantID},
		onlineCollectorID,
		2,
	)
	if persistedOnline.ActiveLease == nil ||
		persistedOnline.ActiveLease.InstanceID != onlineInstanceID {
		t.Fatalf("persisted online collector = %#v", persistedOnline)
	}

	offlineClaim := collectorfleet.ClaimRequest{
		Scope:       collectorfleet.Scope{TenantID: tenantID},
		CollectorID: offlineCollectorID,
		BootEpoch:   "boot-admin-offline",
		StreamID:    "stream-admin-offline",
		ReceivedAt:  offlineConnectedAt,
		Hello: collectorfleet.Hello{
			InstanceID:        "instance-admin-offline",
			ProtocolMajor:     1,
			CollectorVersion:  "collector-admin-offline",
			Hostname:          "z-offline.example",
			OperatingSystem:   "linux",
			Architecture:      "arm64",
			StartedAt:         offlineConnectedAt.Add(-time.Minute),
			Capabilities:      []uint32{1},
			AuthorizedIndexes: []string{"main"},
			Inputs: []collectorfleet.InputRegistration{{
				InputID:   "input-admin-offline",
				InputType: 1,
				IndexName: "main",
			}},
		},
	}
	_, offlineLease, err := fleet.Claim(ctx, offlineClaim)
	if err != nil {
		t.Fatal(err)
	}
	if disconnected, err := fleet.Disconnect(
		ctx,
		offlineLease,
		offlineDisconnectedAt,
	); err != nil || !disconnected {
		t.Fatalf("disconnect offline collector = %t, %v", disconnected, err)
	}

	collectorAdministration, err := newRuntimeCollectorAdministration(
		ctx,
		firstDatabase,
		fleet,
		heartbeatRuntime,
		masterKeyPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	var administratorClockUnixMicro atomic.Int64
	administratorClockUnixMicro.Store(displayUpdatedAt.UnixMicro())
	firstHandler := newCollectorAdminAcceptanceHandler(
		t,
		firstDatabase,
		collectorAdministration,
		authenticator,
		tenantID,
		ownerID,
		func() time.Time {
			return time.UnixMicro(administratorClockUnixMicro.Load()).UTC()
		},
	)
	firstHandlerOpen := true
	t.Cleanup(func() {
		if firstHandlerOpen {
			if closeErr := firstHandler.Close(context.Background()); closeErr != nil {
				t.Error(closeErr)
			}
		}
	})

	bootstrapResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
		administratorToken,
	)
	if bootstrapResponse.Code != http.StatusOK {
		t.Fatalf(
			"bootstrap status = %d, body = %s",
			bootstrapResponse.Code,
			bootstrapResponse.Body,
		)
	}
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		bootstrapResponse,
		&bootstrap,
	)
	if !slices.Contains(
		bootstrap.GetFeatures(),
		opensplunkv1.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN,
	) {
		t.Fatalf("bootstrap features = %v", bootstrap.GetFeatures())
	}

	displayName := "Primary collector"
	updateRequest := &opensplunkv1.UpdateCollectorRequest{
		CollectorId:     onlineCollectorID,
		ExpectedVersion: persistedOnline.Version,
		DisplayName:     &displayName,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"display_name"},
		},
	}
	wrongAdministratorToken := bytes.Repeat(
		[]byte("b"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	for name, bearer := range map[string][]byte{
		"missing": nil,
		"wrong":   wrongAdministratorToken,
	} {
		response := postCollectorAdminAcceptanceProto(
			t,
			firstHandler,
			"/api/v1/collectors/update",
			updateRequest,
			bearer,
		)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf(
				"%s authentication status = %d, body = %s",
				name,
				response.Code,
				response.Body,
			)
		}
	}
	unchangedAdministration, err := fleet.GetAdministration(
		ctx,
		collectorfleet.Scope{TenantID: tenantID},
		onlineCollectorID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if unchangedAdministration.Version != persistedOnline.Version ||
		unchangedAdministration.DisplayName != nil {
		t.Fatalf(
			"unauthenticated requests changed administration: %#v",
			unchangedAdministration,
		)
	}

	getResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/get",
		&opensplunkv1.GetCollectorRequest{
			CollectorId: onlineCollectorID,
		},
		administratorToken,
	)
	if getResponse.Code != http.StatusOK {
		t.Fatalf(
			"get online status = %d, body = %s",
			getResponse.Code,
			getResponse.Body,
		)
	}
	var onlineGet opensplunkv1.GetCollectorResponse
	unmarshalCollectorAdminAcceptanceResponse(t, getResponse, &onlineGet)
	assertCollectorAdminAcceptanceOnlineRecord(
		t,
		onlineGet.GetCollector(),
		onlineCollectorID,
		onlineInstanceID,
		connectedAt,
		heartbeatAt,
	)

	pageSize := uint32(1)
	onlineListRequest := collectorAdminAcceptanceListRequest(pageSize, "")
	onlineListResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/list",
		onlineListRequest,
		administratorToken,
	)
	if onlineListResponse.Code != http.StatusOK {
		t.Fatalf(
			"list online status = %d, body = %s",
			onlineListResponse.Code,
			onlineListResponse.Body,
		)
	}
	var onlinePage opensplunkv1.ListCollectorsResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		onlineListResponse,
		&onlinePage,
	)
	if len(onlinePage.GetCollectors()) != 1 {
		t.Fatalf("online collector page = %#v", &onlinePage)
	}
	assertCollectorAdminAcceptanceOnlineRecord(
		t,
		onlinePage.GetCollectors()[0],
		onlineCollectorID,
		onlineInstanceID,
		connectedAt,
		heartbeatAt,
	)
	if onlinePage.GetPage() == nil ||
		onlinePage.GetPage().NextPageToken == nil ||
		onlinePage.GetPage().GetNextPageToken() == "" ||
		onlinePage.GetPage().TotalSize == nil ||
		onlinePage.GetPage().GetTotalSize() != 2 ||
		!onlinePage.GetPage().GetTotalSizeExact() {
		t.Fatalf("online collector pagination = %#v", onlinePage.GetPage())
	}
	onlineCursor := strings.Clone(
		onlinePage.GetPage().GetNextPageToken(),
	)

	updateResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/update",
		updateRequest,
		administratorToken,
	)
	if updateResponse.Code != http.StatusOK {
		t.Fatalf(
			"update display status = %d, body = %s",
			updateResponse.Code,
			updateResponse.Body,
		)
	}
	var displayUpdated opensplunkv1.UpdateCollectorResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		updateResponse,
		&displayUpdated,
	)
	assertCollectorAdminAcceptanceSnapshot(
		t,
		displayUpdated.GetCollector(),
		onlineCollectorID,
		persistedOnline.Version+1,
		displayName,
		opensplunkv1.
			CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_ENABLED,
		connectedAt,
		displayUpdatedAt,
	)

	invalidatedRequest := collectorAdminAcceptanceListRequest(
		pageSize,
		onlineCursor,
	)
	invalidatedResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/list",
		invalidatedRequest,
		administratorToken,
	)
	if invalidatedResponse.Code != http.StatusBadRequest ||
		strings.Contains(invalidatedResponse.Body.String(), onlineCursor) ||
		strings.Contains(
			invalidatedResponse.Body.String(),
			offlineCollectorID,
		) {
		t.Fatalf(
			"invalidated cursor status = %d, body = %s",
			invalidatedResponse.Code,
			invalidatedResponse.Body,
		)
	}

	administratorClockUnixMicro.Store(disabledAt.UnixMicro())
	disableResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/state/set",
		&opensplunkv1.SetCollectorEnabledRequest{
			CollectorId:     onlineCollectorID,
			ExpectedVersion: persistedOnline.Version + 1,
			AdministrativeState: opensplunkv1.
				CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED,
		},
		administratorToken,
	)
	if disableResponse.Code != http.StatusOK {
		t.Fatalf(
			"disable status = %d, body = %s",
			disableResponse.Code,
			disableResponse.Body,
		)
	}
	var disabled opensplunkv1.SetCollectorEnabledResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		disableResponse,
		&disabled,
	)
	assertCollectorAdminAcceptanceSnapshot(
		t,
		disabled.GetCollector(),
		onlineCollectorID,
		persistedOnline.Version+2,
		displayName,
		opensplunkv1.
			CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED,
		connectedAt,
		disabledAt,
	)

	disabledGetResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/get",
		&opensplunkv1.GetCollectorRequest{
			CollectorId: onlineCollectorID,
		},
		administratorToken,
	)
	if disabledGetResponse.Code != http.StatusOK {
		t.Fatalf(
			"get disabled status = %d, body = %s",
			disabledGetResponse.Code,
			disabledGetResponse.Body,
		)
	}
	var disabledGet opensplunkv1.GetCollectorResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		disabledGetResponse,
		&disabledGet,
	)
	assertCollectorAdminAcceptanceDisabledRecord(
		t,
		disabledGet.GetCollector(),
		onlineCollectorID,
		displayName,
		persistedOnline.Version+2,
		connectedAt,
		disabledAt,
	)

	stopGRPC()
	waitForCollectorAdminAcceptanceLiveness(
		t,
		heartbeatRuntime,
		collectorfleet.Scope{TenantID: tenantID},
		0,
	)

	stableListResponse := postCollectorAdminAcceptanceProto(
		t,
		firstHandler,
		"/api/v1/collectors/list",
		collectorAdminAcceptanceListRequest(pageSize, ""),
		administratorToken,
	)
	if stableListResponse.Code != http.StatusOK {
		t.Fatalf(
			"stable list status = %d, body = %s",
			stableListResponse.Code,
			stableListResponse.Body,
		)
	}
	var stableFirstPage opensplunkv1.ListCollectorsResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		stableListResponse,
		&stableFirstPage,
	)
	if len(stableFirstPage.GetCollectors()) != 1 ||
		stableFirstPage.GetCollectors()[0].GetCollectorId() !=
			onlineCollectorID ||
		stableFirstPage.GetCollectors()[0].GetConnectionState() !=
			opensplunkv1.
				CollectorConnectionState_COLLECTOR_CONNECTION_STATE_DISABLED ||
		stableFirstPage.GetPage() == nil ||
		stableFirstPage.GetPage().NextPageToken == nil ||
		stableFirstPage.GetPage().GetNextPageToken() == "" ||
		stableFirstPage.GetPage().TotalSize == nil ||
		stableFirstPage.GetPage().GetTotalSize() != 2 ||
		!stableFirstPage.GetPage().GetTotalSizeExact() {
		t.Fatalf("stable first collector page = %#v", &stableFirstPage)
	}
	stableOfflineCursor := strings.Clone(
		stableFirstPage.GetPage().GetNextPageToken(),
	)

	if err := firstHandler.Close(ctx); err != nil {
		t.Fatal(err)
	}
	firstHandlerOpen = false
	closeHeartbeat()
	if err := firstDatabase.Close(); err != nil {
		t.Fatal(err)
	}
	firstDatabaseOpen = false

	reopenedDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := reopenedDatabase.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if reopenedDatabase.GORMDB() == nil {
		t.Fatal("reopened control database did not expose its GORM handle")
	}
	reopenedFleet, err := collectorfleet.New(reopenedDatabase)
	if err != nil {
		t.Fatal(err)
	}
	reopenedAdministration, err := newRuntimeCollectorAdministration(
		ctx,
		reopenedDatabase,
		reopenedFleet,
		nil,
		masterKeyPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	reopenedHandler := newCollectorAdminAcceptanceHandler(
		t,
		reopenedDatabase,
		reopenedAdministration,
		authenticator,
		tenantID,
		ownerID,
		func() time.Time { return disabledAt },
	)
	t.Cleanup(func() {
		if closeErr := reopenedHandler.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	})

	reopenedContinuationResponse := postCollectorAdminAcceptanceProto(
		t,
		reopenedHandler,
		"/api/v1/collectors/list",
		collectorAdminAcceptanceListRequest(
			pageSize,
			stableOfflineCursor,
		),
		administratorToken,
	)
	if reopenedContinuationResponse.Code != http.StatusOK {
		t.Fatalf(
			"reopened continuation status = %d, body = %s",
			reopenedContinuationResponse.Code,
			reopenedContinuationResponse.Body,
		)
	}
	var reopenedPage opensplunkv1.ListCollectorsResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		reopenedContinuationResponse,
		&reopenedPage,
	)
	if len(reopenedPage.GetCollectors()) != 1 {
		t.Fatalf("reopened collector page = %#v", &reopenedPage)
	}
	assertCollectorAdminAcceptanceOfflineRecord(
		t,
		reopenedPage.GetCollectors()[0],
		offlineCollectorID,
		offlineConnectedAt,
		offlineDisconnectedAt,
	)
	if reopenedPage.GetPage() == nil ||
		reopenedPage.GetPage().NextPageToken != nil ||
		reopenedPage.GetPage().TotalSize == nil ||
		reopenedPage.GetPage().GetTotalSize() != 2 ||
		!reopenedPage.GetPage().GetTotalSizeExact() {
		t.Fatalf("reopened pagination = %#v", reopenedPage.GetPage())
	}

	reopenedGetResponse := postCollectorAdminAcceptanceProto(
		t,
		reopenedHandler,
		"/api/v1/collectors/get",
		&opensplunkv1.GetCollectorRequest{
			CollectorId: offlineCollectorID,
		},
		administratorToken,
	)
	if reopenedGetResponse.Code != http.StatusOK {
		t.Fatalf(
			"reopened get status = %d, body = %s",
			reopenedGetResponse.Code,
			reopenedGetResponse.Body,
		)
	}
	var reopenedGet opensplunkv1.GetCollectorResponse
	unmarshalCollectorAdminAcceptanceResponse(
		t,
		reopenedGetResponse,
		&reopenedGet,
	)
	assertCollectorAdminAcceptanceOfflineRecord(
		t,
		reopenedGet.GetCollector(),
		offlineCollectorID,
		offlineConnectedAt,
		offlineDisconnectedAt,
	)
}

func newCollectorAdminAcceptanceHandler(
	t *testing.T,
	database *control.DB,
	administration *runtimeCollectorAdministration,
	authenticator auth.BrowserAuthenticator,
	tenantID string,
	ownerID string,
	now func() time.Time,
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	config.Indexes = database
	config.CollectorAdmin = administration
	config.BrowserAuthenticator = authenticator
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	config.TenantID = tenantID
	config.OwnerID = ownerID
	config.Now = now
	handler, err := server.NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postCollectorAdminAcceptanceProto(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
	bearerToken []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal %T: %v", message, err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://127.0.0.1"+path,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	if len(bearerToken) != 0 {
		request.Header.Set(
			"Authorization",
			"Bearer "+string(bearerToken),
		)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func unmarshalCollectorAdminAcceptanceResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
	message proto.Message,
) {
	t.Helper()
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("unmarshal %T: %v", message, err)
	}
}

func collectorAdminAcceptanceListRequest(
	pageSize uint32,
	pageToken string,
) *opensplunkv1.ListCollectorsRequest {
	request := &opensplunkv1.ListCollectorsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		},
		SortBy: opensplunkv1.
			CollectorSortBy_COLLECTOR_SORT_BY_HOSTNAME,
		SortDirection: opensplunkv1.
			SortDirection_SORT_DIRECTION_ASCENDING,
	}
	if pageToken != "" {
		token := strings.Clone(pageToken)
		request.Page.PageToken = &token
	}
	return request
}

func assertCollectorAdminAcceptanceOnlineRecord(
	t *testing.T,
	record *opensplunkv1.CollectorRecord,
	collectorID string,
	instanceID string,
	connectedAt time.Time,
	heartbeatAt time.Time,
) {
	t.Helper()
	if record == nil ||
		record.GetCollectorId() != collectorID ||
		record.GetVersion() != 1 ||
		record.DisplayName != nil ||
		record.GetConnectionState() !=
			opensplunkv1.
				CollectorConnectionState_COLLECTOR_CONNECTION_STATE_ONLINE ||
		record.ActiveInstanceId == nil ||
		record.GetActiveInstanceId() != instanceID ||
		record.CollectorVersion == nil ||
		record.GetCollectorVersion() != "collector-admin-acceptance" ||
		record.Hostname == nil ||
		record.GetHostname() != "a-online.example" ||
		record.OperatingSystem == nil ||
		record.GetOperatingSystem() != "linux" ||
		record.Architecture == nil ||
		record.GetArchitecture() != "amd64" ||
		!slices.Equal(
			record.GetCapabilities(),
			[]opensplunkv1.CollectorCapability{
				opensplunkv1.
					CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
			},
		) ||
		!slices.Equal(record.GetAuthorizedIndexes(), []string{"main"}) ||
		record.GetAdministrativeState() !=
			opensplunkv1.
				CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_ENABLED ||
		record.GetFirstSeenAt() == nil ||
		!record.GetFirstSeenAt().AsTime().Equal(connectedAt) ||
		record.GetConnectedAt() == nil ||
		!record.GetConnectedAt().AsTime().Equal(connectedAt) ||
		record.GetLastSeenAt() == nil ||
		!record.GetLastSeenAt().AsTime().Equal(heartbeatAt) ||
		record.GetDisconnectedAt() != nil {
		t.Fatalf("online collector record = %#v", record)
	}
	queue := record.GetQueue()
	if queue == nil ||
		queue.GetQueuedEvents() != 7 ||
		queue.GetQueuedBytes() != 4096 ||
		queue.GetOldestEventAge() == nil ||
		queue.GetOldestEventAge().AsDuration() != 2*time.Second ||
		queue.GetSentEventsTotal() != 11 ||
		queue.GetAcknowledgedEventsTotal() != 9 ||
		queue.GetRetriedBatchesTotal() != 2 ||
		queue.GetRejectedEventsTotal() != 1 {
		t.Fatalf("online collector queue = %#v", queue)
	}
	if len(record.GetInputs()) != 1 ||
		record.GetInputs()[0].GetInputId() != "input-admin-online" ||
		record.GetInputs()[0].GetState() !=
			opensplunkv1.
				CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY ||
		record.GetInputs()[0].GetStatusMessage() != "healthy" ||
		record.GetInputs()[0].GetDiscoveredSources() != 3 ||
		record.GetInputs()[0].GetActiveSources() != 2 ||
		record.GetInputs()[0].GetEventsReadTotal() != 11 ||
		record.GetInputs()[0].GetBytesReadTotal() != 2048 ||
		record.GetInputs()[0].GetLastEventAt() == nil ||
		!record.GetInputs()[0].GetLastEventAt().AsTime().Equal(heartbeatAt) {
		t.Fatalf("online collector inputs = %#v", record.GetInputs())
	}
}

func assertCollectorAdminAcceptanceSnapshot(
	t *testing.T,
	snapshot *opensplunkv1.CollectorAdministrationSnapshot,
	collectorID string,
	version uint64,
	displayName string,
	state opensplunkv1.CollectorAdministrativeState,
	firstSeenAt time.Time,
	updatedAt time.Time,
) {
	t.Helper()
	if snapshot == nil ||
		snapshot.GetCollectorId() != collectorID ||
		snapshot.GetVersion() != version ||
		snapshot.DisplayName == nil ||
		snapshot.GetDisplayName() != displayName ||
		snapshot.GetAdministrativeState() != state ||
		snapshot.GetFirstSeenAt() == nil ||
		!snapshot.GetFirstSeenAt().AsTime().Equal(firstSeenAt) ||
		snapshot.GetUpdatedAt() == nil ||
		!snapshot.GetUpdatedAt().AsTime().Equal(updatedAt) {
		t.Fatalf("collector administration snapshot = %#v", snapshot)
	}
}

func assertCollectorAdminAcceptanceDisabledRecord(
	t *testing.T,
	record *opensplunkv1.CollectorRecord,
	collectorID string,
	displayName string,
	version uint64,
	firstSeenAt time.Time,
	disabledAt time.Time,
) {
	t.Helper()
	if record == nil ||
		record.GetCollectorId() != collectorID ||
		record.GetVersion() != version ||
		record.DisplayName == nil ||
		record.GetDisplayName() != displayName ||
		record.GetConnectionState() !=
			opensplunkv1.
				CollectorConnectionState_COLLECTOR_CONNECTION_STATE_DISABLED ||
		record.ActiveInstanceId != nil ||
		record.GetAdministrativeState() !=
			opensplunkv1.
				CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED ||
		record.GetFirstSeenAt() == nil ||
		!record.GetFirstSeenAt().AsTime().Equal(firstSeenAt) ||
		record.GetLastSeenAt() == nil ||
		!record.GetLastSeenAt().AsTime().Equal(disabledAt) ||
		record.GetDisconnectedAt() == nil ||
		!record.GetDisconnectedAt().AsTime().Equal(disabledAt) ||
		record.GetQueue().GetQueuedEvents() != 7 ||
		len(record.GetInputs()) != 1 {
		t.Fatalf("disabled collector record = %#v", record)
	}
}

func assertCollectorAdminAcceptanceOfflineRecord(
	t *testing.T,
	record *opensplunkv1.CollectorRecord,
	collectorID string,
	connectedAt time.Time,
	disconnectedAt time.Time,
) {
	t.Helper()
	if record == nil ||
		record.GetCollectorId() != collectorID ||
		record.GetVersion() != 1 ||
		record.DisplayName != nil ||
		record.GetConnectionState() !=
			opensplunkv1.
				CollectorConnectionState_COLLECTOR_CONNECTION_STATE_OFFLINE ||
		record.ActiveInstanceId != nil ||
		record.Hostname == nil ||
		record.GetHostname() != "z-offline.example" ||
		record.GetAdministrativeState() !=
			opensplunkv1.
				CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_ENABLED ||
		record.GetFirstSeenAt() == nil ||
		!record.GetFirstSeenAt().AsTime().Equal(connectedAt) ||
		record.GetConnectedAt() == nil ||
		!record.GetConnectedAt().AsTime().Equal(connectedAt) ||
		record.GetLastSeenAt() == nil ||
		!record.GetLastSeenAt().AsTime().Equal(disconnectedAt) ||
		record.GetDisconnectedAt() == nil ||
		!record.GetDisconnectedAt().AsTime().Equal(disconnectedAt) {
		t.Fatalf("offline collector record = %#v", record)
	}
}

func waitForCollectorAdminAcceptanceObservation(
	t *testing.T,
	fleet *collectorfleet.Store,
	scope collectorfleet.Scope,
	collectorID string,
	observationSequence uint64,
) collectorfleet.Collector {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		collector, err := fleet.Get(ctx, scope, collectorID)
		if err != nil {
			t.Fatal(err)
		}
		if collector.ObservationSequence == observationSequence {
			return collector
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"collector observation sequence = %d, want %d",
				collector.ObservationSequence,
				observationSequence,
			)
		case <-ticker.C:
		}
	}
}

func waitForCollectorAdminAcceptanceLiveness(
	t *testing.T,
	runtime *collectorfleet.HeartbeatRuntime,
	scope collectorfleet.Scope,
	expected int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := runtime.SnapshotLiveness(scope)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot) == expected {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"collector liveness size = %d, want %d",
				len(snapshot),
				expected,
			)
		case <-ticker.C:
		}
	}
}
