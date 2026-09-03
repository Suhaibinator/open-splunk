package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/alertstore"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
)

type runtimeAlertLifecycleConfig struct {
	ctx                    context.Context
	controlDB              *control.DB
	jobs                   *searchjobs.Manager
	artifacts              *searchartifacts.Store
	auditAppender          control.AppMutationAuditAppender
	featureOperations      *runtimeFeatureOperations
	masterKeyPath          string
	tenantID               string
	ownerID                string
	publicBaseURL          string
	privateWebhookHostsCSV string
	logger                 *zap.Logger
	closeTimeout           time.Duration
}

type runtimeAlertLifecycle struct {
	appCatalog       *runtimeAppCatalog
	appCursorKey     []byte
	admission        *runtimeTrustedSearchAdmission
	service          *alerts.Service
	repository       alerts.Repository
	testDeliverer    alerts.WebhookDeliverer
	coordinator      *alerts.Coordinator
	engine           *scheduler.Engine
	logger           *zap.Logger
	closeTimeout     time.Duration
	closeCoordinator func() error
	closeOnce        sync.Once
	cursorOnce       sync.Once
}

func newRuntimeAlertLifecycle(config runtimeAlertLifecycleConfig) (_ *runtimeAlertLifecycle, err error) {
	lifecycle := &runtimeAlertLifecycle{logger: config.logger, closeTimeout: config.closeTimeout}
	defer func() {
		if err != nil {
			lifecycle.Close()
		}
	}()
	lifecycle.appCatalog, lifecycle.appCursorKey, err = newRuntimeAuditedAppCatalog(
		config.ctx, config.controlDB, config.masterKeyPath, config.auditAppender,
	)
	if err != nil {
		return nil, err
	}
	lifecycle.admission = &runtimeTrustedSearchAdmission{
		jobs: config.jobs, indexes: config.controlDB, apps: lifecycle.appCatalog,
	}
	repository, err := alertstore.NewSQLRepository(config.controlDB, alertstore.SQLRepositoryOptions{TenantID: config.tenantID})
	if err != nil {
		return nil, fmt.Errorf("open alert repository: %w", err)
	}
	observability := alerts.ObservabilityOptions{Observer: config.featureOperations}
	lifecycle.repository = alerts.ObserveRepository(repository, observability)
	runRepository := alerts.ObserveRunRepository(repository, observability)
	keyMaterial, err := loadVerifiedMasterKey(config.ctx, config.controlDB, config.masterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("open alert encryption key: %w", err)
	}
	key, err := deriveServerKey(keyMaterial, "webhook-alert-secrets")
	clear(keyMaterial)
	if err != nil {
		return nil, fmt.Errorf("derive alert encryption key: %w", err)
	}
	cipher, err := alerts.NewAESGCMCipher(key, nil)
	clear(key)
	if err != nil {
		return nil, fmt.Errorf("create alert encryption: %w", err)
	}
	lifecycle.service, err = alerts.NewService(lifecycle.repository, cipher, alerts.ServiceOptions{PublicBaseURL: config.publicBaseURL})
	if err != nil {
		return nil, fmt.Errorf("create alert service: %w", err)
	}
	if err := lifecycle.service.ValidateEnabledRuntime(config.ctx, config.ownerID); err != nil {
		return nil, fmt.Errorf("validate enabled alerts: %w", err)
	}
	deliverer, err := alerts.NewDeliverer(alerts.DeliveryOptions{DestinationPolicy: alerts.DestinationPolicy{
		PrivateHostAllowlist: splitConfiguredValues(config.privateWebhookHostsCSV),
	}})
	if err != nil {
		return nil, fmt.Errorf("create alert deliverer: %w", err)
	}
	lifecycle.testDeliverer = alerts.ObserveTestWebhookDeliverer(deliverer, observability)
	alertArtifacts, err := newRuntimeAlertArtifacts(config.artifacts, config.tenantID)
	if err != nil {
		return nil, fmt.Errorf("create alert artifact runtime: %w", err)
	}
	lifecycle.coordinator, err = alerts.NewCoordinator(alerts.CoordinatorOptions{
		RunRepository: runRepository, Admission: lifecycle.admission,
		Jobs: alertArtifacts, Results: alertArtifacts, Retention: alertArtifacts,
		Authorizer: lifecycle.service, Deliverer: deliverer, PublicBaseURL: config.publicBaseURL,
	})
	if err != nil {
		return nil, fmt.Errorf("create alert coordinator: %w", err)
	}
	lifecycle.closeCoordinator = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), lifecycle.closeTimeout)
		defer cancel()
		return lifecycle.coordinator.Close(ctx)
	}
	if _, err := lifecycle.coordinator.Recover(config.ctx); err != nil {
		return nil, fmt.Errorf("recover alert runs: %w", err)
	}
	lifecycle.engine, err = scheduler.NewEngine(scheduler.EngineOptions{Stepper: lifecycle.coordinator})
	if err != nil {
		return nil, fmt.Errorf("create alert scheduler: %w", err)
	}
	return lifecycle, nil
}

func (lifecycle *runtimeAlertLifecycle) EraseCursorKey() {
	if lifecycle == nil {
		return
	}
	lifecycle.cursorOnce.Do(func() { clear(lifecycle.appCursorKey) })
}

func (lifecycle *runtimeAlertLifecycle) Close() {
	if lifecycle == nil {
		return
	}
	lifecycle.closeOnce.Do(func() {
		if lifecycle.closeCoordinator != nil {
			if err := lifecycle.closeCoordinator(); err != nil {
				logger := lifecycle.logger
				if logger == nil {
					logger = zap.NewNop()
				}
				logger.Warn("close alert coordinator", zap.Error(err))
			}
		}
		lifecycle.EraseCursorKey()
	})
}
