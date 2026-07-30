package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

const maximumDurableTenantIDBytes = 255

func newCollectorBootEpoch() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate collector server boot epoch: %w", err)
	}
	return hex.EncodeToString(random[:]), nil
}

func normalizeRuntimeOptions(config *options) error {
	if config == nil {
		return errors.New("server options are required")
	}
	if config.indexRetention <= 0 {
		return errors.New("default index retention must be positive")
	}
	if config.indexRetention%time.Millisecond != 0 {
		return errors.New("default index retention must use whole milliseconds")
	}
	searchHistoryRetention, err := searchhistory.ResolveRetentionPolicy(
		config.searchHistoryMaximumAge,
		config.searchHistoryMaximumEntriesPerOwner,
	)
	if err != nil {
		return fmt.Errorf("validate search-history retention: %w", err)
	}
	config.searchHistoryMaximumAge = searchHistoryRetention.MaximumAge
	config.searchHistoryMaximumEntriesPerOwner =
		searchHistoryRetention.MaximumEntriesPerOwner
	config.exportArtifactDir = strings.TrimSpace(config.exportArtifactDir)
	if config.exportArtifactDir == "" {
		controlDBPath := strings.TrimSpace(config.controlDBPath)
		if controlDBPath == "" {
			controlDBPath = "open-splunk.db"
		}
		config.exportArtifactDir = controlDBPath + ".exports"
	}
	if !utf8.ValidString(config.exportArtifactDir) || strings.IndexByte(config.exportArtifactDir, 0) >= 0 {
		return errors.New("export artifact directory must be valid UTF-8 without NUL bytes")
	}
	config.exportArtifactDir = filepath.Clean(config.exportArtifactDir)
	if config.exportArtifactDir == "." || config.exportArtifactDir == ".." ||
		strings.HasPrefix(config.exportArtifactDir, ".."+string(filepath.Separator)) {
		return errors.New("export artifact directory must be a dedicated child path")
	}
	absoluteArtifactDir, err := filepath.Abs(config.exportArtifactDir)
	if err != nil {
		return errors.New("export artifact directory is invalid")
	}
	if filepath.Dir(absoluteArtifactDir) == absoluteArtifactDir {
		return errors.New("export artifact directory cannot be a filesystem root")
	}
	config.httpAddress = strings.TrimSpace(config.httpAddress)
	host, _, err := net.SplitHostPort(config.httpAddress)
	if err != nil {
		return errors.New("HTTP listen address must include a valid host and port")
	}
	if !loopbackAddress(config.httpAddress) {
		return errors.New("administrator browser routes require a loopback HTTP listen address until HTTPS is configured")
	}
	config.httpAllowedHosts = config.httpAllowedHosts[:0]
	for _, candidate := range strings.Split(config.httpAllowedHostsCSV, ",") {
		if candidate = strings.TrimSpace(candidate); candidate != "" {
			config.httpAllowedHosts = append(config.httpAllowedHosts, candidate)
		}
	}
	if len(config.httpAllowedHosts) == 0 {
		ip := net.ParseIP(host)
		if host == "" || (ip != nil && ip.IsUnspecified()) {
			return errors.New("wildcard HTTP listeners require an explicit -http-allowed-hosts value")
		}
		config.httpAllowedHosts = append(config.httpAllowedHosts, host)
		if loopbackAddress(config.httpAddress) {
			config.httpAllowedHosts = append(config.httpAllowedHosts, "localhost", "127.0.0.1", "::1")
		}
	}
	if !utf8.ValidString(config.tenantID) ||
		strings.ContainsFunc(config.tenantID, unicode.IsControl) {
		return fmt.Errorf(
			"tenant ID must be non-empty valid UTF-8 without control characters and at most %d bytes",
			maximumDurableTenantIDBytes,
		)
	}
	config.tenantID = strings.TrimSpace(config.tenantID)
	if config.tenantID == "" || len(config.tenantID) > maximumDurableTenantIDBytes {
		return fmt.Errorf(
			"tenant ID must be non-empty valid UTF-8 without control characters and at most %d bytes",
			maximumDurableTenantIDBytes,
		)
	}
	return nil
}

type collectorAuthenticationStore interface {
	Authenticate(context.Context, string) (auth.Authentication, error)
}

type collectorAuthorizer struct {
	store    collectorAuthenticationStore
	tenantID string
}

func (authorizer collectorAuthorizer) Authorize(ctx context.Context, token string) (ingest.Authorization, error) {
	if authorizer.store == nil || strings.TrimSpace(authorizer.tenantID) == "" {
		return ingest.Authorization{}, errors.New("collector authorization is unavailable")
	}
	authentication, err := authorizer.store.Authenticate(ctx, token)
	if err != nil {
		if errors.Is(err, auth.ErrUnauthorized) {
			return ingest.Authorization{}, ingest.ErrUnauthorized
		}
		return ingest.Authorization{}, err
	}
	if authentication.TokenID == "" || authentication.BoundCollectorID == "" ||
		len(authentication.AllowedIndexNames) == 0 {
		return ingest.Authorization{}, ingest.ErrUnauthorized
	}
	return ingest.Authorization{
		SubjectID:         authentication.TokenID,
		TenantID:          authorizer.tenantID,
		CollectorID:       authentication.BoundCollectorID,
		AuthorizedIndexes: append([]string(nil), authentication.AllowedIndexNames...),
	}, nil
}

type collectorAdmissionRuntimeStore interface {
	Admit(
		context.Context,
		string,
		collectoradmission.Request,
	) (collectoradmission.Result, error)
	AuthorizeLease(
		context.Context,
		string,
		collectorfleet.Lease,
		time.Time,
	) (auth.Authentication, error)
}

type collectorFleetRuntimeStore interface {
	Disconnect(
		context.Context,
		collectorfleet.Lease,
		time.Time,
	) (bool, error)
}

type collectorHeartbeatRuntime interface {
	Activate(collectorfleet.Lease) error
	Offer(
		context.Context,
		collectorfleet.Lease,
		collectorfleet.Heartbeat,
	) (bool, error)
	Release(context.Context, collectorfleet.Lease) error
}

type collectorSessionManager struct {
	admission               collectorAdmissionRuntimeStore
	fleet                   collectorFleetRuntimeStore
	heartbeats              collectorHeartbeatRuntime
	heartbeatReleaseTimeout time.Duration
}

func (manager collectorSessionManager) Admit(
	ctx context.Context,
	bearer string,
	request ingest.CollectorSessionAdmissionRequest,
) (ingest.CollectorSessionAdmission, error) {
	if manager.admission == nil {
		return ingest.CollectorSessionAdmission{}, errors.New(
			"collector admission persistence is unavailable",
		)
	}
	result, err := manager.admission.Admit(
		ctx,
		bearer,
		collectoradmission.Request{
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			AcceptedAt:  request.AcceptedAt,
			Hello:       request.Hello,
		},
	)
	if err != nil {
		return ingest.CollectorSessionAdmission{}, mapCollectorSessionError(err)
	}
	return ingest.CollectorSessionAdmission{
		Authorization: collectorAuthenticationAuthorization(
			result.Authentication,
			result.Lease.TenantID,
		),
		Lease: result.Lease,
	}, nil
}

func (manager collectorSessionManager) AuthorizeLease(
	ctx context.Context,
	bearer string,
	lease collectorfleet.Lease,
	checkedAt time.Time,
) (ingest.Authorization, error) {
	if manager.admission == nil {
		return ingest.Authorization{}, errors.New(
			"collector admission persistence is unavailable",
		)
	}
	authentication, err := manager.admission.AuthorizeLease(
		ctx,
		bearer,
		lease,
		checkedAt,
	)
	if err != nil {
		return ingest.Authorization{}, mapCollectorSessionError(err)
	}
	return collectorAuthenticationAuthorization(
		authentication,
		lease.TenantID,
	), nil
}

func (manager collectorSessionManager) RecordHeartbeat(
	ctx context.Context,
	lease collectorfleet.Lease,
	heartbeat collectorfleet.Heartbeat,
) (bool, error) {
	if manager.heartbeats == nil {
		return false, errors.New("collector heartbeat runtime is unavailable")
	}
	accepted, err := manager.heartbeats.Offer(ctx, lease, heartbeat)
	return accepted, mapCollectorSessionError(err)
}

func (manager collectorSessionManager) Activate(
	lease collectorfleet.Lease,
) error {
	if manager.heartbeats == nil {
		return errors.New("collector heartbeat runtime is unavailable")
	}
	return mapCollectorSessionError(manager.heartbeats.Activate(lease))
}

func (manager collectorSessionManager) Disconnect(
	ctx context.Context,
	lease collectorfleet.Lease,
	receivedAt time.Time,
) (bool, error) {
	var releaseErr error
	if manager.heartbeats == nil {
		releaseErr = errors.New("collector heartbeat runtime is unavailable")
	} else {
		releaseTimeout := manager.heartbeatReleaseTimeout
		if releaseTimeout <= 0 {
			releaseTimeout = collectorHeartbeatReleaseTimeout
		}
		releaseContext, cancelRelease := context.WithTimeout(
			ctx,
			releaseTimeout,
		)
		releaseErr = manager.heartbeats.Release(releaseContext, lease)
		cancelRelease()
	}
	if manager.fleet == nil {
		return false, errors.Join(
			mapCollectorSessionError(releaseErr),
			errors.New("collector fleet persistence is unavailable"),
		)
	}
	applied, err := manager.fleet.Disconnect(ctx, lease, receivedAt)
	return applied, errors.Join(
		mapCollectorSessionError(releaseErr),
		mapCollectorSessionError(err),
	)
}

func collectorAuthenticationAuthorization(
	authentication auth.Authentication,
	tenantID string,
) ingest.Authorization {
	return ingest.Authorization{
		SubjectID:         authentication.TokenID,
		TenantID:          tenantID,
		CollectorID:       authentication.BoundCollectorID,
		AuthorizedIndexes: append([]string(nil), authentication.AllowedIndexNames...),
	}
}

func mapCollectorSessionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, auth.ErrUnauthorized),
		errors.Is(err, auth.ErrInactiveToken):
		return ingest.ErrUnauthorized
	case errors.Is(err, collectoradmission.ErrLeaseNotCurrent):
		return ingest.ErrCollectorLeaseNotCurrent
	case errors.Is(err, collectorfleet.ErrHeartbeatLeaseNotActive):
		return ingest.ErrCollectorLeaseNotCurrent
	case errors.Is(err, collectorfleet.ErrCollectorDisabled):
		return ingest.ErrCollectorDisabled
	case errors.Is(err, control.ErrInvalidArgument):
		return ingest.ErrInvalidCollectorSnapshot
	case errors.Is(err, control.ErrCapacityExceeded):
		return ingest.ErrCollectorSessionCapacity
	default:
		return err
	}
}

var _ ingest.CollectorSessionManager = collectorSessionManager{}

type indexRetentionCatalog interface {
	GetIndexByName(context.Context, string) (control.Index, error)
}

type controlRetentionProvider struct {
	catalog          indexRetentionCatalog
	tenantID         string
	defaultRetention time.Duration
}

func (provider controlRetentionProvider) RetentionForIndex(ctx context.Context, tenantID, indexName string) (time.Duration, error) {
	if provider.catalog == nil || strings.TrimSpace(provider.tenantID) == "" || tenantID != provider.tenantID {
		return 0, errors.New("resolve index retention: tenant is unavailable")
	}
	index, err := provider.catalog.GetIndexByName(ctx, indexName)
	if err != nil {
		return 0, fmt.Errorf("resolve index retention: %w", err)
	}
	if index.State != control.IndexStateActive || !index.Definition.IngestionEnabled {
		return 0, errors.New("resolve index retention: index is not ingestion-enabled")
	}
	retention := index.Definition.RetentionPeriod
	if retention == 0 {
		retention = provider.defaultRetention
	}
	if retention <= 0 {
		return 0, errors.New("resolve index retention: index has no positive retention period")
	}
	return retention, nil
}
