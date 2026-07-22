package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

const maximumDurableTenantIDBytes = 255

func normalizeRuntimeOptions(config *options) error {
	if config == nil {
		return errors.New("server options are required")
	}
	if config.indexRetention <= 0 {
		return errors.New("default index retention must be positive")
	}
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
	if !loopbackAddress(config.httpAddress) && !config.httpInsecureTrustedNetwork {
		return errors.New("plaintext browser HTTP on a non-loopback address requires -http-insecure-trusted-network")
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
	config.tenantID = strings.TrimSpace(config.tenantID)
	if config.tenantID == "" || len(config.tenantID) > maximumDurableTenantIDBytes ||
		!utf8.ValidString(config.tenantID) || strings.IndexByte(config.tenantID, 0) >= 0 {
		return fmt.Errorf("tenant ID must be non-empty valid UTF-8 without NUL bytes and at most %d bytes", maximumDurableTenantIDBytes)
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
	if authentication.TokenID == "" || len(authentication.AllowedIndexNames) == 0 {
		return ingest.Authorization{}, ingest.ErrUnauthorized
	}
	return ingest.Authorization{
		SubjectID:         authentication.TokenID,
		TenantID:          authorizer.tenantID,
		AuthorizedIndexes: append([]string(nil), authentication.AllowedIndexNames...),
	}, nil
}

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
