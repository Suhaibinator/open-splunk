package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

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
