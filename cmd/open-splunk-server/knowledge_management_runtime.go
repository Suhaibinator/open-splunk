package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const (
	knowledgeCatalogCursorKeyPurpose = "knowledge-catalog-cursors"
	lookupManagementCursorKeyPurpose = "lookup-management-cursors"
)

// runtimeKnowledgeManagement groups the same-control-database catalog,
// management, and search-admission authorities. These stores borrow the
// process database and need no independent shutdown path.
type runtimeKnowledgeManagement struct {
	catalog          *knowledgecatalog.Store
	resolver         *knowledgecatalog.Resolver
	writer           *knowledgecatalog.Writer
	attempts         *knowledgeattemptaudit.Store
	lookupAssets     *lookupasset.Store
	lookupCatalog    *lookupcatalog.Catalog
	lookupManagement *lookupservice.Service
	lookupResolver   *runtimeLookupSearchResolver
}

func newRuntimeKnowledgeManagement(
	ctx context.Context,
	database *control.DB,
	masterKeyPath string,
	auditAppender audit.TransactionAppender,
) (runtimeKnowledgeManagement, error) {
	if ctx == nil || database == nil || database.GORMDB() == nil ||
		database.SQLDB() == nil || nilRuntimeDependency(auditAppender) {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"%w: knowledge-management startup dependencies are incomplete",
			control.ErrInvalidArgument,
		)
	}
	masterKey, err := loadVerifiedMasterKey(ctx, database, masterKeyPath)
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"open knowledge-catalog master key: %w",
			err,
		)
	}
	defer clear(masterKey)

	cursorKey, err := deriveKnowledgeCatalogCursorKey(masterKey)
	if err != nil {
		return runtimeKnowledgeManagement{}, err
	}
	defer clear(cursorKey)
	lookupCursorKey, err := deriveLookupManagementCursorKey(masterKey)
	if err != nil {
		return runtimeKnowledgeManagement{}, err
	}
	defer clear(lookupCursorKey)

	catalog, err := knowledgecatalog.New(
		database,
		knowledgecatalog.Options{CursorKey: cursorKey},
	)
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create knowledge catalog: %w",
			err,
		)
	}
	resolver, err := catalog.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create knowledge resolver: %w",
			err,
		)
	}
	attempts, err := knowledgeattemptaudit.NewWithContext(ctx, database)
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create knowledge-attempt audit store: %w",
			err,
		)
	}
	writer, err := knowledgecatalog.NewWriter(
		database,
		auditAppender,
		knowledgecatalog.WriterOptions{},
	)
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create knowledge writer: %w",
			err,
		)
	}
	lookupAssets, err := lookupasset.NewStore(database, lookupasset.StoreOptions{})
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create lookup asset store: %w",
			err,
		)
	}
	lookupCatalog, err := lookupcatalog.New(
		database,
		lookupAssets,
		lookupcatalog.Options{},
	)
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create lookup catalog: %w",
			err,
		)
	}
	lookupManagement, err := lookupservice.New(lookupservice.Config{
		Assets:    lookupAssets,
		Catalog:   lookupCatalog,
		CursorKey: lookupCursorKey,
	})
	if err != nil {
		return runtimeKnowledgeManagement{}, fmt.Errorf(
			"create lookup management service: %w",
			err,
		)
	}
	lookupResolver, err := newRuntimeLookupSearchResolver(lookupCatalog)
	if err != nil {
		return runtimeKnowledgeManagement{}, err
	}
	return runtimeKnowledgeManagement{
		catalog:          catalog,
		resolver:         resolver,
		writer:           writer,
		attempts:         attempts,
		lookupAssets:     lookupAssets,
		lookupCatalog:    lookupCatalog,
		lookupManagement: lookupManagement,
		lookupResolver:   lookupResolver,
	}, nil
}

func deriveKnowledgeCatalogCursorKey(masterKey []byte) ([]byte, error) {
	key, err := deriveServerKey(masterKey, knowledgeCatalogCursorKeyPurpose)
	if err != nil {
		return nil, fmt.Errorf("derive knowledge-catalog cursor key: %w", err)
	}
	return key, nil
}

func deriveLookupManagementCursorKey(masterKey []byte) ([]byte, error) {
	key, err := deriveServerKey(masterKey, lookupManagementCursorKeyPurpose)
	if err != nil {
		return nil, fmt.Errorf("derive lookup-management cursor key: %w", err)
	}
	return key, nil
}

func configureRuntimeKnowledgeManagement(
	config *server.Config,
	runtime runtimeKnowledgeManagement,
	apps *runtimeAppCatalog,
	preview *knowledgepreview.Service,
) error {
	if config == nil || runtime.catalog == nil || runtime.resolver == nil ||
		runtime.writer == nil ||
		!runtime.writer.ReadyForManagement() || runtime.attempts == nil ||
		runtime.lookupAssets == nil || runtime.lookupCatalog == nil ||
		runtime.lookupManagement == nil || !runtime.lookupManagement.Ready() ||
		runtime.lookupResolver == nil || runtime.lookupResolver.catalog == nil ||
		runtime.lookupResolver.catalog != runtime.lookupCatalog || apps == nil ||
		nilRuntimeDependency(apps.catalog) || preview == nil || !preview.Ready() {
		return errors.New(
			"configure knowledge runtime: dependencies are incomplete",
		)
	}
	if config.KnowledgeCatalog != nil || config.KnowledgeWriter != nil ||
		config.KnowledgeApps != nil || config.KnowledgeAttempts != nil ||
		config.KnowledgePreview != nil || config.LookupManagement != nil {
		return errors.New(
			"configure knowledge runtime: dependencies are already configured",
		)
	}
	config.KnowledgeCatalog = runtime.catalog
	config.KnowledgeWriter = runtime.writer
	config.KnowledgeApps = apps
	config.KnowledgeAttempts = runtime.attempts
	config.KnowledgePreview = preview
	config.LookupManagement = runtime.lookupManagement
	return nil
}
