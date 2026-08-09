package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const knowledgeCatalogCursorKeyPurpose = "knowledge-catalog-cursors"

// runtimeKnowledgeManagement groups the same-control-database authorities for
// the management API. These stores borrow the process database and need no
// independent shutdown path.
type runtimeKnowledgeManagement struct {
	catalog  *knowledgecatalog.Store
	writer   *knowledgecatalog.Writer
	attempts *knowledgeattemptaudit.Store
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
	return runtimeKnowledgeManagement{
		catalog:  catalog,
		writer:   writer,
		attempts: attempts,
	}, nil
}

func deriveKnowledgeCatalogCursorKey(masterKey []byte) ([]byte, error) {
	key, err := deriveServerKey(masterKey, knowledgeCatalogCursorKeyPurpose)
	if err != nil {
		return nil, fmt.Errorf("derive knowledge-catalog cursor key: %w", err)
	}
	return key, nil
}

func configureRuntimeKnowledgeManagement(
	config *server.Config,
	runtime runtimeKnowledgeManagement,
	apps *runtimeAppCatalog,
) error {
	if config == nil || runtime.catalog == nil || runtime.writer == nil ||
		!runtime.writer.ReadyForManagement() || runtime.attempts == nil || apps == nil ||
		nilRuntimeDependency(apps.catalog) {
		return errors.New(
			"configure knowledge management: dependencies are incomplete",
		)
	}
	if config.KnowledgeCatalog != nil || config.KnowledgeWriter != nil ||
		config.KnowledgeApps != nil || config.KnowledgeAttempts != nil {
		return errors.New(
			"configure knowledge management: dependencies are already configured",
		)
	}
	config.KnowledgeCatalog = runtime.catalog
	config.KnowledgeWriter = runtime.writer
	config.KnowledgeApps = apps
	config.KnowledgeAttempts = runtime.attempts
	return nil
}
