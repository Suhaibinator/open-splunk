package knowledgecatalog_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"gorm.io/gorm"
)

type writerReadinessPanicAppender struct{}

func (writerReadinessPanicAppender) AppendInTransaction(
	context.Context,
	*gorm.DB,
	string,
	audit.SuccessfulEvent,
) (audit.Event, error) {
	panic("ReadyForManagement called the audit appender")
}

func TestWriterReadyForManagementRequiresConstructorAuthority(t *testing.T) {
	var nilWriter *knowledgecatalog.Writer
	if nilWriter.ReadyForManagement() {
		t.Fatal("nil writer reported ready for management")
	}
	if (&knowledgecatalog.Writer{}).ReadyForManagement() {
		t.Fatal("zero writer reported ready for management")
	}

	database, closeDatabase := openWriterReadinessDatabase(t)
	writer, err := knowledgecatalog.NewWriter(
		database,
		writerReadinessPanicAppender{},
		knowledgecatalog.WriterOptions{},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(): %v", err)
	}
	if !writer.ReadyForManagement() {
		t.Fatal("constructor-initialized writer did not report ready for management")
	}
	closeDatabase()
	if !writer.ReadyForManagement() {
		t.Fatal("writer readiness probed closed storage")
	}
}

func TestNewWriterRejectsTypedNilAuditAppender(t *testing.T) {
	database, _ := openWriterReadinessDatabase(t)
	var auditStore *audit.Store

	writer, err := knowledgecatalog.NewWriter(
		database,
		auditStore,
		knowledgecatalog.WriterOptions{},
	)
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewWriter() error = %v, want control.ErrInvalidArgument", err)
	}
	if writer != nil {
		t.Fatalf("NewWriter() writer = %#v, want nil", writer)
	}
}

func openWriterReadinessDatabase(t *testing.T) (*control.DB, func()) {
	t.Helper()

	database, err := control.Open(t.Context(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	closed := false
	closeDatabase := func() {
		if closed {
			return
		}
		closed = true
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	}
	t.Cleanup(closeDatabase)
	return database, closeDatabase
}
