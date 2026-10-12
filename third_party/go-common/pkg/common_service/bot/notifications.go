package bot

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

func (d *AsyncDispatcher[U]) SetNotificationSink(sink ports.ChatNotificationSink) {
	d.notificationSink = sink
}

func (w *agentTranscriptWriter[U]) createTranscriptWithNotification(in persistence.CreateTranscriptMessageInput) (uint64, bool, error) {
	if w.d.notificationSink == nil {
		return w.d.persistenceHandler.CreateTranscriptMessage(w.ctx, in)
	}
	write := func(ctx context.Context) (uint64, bool, error) {
		id, created, err := w.d.persistenceHandler.CreateTranscriptMessage(ctx, in)
		if err == nil && created {
			err = w.d.notificationSink.ChatMessageRecorded(ctx, id)
		}
		return id, created, err
	}
	if tx, ok := scontext.GetTransaction(w.ctx); ok && tx != nil {
		return write(w.ctx)
	}
	tx, err := w.d.persistenceHandler.BeginTransaction(w.ctx)
	if err != nil {
		return 0, false, err
	}
	hooks := &persistence.GormTransactionWithHooks{Tx: tx}
	defer hooks.Rollback()
	ctx := scontext.CopySRouterContext[common_service_models.UserIdType, U](w.ctx, w.ctx)
	ctx = scontext.WithTransaction[common_service_models.UserIdType, U](ctx, hooks)
	id, created, err := write(ctx)
	if err != nil {
		return 0, false, err
	}
	if err = hooks.Commit(); err != nil {
		return 0, false, err
	}
	w.d.runPostCommitHooks(w.ctx, hooks)
	return id, created, nil
}
