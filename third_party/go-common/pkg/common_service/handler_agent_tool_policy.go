package common_service

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
)

type transactionalSessionToolConfigReader struct {
	commonai.SessionToolConfigReaderFunc
}

// Transaction-local snapshots must neither enter nor consume a batch cache: a
// later rollback or a different transaction must perform its own authoritative read.
func (transactionalSessionToolConfigReader) CanCacheSessionToolConfigs(ctx context.Context) bool {
	tx, ok := scontext.GetTransaction(ctx)
	return !ok || tx == nil
}
