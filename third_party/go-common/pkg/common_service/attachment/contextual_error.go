package attachment

import (
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

func contextualAttachmentError(err error, fields ...zap.Field) error {
	return appLogger.WithErrorFields(err, fields...)
}

func validateAttachmentUploadResultIndex(index, count int) error {
	if index >= 0 && index < count {
		return nil
	}
	cause := fmt.Errorf("invalid index %d in URL generation result", index)
	return appLogger.InvariantError("attachment_upload_url_result_index_in_range", cause,
		zap.String(logkeys.Stage, "collect_bulk_attachment_upload_urls"),
		zap.Int(appLogger.KeyIndex, index),
		zap.Int(appLogger.KeyAttachmentCount, count),
		zap.String(logkeys.Expected, "result index within attachment batch"),
		zap.Int(logkeys.Actual, index))
}
