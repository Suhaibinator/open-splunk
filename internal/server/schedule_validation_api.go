package server

import (
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/schedulevalidation"
)

var errScheduleValidationProjection = errors.New("schedule-validation projection is unsupported")

func (handler *apiHandler) scheduleValidationRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunk.ValidateScheduleRequest, *opensplunk.ValidateScheduleResponse](router.RouteConfig[*opensplunk.ValidateScheduleRequest, *opensplunk.ValidateScheduleResponse]{
			Path: "/schedules/validate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ValidateScheduleRequest, *opensplunk.ValidateScheduleResponse](), Handler: handler.validateSchedule,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) validateSchedule(_ *http.Request, input *opensplunk.ValidateScheduleRequest) (*opensplunk.ValidateScheduleResponse, error) {
	mode, err := scheduleValidationMode(input.GetMode())
	if err != nil {
		return nil, err
	}
	result, validationErr := schedulevalidation.ValidateAt(schedulevalidation.Input{
		Mode:        mode,
		Cron:        input.GetCron(),
		Timezone:    input.GetTimezone(),
		DispatchTTL: input.GetDispatchTtl(),
		WebhookTTL:  input.GetWebhookTtl(),
	}, handler.now())
	if validationErr != nil {
		return nil, internalError()
	}
	violations := make([]*opensplunk.ScheduleValidationViolation, len(result.Violations))
	for index, violation := range result.Violations {
		field, fieldErr := scheduleValidationProtoField(violation.Field)
		code, codeErr := scheduleValidationProtoCode(violation.Code)
		if fieldErr != nil || codeErr != nil {
			return nil, internalError()
		}
		violations[index] = &opensplunk.ScheduleValidationViolation{
			Field: field,
			Code:  code,
			Message: scheduleValidationMessage(
				violation.Field,
				violation.Code,
			),
		}
	}
	return &opensplunk.ValidateScheduleResponse{Valid: result.Valid(), Violations: violations}, nil
}

func scheduleValidationMode(mode opensplunk.ScheduleValidationMode) (schedulevalidation.Mode, error) {
	switch mode {
	case opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT:
		return schedulevalidation.ModeScheduledReport, nil
	case opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_WEBHOOK_ALERT:
		return schedulevalidation.ModeWebhookAlert, nil
	case opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_UNSPECIFIED:
		return schedulevalidation.ModeInvalid, badRequestError("schedule-validation mode is required")
	default:
		return schedulevalidation.ModeInvalid, badRequestError("schedule-validation mode is unsupported")
	}
}

func scheduleValidationProtoField(field schedulevalidation.Field) (opensplunk.ScheduleValidationField, error) {
	switch field {
	case schedulevalidation.FieldCron:
		return opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_CRON, nil
	case schedulevalidation.FieldTimezone:
		return opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_TIMEZONE, nil
	case schedulevalidation.FieldDispatchTTL:
		return opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_DISPATCH_TTL, nil
	case schedulevalidation.FieldWebhookTTL:
		return opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_WEBHOOK_TTL, nil
	default:
		return opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_UNSPECIFIED, errScheduleValidationProjection
	}
}

func scheduleValidationProtoCode(code schedulevalidation.Code) (opensplunk.ScheduleValidationCode, error) {
	switch code {
	case schedulevalidation.CodeRequired:
		return opensplunk.ScheduleValidationCode_SCHEDULE_VALIDATION_CODE_REQUIRED, nil
	case schedulevalidation.CodeInvalid:
		return opensplunk.ScheduleValidationCode_SCHEDULE_VALIDATION_CODE_INVALID, nil
	case schedulevalidation.CodeTooLarge:
		return opensplunk.ScheduleValidationCode_SCHEDULE_VALIDATION_CODE_TOO_LARGE, nil
	default:
		return opensplunk.ScheduleValidationCode_SCHEDULE_VALIDATION_CODE_UNSPECIFIED, errScheduleValidationProjection
	}
}

func scheduleValidationMessage(field schedulevalidation.Field, code schedulevalidation.Code) string {
	if code == schedulevalidation.CodeRequired {
		switch field {
		case schedulevalidation.FieldCron:
			return "Cron schedule is required."
		case schedulevalidation.FieldTimezone:
			return "Schedule timezone is required."
		case schedulevalidation.FieldDispatchTTL:
			return "Dispatch retention is required."
		case schedulevalidation.FieldWebhookTTL:
			return "Webhook retention is required."
		}
	}
	if code == schedulevalidation.CodeTooLarge {
		return "Retention cannot exceed ten years for this schedule."
	}
	switch field {
	case schedulevalidation.FieldCron:
		return "Use a strict five-field cron schedule with weekday values from 0 through 6."
	case schedulevalidation.FieldTimezone:
		return "Choose a valid IANA timezone, such as UTC or America/Los_Angeles."
	case schedulevalidation.FieldDispatchTTL:
		return "Dispatch retention must be positive seconds or a schedule multiplier such as 2p."
	case schedulevalidation.FieldWebhookTTL:
		return "Webhook retention must be positive seconds or a schedule multiplier such as 10p."
	default:
		return "Schedule configuration is invalid."
	}
}
