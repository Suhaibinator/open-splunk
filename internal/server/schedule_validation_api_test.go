package server

import (
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestValidateScheduleReturnsStableFieldCodedViolations(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{now: func() time.Time {
		return time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	}}
	response, err := handler.validateSchedule(nil, &opensplunk.ValidateScheduleRequest{
		Mode:        opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_WEBHOOK_ALERT,
		Cron:        "0 * * * *",
		Timezone:    "UTC",
		DispatchTtl: "315360001",
		WebhookTtl:  "999999999p",
	})
	if err != nil {
		t.Fatalf("validate schedule: %v", err)
	}
	if response.GetValid() || len(response.GetViolations()) != 2 {
		t.Fatalf("response = %#v", response)
	}
	wantFields := []opensplunk.ScheduleValidationField{
		opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_DISPATCH_TTL,
		opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_WEBHOOK_TTL,
	}
	for index, violation := range response.GetViolations() {
		if violation.GetField() != wantFields[index] || violation.GetCode() == opensplunk.ScheduleValidationCode_SCHEDULE_VALIDATION_CODE_UNSPECIFIED || violation.GetMessage() == "" {
			t.Fatalf("violation[%d] = %#v", index, violation)
		}
	}
	weekdayResponse, weekdayErr := handler.validateSchedule(nil, &opensplunk.ValidateScheduleRequest{
		Mode:        opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
		Cron:        "0 0 * * 7",
		Timezone:    "UTC",
		DispatchTtl: "2p",
	})
	if weekdayErr != nil || weekdayResponse.GetValid() || len(weekdayResponse.GetViolations()) != 1 ||
		weekdayResponse.GetViolations()[0].GetField() != opensplunk.ScheduleValidationField_SCHEDULE_VALIDATION_FIELD_CRON {
		t.Fatalf("weekday response = %#v, error = %v", weekdayResponse, weekdayErr)
	}
}

func TestValidateScheduleAcceptsDSTPeriodRetention(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{now: func() time.Time {
		return time.Date(2026, time.March, 8, 8, 30, 0, 0, time.UTC)
	}}
	response, err := handler.validateSchedule(nil, &opensplunk.ValidateScheduleRequest{
		Mode:        opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
		Cron:        "0 1 * * *",
		Timezone:    "America/Los_Angeles",
		DispatchTtl: "2p",
	})
	if err != nil {
		t.Fatalf("validate schedule: %v", err)
	}
	if !response.GetValid() || len(response.GetViolations()) != 0 {
		t.Fatalf("response = %#v", response)
	}
}

func TestValidateScheduleRejectsUnknownMode(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{now: time.Now}
	if _, err := handler.validateSchedule(nil, &opensplunk.ValidateScheduleRequest{}); err == nil {
		t.Fatal("expected unspecified mode to fail")
	}
}
