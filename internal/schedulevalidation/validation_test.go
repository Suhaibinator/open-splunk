package schedulevalidation

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

func TestValidateAtReportAndAlertRetention(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.March, 8, 8, 30, 0, 0, time.UTC)

	report, err := ValidateAt(Input{
		Mode: ModeScheduledReport, Cron: " 0 * * * * ", Timezone: " UTC ", DispatchTTL: " 3p ", WebhookTTL: "invalid",
	}, at)
	if err != nil {
		t.Fatalf("ValidateAt report: %v", err)
	}
	if !report.Valid() || report.Cron != "0 * * * *" || report.Timezone != "UTC" ||
		report.Period != time.Hour || report.DispatchLifetime != 3*time.Hour ||
		report.EffectiveLifetime != 3*time.Hour || report.WebhookLifetime != 0 {
		t.Fatalf("report result = %#v", report)
	}

	alert, err := ValidateAt(Input{
		Mode: ModeWebhookAlert, Cron: "0 * * * *", Timezone: "UTC", DispatchTTL: "2p", WebhookTTL: "5p",
	}, at)
	if err != nil {
		t.Fatalf("ValidateAt alert: %v", err)
	}
	if !alert.Valid() || alert.DispatchLifetime != 2*time.Hour || alert.WebhookLifetime != 5*time.Hour || alert.EffectiveLifetime != 5*time.Hour {
		t.Fatalf("alert result = %#v", alert)
	}
}

func TestValidateAtUsesClaimedPeriodAcrossDST(t *testing.T) {
	t.Parallel()
	result, err := ValidateAt(Input{
		Mode: ModeScheduledReport, Cron: "0 1 * * *", Timezone: "America/Los_Angeles", DispatchTTL: "2p",
	}, time.Date(2026, time.March, 8, 8, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ValidateAt: %v", err)
	}
	if !result.Valid() || result.Next != time.Date(2026, time.March, 8, 9, 0, 0, 0, time.UTC) ||
		result.Period != 23*time.Hour || result.DispatchLifetime != 46*time.Hour {
		t.Fatalf("DST result = %#v", result)
	}
}

func TestValidateAtReturnsStableFieldCodedViolations(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		input Input
		want  []Violation
	}{
		{
			name:  "required schedule fields",
			input: Input{Mode: ModeScheduledReport},
			want:  []Violation{{Field: FieldCron, Code: CodeRequired}, {Field: FieldTimezone, Code: CodeRequired}},
		},
		{
			name:  "weekday seven",
			input: Input{Mode: ModeScheduledReport, Cron: "0 0 * * 7", Timezone: "UTC"},
			want:  []Violation{{Field: FieldCron, Code: CodeInvalid}},
		},
		{
			name:  "invalid timezone",
			input: Input{Mode: ModeScheduledReport, Cron: "0 0 * * *", Timezone: "OpenSplunk/Invalid"},
			want:  []Violation{{Field: FieldTimezone, Code: CodeInvalid}},
		},
		{
			name:  "report dispatch over maximum",
			input: Input{Mode: ModeScheduledReport, Cron: "0 * * * *", Timezone: "UTC", DispatchTTL: "315360001"},
			want:  []Violation{{Field: FieldDispatchTTL, Code: CodeTooLarge}},
		},
		{
			name:  "alert validates both TTLs",
			input: Input{Mode: ModeWebhookAlert, Cron: "0 * * * *", Timezone: "UTC", DispatchTTL: "bogus", WebhookTTL: "999999999p"},
			want:  []Violation{{Field: FieldDispatchTTL, Code: CodeInvalid}, {Field: FieldWebhookTTL, Code: CodeTooLarge}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := ValidateAt(test.input, at)
			if err != nil {
				t.Fatalf("ValidateAt: %v", err)
			}
			if result.Valid() || !slices.Equal(result.Violations, test.want) {
				t.Fatalf("violations = %#v, want %#v", result.Violations, test.want)
			}
		})
	}
}

func TestValidateAtRejectsInvalidClock(t *testing.T) {
	t.Parallel()
	_, err := ValidateAt(Input{Mode: ModeScheduledReport, Cron: "* * * * *", Timezone: "UTC"}, time.Time{})
	if !errors.Is(err, ErrInvalidClock) {
		t.Fatalf("error = %v, want ErrInvalidClock", err)
	}
}

func TestValidateAtDefaultsAlertTTLs(t *testing.T) {
	t.Parallel()
	result, err := ValidateAt(
		Input{Mode: ModeWebhookAlert, Cron: "*/5 * * * *", Timezone: "UTC"},
		time.Date(2026, time.August, 30, 12, 0, 1, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("ValidateAt: %v", err)
	}
	if !result.Valid() || result.DispatchLifetime != time.Duration(searchretention.DefaultDispatchPeriods)*5*time.Minute ||
		result.WebhookLifetime != time.Duration(searchretention.DefaultWebhookPeriods)*5*time.Minute {
		t.Fatalf("default TTL result = %#v", result)
	}
}
