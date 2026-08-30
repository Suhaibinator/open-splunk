// Package schedulevalidation centralizes server-authoritative validation of
// scheduled-report and webhook-alert timing and retained-result settings.
package schedulevalidation

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

// Mode selects the retention rules applied to an otherwise shared schedule.
type Mode uint8

const (
	ModeInvalid Mode = iota
	ModeScheduledReport
	ModeWebhookAlert
)

// Field is a stable machine-readable input location for one violation.
type Field string

const (
	FieldMode        Field = "mode"
	FieldCron        Field = "cron"
	FieldTimezone    Field = "timezone"
	FieldDispatchTTL Field = "dispatch_ttl"
	FieldWebhookTTL  Field = "webhook_ttl"
)

// Code is a stable machine-readable validation failure category.
type Code string

const (
	CodeRequired Code = "required"
	CodeInvalid  Code = "invalid"
	CodeTooLarge Code = "too_large"
)

// ErrInvalidClock identifies a programmer-supplied validation clock that
// cannot be used to calculate the next occurrence.
var ErrInvalidClock = errors.New("schedule validation clock is invalid")

// Input is raw operator intent. All text is trimmed before validation.
type Input struct {
	Mode        Mode
	Cron        string
	Timezone    string
	DispatchTTL string
	WebhookTTL  string
}

// Violation identifies one invalid field without exposing parser-dependent
// error text as an API contract.
type Violation struct {
	Field Field
	Code  Code
}

// Result contains normalized input and the timing decisions derived from it.
// Lifetime values are zero when their corresponding field is invalid.
type Result struct {
	Mode              Mode
	Cron              string
	Timezone          string
	DispatchTTL       string
	WebhookTTL        string
	Next              time.Time
	Period            time.Duration
	DispatchLifetime  time.Duration
	WebhookLifetime   time.Duration
	EffectiveLifetime time.Duration
	Violations        []Violation
}

// Valid reports whether every applicable field passed validation.
func (result Result) Valid() bool { return len(result.Violations) == 0 }

// ValidateAt parses the five-field cron in its IANA timezone, calculates the
// next claimed period at the supplied clock, and applies the mode-specific
// retention rules. Invalid user input is returned as field-coded violations;
// only an invalid injected clock is returned as an error.
func ValidateAt(input Input, at time.Time) (Result, error) {
	if at.IsZero() || at.UnixMicro() <= 0 {
		return Result{}, ErrInvalidClock
	}
	result := Result{
		Mode:        input.Mode,
		Cron:        strings.TrimSpace(input.Cron),
		Timezone:    strings.TrimSpace(input.Timezone),
		DispatchTTL: strings.TrimSpace(input.DispatchTTL),
		WebhookTTL:  strings.TrimSpace(input.WebhookTTL),
	}
	if input.Mode != ModeScheduledReport && input.Mode != ModeWebhookAlert {
		result.add(FieldMode, CodeInvalid)
	}

	cronValid := true
	if result.Cron == "" {
		result.add(FieldCron, CodeRequired)
		cronValid = false
	} else if _, err := scheduler.ParseCron(result.Cron, "UTC"); err != nil {
		result.add(FieldCron, CodeInvalid)
		cronValid = false
	}
	timezoneValid := true
	if result.Timezone == "" {
		result.add(FieldTimezone, CodeRequired)
		timezoneValid = false
	} else if _, err := scheduler.ParseCron("* * * * *", result.Timezone); err != nil {
		result.add(FieldTimezone, CodeInvalid)
		timezoneValid = false
	}
	if !cronValid || !timezoneValid {
		return result, nil
	}

	parsed, err := scheduler.ParseCron(result.Cron, result.Timezone)
	if err != nil {
		result.add(FieldCron, CodeInvalid)
		return result, nil
	}
	result.Cron = parsed.Expression()
	result.Timezone = parsed.Timezone()
	result.Next = parsed.Next(at)
	result.Period, err = parsed.Period(result.Next)
	if err != nil {
		result.add(FieldCron, CodeInvalid)
		return result, nil
	}

	switch input.Mode {
	case ModeScheduledReport:
		decision, retentionErr := searchretention.ScheduledReport(result.DispatchTTL, result.Period)
		if retentionErr != nil {
			result.add(FieldDispatchTTL, ttlViolationCode(result.DispatchTTL, result.Period, searchretention.DefaultDispatchPeriods))
			return result, nil
		}
		result.DispatchLifetime = decision.Lifetime
		result.EffectiveLifetime = decision.Lifetime
	case ModeWebhookAlert:
		dispatch, dispatchErr := searchretention.ScheduledAlert(result.DispatchTTL, result.Period)
		if dispatchErr != nil {
			result.add(FieldDispatchTTL, ttlViolationCode(result.DispatchTTL, result.Period, searchretention.DefaultDispatchPeriods))
		} else {
			result.DispatchLifetime = dispatch.Lifetime
		}
		webhook, webhookErr := searchretention.Resolve(result.WebhookTTL, searchretention.DefaultWebhookPeriods, result.Period)
		if webhookErr != nil {
			result.add(FieldWebhookTTL, ttlViolationCode(result.WebhookTTL, result.Period, searchretention.DefaultWebhookPeriods))
		} else {
			result.WebhookLifetime = webhook
		}
		if dispatchErr == nil && webhookErr == nil {
			result.EffectiveLifetime = max(result.DispatchLifetime, result.WebhookLifetime)
		}
	}
	return result, nil
}

func (result *Result) add(field Field, code Code) {
	result.Violations = append(result.Violations, Violation{Field: field, Code: code})
}

func ttlViolationCode(expression string, period time.Duration, defaultPeriods uint32) Code {
	if len(expression) > searchretention.MaximumTTLExpressionBytes {
		return CodeTooLarge
	}
	if expression == "" {
		if period > searchretention.MaximumLifetime/time.Duration(defaultPeriods) {
			return CodeTooLarge
		}
		return CodeInvalid
	}
	digits := expression
	periods := false
	if value, found := strings.CutSuffix(expression, "p"); found {
		digits = value
		periods = true
	}
	if digits == "" || strings.IndexFunc(digits, func(value rune) bool { return value < '0' || value > '9' }) >= 0 {
		return CodeInvalid
	}
	bits := 63
	if periods {
		bits = 32
	}
	value, err := strconv.ParseUint(digits, 10, bits)
	if err != nil {
		return CodeTooLarge
	}
	if value == 0 {
		return CodeInvalid
	}
	if periods {
		periodCount, conversionErr := safecast.Conv[time.Duration](value)
		if conversionErr != nil || period > searchretention.MaximumLifetime/periodCount {
			return CodeTooLarge
		}
	} else if value > uint64(searchretention.MaximumLifetime/time.Second) {
		return CodeTooLarge
	}
	return CodeInvalid
}
