// Package searchretention centralizes Splunk-compatible retained-result
// lifetime decisions. Callers snapshot the returned duration at admission.
package searchretention

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ManualLifetime            = 10 * time.Minute
	SharedLifetime            = 7 * 24 * time.Hour
	DefaultDispatchPeriods    = uint32(2)
	DefaultWebhookPeriods     = uint32(10)
	DefaultDispatchExpression = "2p"
	DefaultWebhookExpression  = "10p"
	MaximumLifetime           = 10 * 365 * 24 * time.Hour
	MaximumTTLExpressionBytes = 32
)

var ErrInvalidTTL = errors.New("invalid retention TTL")

// Class identifies why a job has its current sliding lifetime.
type Class uint8

const (
	ClassInvalid Class = iota
	ClassManual
	ClassShared
	ClassScheduledReport
	ClassScheduledAlert
	ClassTriggeredWebhook
)

// Decision is an immutable retention snapshot stored with one job.
type Decision struct {
	Class    Class
	Lifetime time.Duration
}

// Manual returns the fresh-install default unless an existing explicit
// server setting was supplied. The setting is deliberately snapshotted.
func Manual(explicit time.Duration) (Decision, error) {
	if explicit == 0 {
		explicit = ManualLifetime
	}
	if err := validateLifetime(explicit); err != nil {
		return Decision{}, err
	}
	return Decision{Class: ClassManual, Lifetime: explicit}, nil
}

func Shared() Decision { return Decision{Class: ClassShared, Lifetime: SharedLifetime} }

func ScheduledReport(expression string, period time.Duration) (Decision, error) {
	lifetime, err := Resolve(expression, DefaultDispatchPeriods, period)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Class: ClassScheduledReport, Lifetime: lifetime}, nil
}

func ScheduledAlert(expression string, period time.Duration) (Decision, error) {
	lifetime, err := Resolve(expression, DefaultDispatchPeriods, period)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Class: ClassScheduledAlert, Lifetime: lifetime}, nil
}

func Alert(dispatchExpression, webhookExpression string, period time.Duration) (Decision, error) {
	dispatch, err := Resolve(dispatchExpression, DefaultDispatchPeriods, period)
	if err != nil {
		return Decision{}, fmt.Errorf("dispatch TTL: %w", err)
	}
	webhook, err := Resolve(webhookExpression, DefaultWebhookPeriods, period)
	if err != nil {
		return Decision{}, fmt.Errorf("webhook TTL: %w", err)
	}
	if webhook > dispatch {
		dispatch = webhook
	}
	return Decision{Class: ClassTriggeredWebhook, Lifetime: dispatch}, nil
}

// Resolve accepts either positive integer seconds or an Np schedule-period
// multiplier. An empty value uses defaultPeriods.
func Resolve(expression string, defaultPeriods uint32, period time.Duration) (time.Duration, error) {
	expression = strings.TrimSpace(expression)
	if len(expression) > MaximumTTLExpressionBytes || period <= 0 {
		return 0, ErrInvalidTTL
	}
	if expression == "" {
		return multiplyPeriod(defaultPeriods, period)
	}
	if value, periodExpression := strings.CutSuffix(expression, "p"); periodExpression {
		periods, err := strconv.ParseUint(value, 10, 32)
		if err != nil || periods == 0 {
			return 0, ErrInvalidTTL
		}
		return multiplyPeriod(uint32(periods), period)
	}
	seconds, err := strconv.ParseUint(expression, 10, 63)
	if err != nil || seconds == 0 || seconds > uint64(MaximumLifetime/time.Second) {
		return 0, ErrInvalidTTL
	}
	lifetime := time.Duration(seconds) * time.Second
	if err := validateLifetime(lifetime); err != nil {
		return 0, err
	}
	return lifetime, nil
}

func multiplyPeriod(periods uint32, period time.Duration) (time.Duration, error) {
	if periods == 0 || period <= 0 || uint64(period) > uint64(MaximumLifetime)/uint64(periods) {
		return 0, ErrInvalidTTL
	}
	lifetime := time.Duration(periods) * period
	if err := validateLifetime(lifetime); err != nil {
		return 0, err
	}
	return lifetime, nil
}

func validateLifetime(lifetime time.Duration) error {
	if lifetime <= 0 || lifetime > MaximumLifetime {
		return ErrInvalidTTL
	}
	return nil
}
