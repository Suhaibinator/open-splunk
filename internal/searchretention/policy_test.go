package searchretention

import (
	"errors"
	"testing"
	"time"
)

func TestSplunkCompatibleDefaults(t *testing.T) {
	manual, err := Manual(0)
	if err != nil || manual != (Decision{Class: ClassManual, Lifetime: 10 * time.Minute}) {
		t.Fatalf("Manual() = %#v, %v", manual, err)
	}
	if got := Shared(); got != (Decision{Class: ClassShared, Lifetime: 7 * 24 * time.Hour}) {
		t.Fatalf("Shared() = %#v", got)
	}
	report, err := ScheduledReport("", 15*time.Minute)
	if err != nil || report.Lifetime != 30*time.Minute {
		t.Fatalf("ScheduledReport() = %#v, %v", report, err)
	}
	alert, err := Alert("2p", "", 15*time.Minute)
	if err != nil || alert.Class != ClassTriggeredWebhook || alert.Lifetime != 150*time.Minute {
		t.Fatalf("Alert() = %#v, %v", alert, err)
	}
}

func TestResolveExplicitAndPeriodTTL(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"60", time.Minute},
		{"2p", 10 * time.Minute},
		{" 7p ", 35 * time.Minute},
	}
	for _, test := range tests {
		got, err := Resolve(test.input, 2, 5*time.Minute)
		if err != nil || got != test.want {
			t.Errorf("Resolve(%q) = %s, %v; want %s", test.input, got, err, test.want)
		}
	}
}

func TestResolveRejectsInvalidTTL(t *testing.T) {
	for _, input := range []string{"0", "0p", "-1", "1.5p", "p", "2P", "315360001"} {
		if _, err := Resolve(input, 2, time.Minute); !errors.Is(err, ErrInvalidTTL) {
			t.Errorf("Resolve(%q) error = %v", input, err)
		}
	}
}

func TestAlertUsesLongerLifetime(t *testing.T) {
	got, err := Alert("3600", "2p", 15*time.Minute)
	if err != nil || got.Lifetime != time.Hour {
		t.Fatalf("Alert() = %#v, %v", got, err)
	}
}
