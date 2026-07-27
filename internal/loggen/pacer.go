package loggen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	maxCatchUpEvents = 100
	maxScheduleDebt  = 100 * time.Millisecond
)

// Pacer limits event generation to an absolute ordinal schedule while it is
// on pace. Encoding and write time therefore do not accumulate as additional
// delay, and catch-up after missed deadlines is bounded.
//
// A Pacer is not safe for concurrent use.
type Pacer struct {
	start    time.Time
	interval time.Duration
	schedule ordinalSchedule
	timer    *time.Timer
	started  bool
	closed   bool
}

// NewPacer creates a pacer for rate events per second. A zero rate disables
// pacing. Positive rates must have a representable interval of at least one
// nanosecond.
func NewPacer(rate float64) (*Pacer, error) {
	pacer, err := newPacerAt(rate, time.Time{})
	if err != nil {
		return nil, err
	}
	pacer.started = false
	return pacer, nil
}

func newPacerAt(rate float64, start time.Time) (*Pacer, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 {
		return nil, errors.New("rate must be finite and non-negative")
	}
	if rate == 0 {
		schedule, _ := newOrdinalSchedule(0)
		return &Pacer{start: start, schedule: schedule, started: true}, nil
	}

	rawIntervalNanos := float64(time.Second) / rate
	if rawIntervalNanos < 1 {
		return nil, errors.New("rate is too large")
	}
	if rawIntervalNanos >= float64(math.MaxInt64) {
		return nil, errors.New("rate is too small")
	}
	interval := time.Duration(math.Ceil(rawIntervalNanos))
	schedule, scheduleOK := newOrdinalSchedule(interval)
	if !scheduleOK {
		return nil, errors.New("rate produced an invalid interval")
	}
	return &Pacer{
		start:    start,
		interval: interval,
		schedule: schedule,
		timer:    time.NewTimer(time.Hour),
		started:  true,
	}, nil
}

// Wait blocks until the deadline for ordinal. Ordinal zero is due immediately
// on the first call. Wait caps catch-up debt at the smaller of 100 events or
// 100 milliseconds, preventing an unbounded burst after a long stall.
func (p *Pacer) Wait(ctx context.Context, ordinal uint64) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.closed {
		return errors.New("pacer is closed")
	}
	if p.interval == 0 {
		return nil
	}
	if !p.started {
		p.start = time.Now()
		p.started = true
	}
	offset, err := p.scheduleOffset(ordinal)
	if err != nil {
		return err
	}
	deadline := p.start.Add(offset)

	now := time.Now()
	debtLimit := maxScheduleDebt
	if p.interval < maxScheduleDebt/maxCatchUpEvents {
		debtLimit = p.interval * maxCatchUpEvents
	}
	if now.Sub(deadline) >= debtLimit {
		p.start = now.Add(-offset)
		deadline = now
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	p.resetTimer(delay)
	select {
	case <-ctx.Done():
		p.stopTimer()
		return ctx.Err()
	case <-p.timer.C:
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
}

func (p *Pacer) deadline(ordinal uint64) (time.Time, error) {
	offset, err := p.scheduleOffset(ordinal)
	if err != nil {
		return time.Time{}, err
	}
	return p.start.Add(offset), nil
}

func (p *Pacer) scheduleOffset(ordinal uint64) (time.Duration, error) {
	offset, ok := p.schedule.offset(ordinal)
	if !ok {
		return 0, fmt.Errorf("event ordinal %d exceeds the pacing deadline range", ordinal)
	}
	return offset, nil
}

func (p *Pacer) resetTimer(delay time.Duration) {
	p.stopTimer()
	p.timer.Reset(delay)
}

func (p *Pacer) stopTimer() {
	if p.timer == nil || p.timer.Stop() {
		return
	}
	select {
	case <-p.timer.C:
	default:
	}
}

// Close releases the pacer's timer. Close is idempotent.
func (p *Pacer) Close() {
	if p == nil || p.closed {
		return
	}
	p.closed = true
	p.stopTimer()
}
