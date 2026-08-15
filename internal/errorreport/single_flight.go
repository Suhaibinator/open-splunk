// Package errorreport delivers background error callbacks without letting a
// slow or panicking callback stall or crash the reporting worker.
package errorreport

import "sync"

// SingleFlight delivers at most one in-flight error callback, dropping reports
// while a previous callback is still running and containing its panics. The
// zero value drops every report until Callback is set at construction. An
// initialized SingleFlight must never be copied.
type SingleFlight struct {
	Callback func(error)
	mu       sync.Mutex
	alive    bool
}

// Report hands err to the callback on its own goroutine, unless a previous
// callback has not returned yet, in which case the report is dropped.
func (reporter *SingleFlight) Report(err error) {
	if reporter == nil || err == nil || reporter.Callback == nil {
		return
	}
	reporter.mu.Lock()
	if reporter.alive {
		reporter.mu.Unlock()
		return
	}
	reporter.alive = true
	reporter.mu.Unlock()
	go func() {
		defer func() {
			_ = recover()
			reporter.mu.Lock()
			reporter.alive = false
			reporter.mu.Unlock()
		}()
		reporter.Callback(err)
	}()
}
