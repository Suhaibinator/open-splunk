package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestHTTPServerErrorLogSharesHandshakeBudgetAndRenews(t *testing.T) {
	t.Parallel()
	clock := &httpErrorLogClock{now: time.Now()}
	core, observed := observer.New(zapcore.ErrorLevel)
	process := zap.New(core, zap.WithClock(clock))
	errorLog := newHTTPServerErrorLog(process)

	for interval := range 2 {
		for index := range httpTLSHandshakeLogLimit * 4 {
			remote := fmt.Sprintf("192.0.2.1:%d", index)
			if index%2 == 0 {
				remote = fmt.Sprintf("[2001:db8::%x]:443", index)
			}
			errorLog.Printf("http: TLS handshake error from %s: test failure %d", remote, index)
		}
		if got, want := observed.Len(), (interval+1)*httpTLSHandshakeLogLimit; got != want {
			t.Fatalf("handshake records = %d, want %d", got, want)
		}
		clock.now = clock.now.Add(httpTLSHandshakeLogInterval)
	}
	first := observed.All()[0]
	if first.Message != "TLS handshake failed" || first.LoggerName != "http.server" ||
		first.ContextMap()["detail"] != "http: TLS handshake error from [2001:db8::0]:443: test failure 0" {
		t.Fatalf("first handshake diagnostic = %#v", first)
	}

	before := observed.Len()
	for range httpTLSHandshakeLogLimit * 4 {
		process.Error("TLS handshake failed")
	}
	if got := observed.Len() - before; got != httpTLSHandshakeLogLimit*4 {
		t.Fatalf("process records = %d, want unsampled process logger", got)
	}
}

func TestHTTPServerErrorLogPreservesUnexpectedDiagnostics(t *testing.T) {
	t.Parallel()
	clock := &httpErrorLogClock{now: time.Now()}
	core, observed := observer.New(zapcore.ErrorLevel)
	errorLog := newHTTPServerErrorLog(zap.New(core, zap.WithClock(clock)))
	diagnostics := []string{
		"http: Accept error: temporary listener failure",
		"http: panic serving 192.0.2.1:443: test panic\nhttp: TLS handshake error from panic text\nstack trace",
		"http: superfluous response.WriteHeader call from testHandler",
	}
	for phase := range 2 {
		for _, diagnostic := range diagnostics {
			errorLog.Print(diagnostic)
		}
		if got := observed.FilterMessage("HTTP server error").Len(); got != (phase+1)*len(diagnostics) {
			t.Fatalf("unexpected server records = %d", got)
		}
		for range httpTLSHandshakeLogLimit * 4 {
			errorLog.Print("http: TLS handshake error from 192.0.2.1:443: EOF")
		}
	}
	for index, entry := range observed.FilterMessage("HTTP server error").All() {
		if got, want := entry.ContextMap()["detail"], diagnostics[index%len(diagnostics)]; got != want {
			t.Fatalf("server diagnostic = %q, want %q", got, want)
		}
	}
	const suppressed = "http: TLS handshake error from 192.0.2.2:443: EOF\n"
	if count, err := errorLog.Writer().Write([]byte(suppressed)); count != len(suppressed) || err != nil {
		t.Fatalf("suppressed Write = (%d, %v)", count, err)
	}
}

func TestHTTPServerErrorLogBoundsConcurrentHandshakeRecords(t *testing.T) {
	t.Parallel()
	core, observed := observer.New(zapcore.ErrorLevel)
	errorLog := newHTTPServerErrorLog(zap.New(core, zap.WithClock(&httpErrorLogClock{now: time.Now()})))
	var writers sync.WaitGroup
	for index := range httpTLSHandshakeLogLimit * 4 {
		writers.Go(func() {
			errorLog.Printf("http: TLS handshake error from 192.0.2.1:%d: EOF", index)
		})
	}
	writers.Wait()
	if got := observed.Len(); got != httpTLSHandshakeLogLimit {
		t.Fatalf("concurrent handshake records = %d, want %d", got, httpTLSHandshakeLogLimit)
	}
}

func TestHTTPServerErrorLogPreservesSuccessfulHTTPS(t *testing.T) {
	t.Parallel()
	core, observed := observer.New(zapcore.ErrorLevel)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ErrorLog = newHTTPServerErrorLog(zap.New(core))
	server.StartTLS()
	defer server.Close()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent || observed.Len() != 0 {
		t.Fatalf("HTTPS response = %d, logs = %v", response.StatusCode, observed.All())
	}
}

type httpErrorLogClock struct {
	now time.Time
}

func (clock *httpErrorLogClock) Now() time.Time { return clock.now }

func (*httpErrorLogClock) NewTicker(interval time.Duration) *time.Ticker {
	return time.NewTicker(interval)
}
