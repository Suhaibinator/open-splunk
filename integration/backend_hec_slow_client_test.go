//go:build !windows

package integration_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	backendHECSlowClientFlag = "OPEN_SPLUNK_HEC_SLOW_CLIENT"

	// The shipped handler admits sixteen concurrent requests for one token. Hold
	// that complete production envelope rather than a smaller test-only limit.
	backendHECSlowClientCount = 16
	backendHECSlowEventBytes  = 512 << 10

	backendHECSlowMinimumHold   = 25 * time.Second
	backendHECSlowMaximumHold   = 40 * time.Second
	backendHECSlowReadBudget    = 45 * time.Second
	backendHECSlowCleanupBudget = 12 * time.Second

	backendHECSlowMaximumHeapMB          = 256
	backendHECSlowMaximumGoroutineGrowth = 32
	backendHECSlowMaximumThreads         = 128
	backendHECSlowRetainedHeapMB         = 32
	backendHECSlowRetainedGoroutines     = 32

	backendHECSlowRuntimeTraceInterval = 2 * time.Second
	backendHECSlowPayloadCanary        = "hec-slow-compressed-payload-private"
	backendHECSlowPrimerPayload        = "hec-slow-client-primer-private"
	backendHECSlowPostPayload          = "hec-slow-client-post-timeout-private"
)

// TestBackendHECSlowCompressedReadDeadline starts the shipped server and holds
// the complete per-token request envelope in gzip decoding over distinct
// chunked HTTP/1.1 TLS connections. The clients deliberately omit the terminal
// HTTP chunk after a valid gzip member containing an incomplete large JSON
// string. Every connection must remain admitted until the production listener's
// fixed 30-second ReadTimeout, then release its handler and connection resources.
//
// This is opt-in because it builds the shipped binary, owns a pinned ClickHouse
// container, and necessarily waits through the real listener deadline.
func TestBackendHECSlowCompressedReadDeadline(t *testing.T) {
	if os.Getenv(backendHECSlowClientFlag) != "1" {
		t.Skip("set " + backendHECSlowClientFlag + "=1 to run the shipped HEC slow-client gate")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", backendHECSlowClientFlag, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	work := t.TempDir()
	buildDir := t.TempDir()
	serverRuntimeDir := t.TempDir()
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)

	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	clickHouse, err := testsupport.StartClickHouseWithServicePrincipals(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := clickHouse.Close(cleanupCtx); err != nil {
			t.Errorf("ClickHouse cleanup: %v", err)
		}
	})

	serverBinary := filepath.Join(buildDir, "open-splunk-server")
	buildBinary(t, ctx, stagedBackendRepository, serverBinary, "./cmd/open-splunk-server")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	httpTLSIdentity, err := testsupport.WriteServerTLSIdentity(
		filepath.Join(work, "http-tls"),
		"127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	administratorTokenPath, administratorToken := provisionAdministratorToken(t, work)
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := clickHouseServerEnvironment(os.Environ(), clickHouse)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"PATH",
		filepath.Join(serverRuntimeDir, "no-external-runtime"),
	)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"GODEBUG",
		"gctrace=1,schedtrace="+
			strconv.FormatInt(backendHECSlowRuntimeTraceInterval.Milliseconds(), 10)+
			",scheddetail=1",
	)
	// A lower collection percentage makes held and post-cleanup heap samples
	// observable without exposing a diagnostic endpoint in the shipped server.
	serverEnvironment = environmentWithValue(serverEnvironment, "GOGC", "20")
	serverArguments := []string{
		serverBinary,
		"-http-listen-address=" + httpAddress,
		"-http-tls-certificate-file=" + httpTLSIdentity.CertificateFile,
		"-http-tls-private-key-file=" + httpTLSIdentity.PrivateKeyFile,
		"-control-database-file=" + filepath.Join(work, "control.sqlite"),
		"-master-key-file=" + filepath.Join(work, "server.key"),
		"-administrator-token-file=" + administratorTokenPath,
		"-collector-grpc-listen-address=" + collectorAddress,
		"-collector-grpc-plaintext-enabled",
		"-tenant-id=" + backendHECTenantID,
		"-hec-enabled=true",
	}
	serverArguments = append(serverArguments, clickHouseServerArguments(clickHouse)...)
	serverProcess := startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)

	baseURL := "https://" + httpAddress
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.ForceAttemptHTTP2 = true
	httpTransport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    httpTLSIdentity.RootCAs,
	}
	httpClient := &http.Client{Transport: httpTransport, Timeout: 10 * time.Second}
	t.Cleanup(httpTransport.CloseIdleConnections)
	waitForHealth(
		t,
		ctx,
		httpClient,
		baseURL,
		serverProcess,
		administratorToken,
		clickHouse.Password,
		clickHouse.MigrationPassword,
		clickHouse.RuntimePassword,
		clickHouse.DeletionPassword,
	)
	backendHECAssertHealth(t, ctx, httpClient, baseURL)
	backendHECAssertAdvertised(t, ctx, httpClient, baseURL)
	createBackendIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		backendHECIndexName,
		"Backend HEC slow compressed client integration",
	)
	plaintextToken, tokenMetadata := backendHECCreateToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
	)
	protectedValues := []string{
		administratorToken,
		plaintextToken,
		tokenMetadata.GetTokenPrefix(),
		clickHouse.Password,
		clickHouse.MigrationPassword,
		clickHouse.RuntimePassword,
		clickHouse.DeletionPassword,
		backendHECJSONChannel,
		backendHECSlowPayloadCanary,
		backendHECSlowPrimerPayload,
		backendHECSlowPostPayload,
	}

	// Warm the durable HEC/ClickHouse path before the baseline. The first insert
	// initializes persistent driver and reconciliation goroutines; comparing an
	// unwarmed baseline with the post-timeout steady state would misclassify
	// those owned backend workers as leaked slow-client handlers.
	primerAck := backendHECIngest(
		t,
		ctx,
		httpClient,
		baseURL+"/services/collector/event",
		plaintextToken,
		backendHECJSONChannel,
		"application/json",
		[]byte(`{"event":"`+backendHECSlowPrimerPayload+`"}`),
	)
	backendHECWaitForAcknowledgment(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		primerAck,
		serverProcess,
	)
	baselineGCStart := len(parseBackendHECSlowGC(serverProcess.Logs()))
	stimulateBackendHECSlowGC(t, ctx, httpClient, baseURL, serverProcess, baselineGCStart)
	baselineSnapshotStart := len(parseBackendHECSlowScheduler(serverProcess.Logs()))
	baseline := waitForBackendHECSlowRuntimeSnapshot(
		t,
		serverProcess,
		baselineSnapshotStart+1,
		10*time.Second,
	)
	baselineGC := backendHECSlowLatestGC(t, serverProcess.Logs())
	baselineGCCount := len(parseBackendHECSlowGC(serverProcess.Logs()))
	baselineSnapshotCount := len(parseBackendHECSlowScheduler(serverProcess.Logs()))

	compressed := backendHECSlowCompressedBody(t)
	results := make(chan backendHECSlowClientResult, backendHECSlowClientCount)
	clients := make([]*backendHECSlowClient, 0, backendHECSlowClientCount)
	for ordinal := range backendHECSlowClientCount {
		client, err := openBackendHECSlowClient(
			ctx,
			httpAddress,
			httpTLSIdentity.RootCAs,
			plaintextToken,
			backendHECJSONChannel,
			compressed,
		)
		if err != nil {
			closeBackendHECSlowClients(clients)
			t.Fatalf("open slow HEC client %d: %v", ordinal, err)
		}
		clients = append(clients, client)
		go func(index int, held *backendHECSlowClient) {
			results <- held.wait(index)
		}(ordinal, client)
	}
	t.Cleanup(func() { closeBackendHECSlowClients(clients) })

	waitForBackendHECSlowBusy(
		t,
		ctx,
		httpAddress,
		httpTLSIdentity.RootCAs,
		plaintextToken,
		backendHECJSONChannel,
		results,
	)
	held := waitForBackendHECSlowRuntimeSnapshot(
		t,
		serverProcess,
		baselineSnapshotCount+1,
		8*time.Second,
	)
	waitForBackendHECSlowGC(
		t,
		ctx,
		httpClient,
		baseURL,
		serverProcess,
		baselineGCCount,
		results,
	)
	heldGC := backendHECSlowLatestGC(t, serverProcess.Logs())

	completed := make([]backendHECSlowClientResult, 0, backendHECSlowClientCount)
	deadline := time.NewTimer(backendHECSlowReadBudget)
	defer deadline.Stop()
	for len(completed) < backendHECSlowClientCount {
		select {
		case result := <-results:
			completed = append(completed, result)
		case <-deadline.C:
			t.Fatalf(
				"only %d of %d slow HEC connections completed within %s",
				len(completed),
				backendHECSlowClientCount,
				backendHECSlowReadBudget,
			)
		case <-ctx.Done():
			t.Fatalf("wait for slow HEC read deadlines: %v", ctx.Err())
		}
	}
	for _, result := range completed {
		if result.duration < backendHECSlowMinimumHold || result.duration > backendHECSlowMaximumHold {
			t.Fatalf(
				"slow HEC client %d completed after %s, want [%s,%s] around the shipped 30s read deadline",
				result.index,
				result.duration.Round(time.Millisecond),
				backendHECSlowMinimumHold,
				backendHECSlowMaximumHold,
			)
		}
		switch {
		case result.status == 0 && result.err != nil:
			// The listener may close the connection when its read and write
			// deadlines expire together.
		case result.status == http.StatusBadRequest && result.code == 6 && result.err == nil:
			// If the bounded protocol error wins that race, it must remain the
			// documented invalid-data response rather than an internal failure.
		default:
			t.Fatalf(
				"slow HEC client %d timeout outcome = status %d code %d read_error=%t",
				result.index,
				result.status,
				result.code,
				result.err != nil,
			)
		}
	}
	closeBackendHECSlowClients(clients)

	postAck := backendHECIngest(
		t,
		ctx,
		httpClient,
		baseURL+"/services/collector/event",
		plaintextToken,
		backendHECJSONChannel,
		"application/json",
		[]byte(`{"event":"`+backendHECSlowPostPayload+`"}`),
	)
	backendHECWaitForAcknowledgment(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		postAck,
		serverProcess,
	)

	postGCStart := len(parseBackendHECSlowGC(serverProcess.Logs()))
	stimulateBackendHECSlowGC(t, ctx, httpClient, baseURL, serverProcess, postGCStart)
	postSnapshotStart := len(parseBackendHECSlowScheduler(serverProcess.Logs()))
	post := waitForBackendHECSlowCleanupSnapshot(
		t,
		serverProcess,
		postSnapshotStart+1,
		baseline.goroutines+backendHECSlowRetainedGoroutines,
		backendHECSlowCleanupBudget,
		protectedValues,
	)
	postGC := backendHECSlowLatestGC(t, serverProcess.Logs())

	var operations opensplunk.GetHECOperationalSnapshotResponse
	postAdministratorProto(
		t,
		ctx,
		httpClient,
		baseURL+"/api/hec/operations/get",
		administratorToken,
		&opensplunk.GetHECOperationalSnapshotRequest{},
		&operations,
	)
	protocolFailures := make(map[uint32]uint64, len(operations.GetProtocolFailures()))
	for _, failure := range operations.GetProtocolFailures() {
		protocolFailures[failure.GetCode()] = failure.GetCount()
	}
	if operations.GetRequest().GetAuthenticationFailures() != 0 ||
		operations.GetRequest().GetAcceptedRequests() != 2 ||
		protocolFailures[6] < backendHECSlowClientCount ||
		protocolFailures[9] == 0 ||
		operations.GetDurable().GetPendingOutboxReservations() != 0 ||
		operations.GetAcknowledgments().GetPendingRows() != 0 {
		t.Fatalf("post-timeout HEC operational snapshot = %+v", &operations)
	}

	allGC := parseBackendHECSlowGC(serverProcess.Logs())
	allScheduler := parseBackendHECSlowScheduler(serverProcess.Logs())
	maximumHeapMB := uint64(0)
	for _, sample := range allGC {
		maximumHeapMB = max(maximumHeapMB, sample.maximumMB)
	}
	maximumGoroutines := uint64(0)
	maximumThreads := uint64(0)
	for _, sample := range allScheduler {
		maximumGoroutines = max(maximumGoroutines, sample.goroutines)
		maximumThreads = max(maximumThreads, sample.threads)
	}
	retainedHeapMB := backendHECSlowRetainedHeapGrowth(postGC, baselineGC)
	if len(allGC) < 3 || len(allScheduler) < 3 ||
		maximumHeapMB > backendHECSlowMaximumHeapMB ||
		maximumGoroutines > baseline.goroutines+backendHECSlowMaximumGoroutineGrowth ||
		maximumThreads > backendHECSlowMaximumThreads ||
		post.goroutines > baseline.goroutines+backendHECSlowRetainedGoroutines ||
		retainedHeapMB > backendHECSlowRetainedHeapMB {
		t.Fatalf(
			"slow HEC runtime bounds exceeded: gc_samples=%d scheduler_samples=%d baseline=%+v held=%+v post=%+v baseline_gc=%+v held_gc=%+v post_gc=%+v max_heap_mb=%d max_goroutines=%d max_threads=%d retained_heap_mb=%d",
			len(allGC),
			len(allScheduler),
			baseline,
			held,
			post,
			baselineGC,
			heldGC,
			postGC,
			maximumHeapMB,
			maximumGoroutines,
			maximumThreads,
			retainedHeapMB,
		)
	}

	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop slow-client HEC server: %v\nlogs:\n%s",
			err,
			redactForFailure(serverProcess.Logs(), protectedValues...),
		)
	}
	assertManagedProcessLogsComplete(t, "slow-client HEC server", serverProcess, protectedValues...)
	assertProcessLogsDoNotLeak(t, serverProcess.Logs(), protectedValues...)

	t.Logf(
		"shipped HEC slow compressed clients: clients=%d minimum_hold=%s maximum_hold=%s baseline_goroutines=%d held_goroutines=%d post_goroutines=%d max_threads=%d max_heap_mb=%d post_live_heap_growth_mb=%d",
		backendHECSlowClientCount,
		minimumBackendHECSlowDuration(completed).Round(time.Millisecond),
		maximumBackendHECSlowDuration(completed).Round(time.Millisecond),
		baseline.goroutines,
		held.goroutines,
		post.goroutines,
		maximumThreads,
		maximumHeapMB,
		retainedHeapMB,
	)
}

func backendHECSlowCompressedBody(t *testing.T) []byte {
	t.Helper()
	var plain strings.Builder
	plain.Grow(backendHECSlowEventBytes)
	plain.WriteString(`{"event":"`)
	plain.WriteString(backendHECSlowPayloadCanary)
	for plain.Len() < backendHECSlowEventBytes {
		plain.WriteByte('x')
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := io.WriteString(writer, plain.String()); err != nil {
		t.Fatalf("compress slow HEC body: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close slow HEC gzip member: %v", err)
	}
	return compressed.Bytes()
}

type backendHECSlowClient struct {
	connection net.Conn
	started    time.Time
}

type backendHECSlowClientResult struct {
	index    int
	duration time.Duration
	status   int
	code     int
	err      error
}

func openBackendHECSlowClient(
	ctx context.Context,
	address string,
	rootCAs *x509.CertPool,
	credential string,
	channel string,
	compressed []byte,
) (*backendHECSlowClient, error) {
	started := time.Now()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial shipped HEC listener: %w", err)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("split shipped HEC listener address: %w", err)
	}
	connection := tls.Client(raw, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
		ServerName: host,
		NextProtos: []string{"http/1.1"},
	})
	handshakeContext, cancelHandshake := context.WithTimeout(ctx, 5*time.Second)
	defer cancelHandshake()
	if err := connection.HandshakeContext(handshakeContext); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("handshake shipped HEC TLS listener: %w", err)
	}
	if connection.ConnectionState().NegotiatedProtocol != "http/1.1" {
		_ = connection.Close()
		return nil, fmt.Errorf(
			"shipped HEC TLS listener negotiated %q, want HTTP/1.1",
			connection.ConnectionState().NegotiatedProtocol,
		)
	}
	if err := connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("bound slow HEC request write: %w", err)
	}
	request := bufio.NewWriter(connection)
	if _, err := fmt.Fprintf(
		request,
		"POST /services/collector/event HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Authorization: Splunk %s\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Encoding: gzip\r\n"+
			"X-Splunk-Request-Channel: %s\r\n"+
			"Transfer-Encoding: chunked\r\n"+
			"Connection: close\r\n\r\n"+
			"%x\r\n",
		address,
		credential,
		channel,
		len(compressed),
	); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("write slow HEC request headers: %w", err)
	}
	if _, err := request.Write(compressed); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("write slow HEC gzip chunk: %w", err)
	}
	if _, err := request.WriteString("\r\n"); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("finish slow HEC gzip chunk: %w", err)
	}
	if err := request.Flush(); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("flush slow HEC gzip chunk: %w", err)
	}
	if err := connection.SetReadDeadline(started.Add(backendHECSlowReadBudget)); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("bound slow HEC response read: %w", err)
	}
	return &backendHECSlowClient{connection: connection, started: started}, nil
}

func closeBackendHECSlowClients(clients []*backendHECSlowClient) {
	for _, client := range clients {
		if client != nil && client.connection != nil {
			_ = client.connection.Close()
		}
	}
}

func (client *backendHECSlowClient) wait(index int) backendHECSlowClientResult {
	result := backendHECSlowClientResult{index: index}
	response, err := http.ReadResponse(
		bufio.NewReader(client.connection),
		&http.Request{Method: http.MethodPost},
	)
	if err != nil {
		result.duration = time.Since(client.started)
		result.err = err
		return result
	}
	result.status = response.StatusCode
	wire, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	result.duration = time.Since(client.started)
	if readErr != nil {
		result.err = readErr
		return result
	}
	var decoded backendHECResponse
	if len(wire) <= 1<<20 && json.Unmarshal(wire, &decoded) == nil {
		result.code = decoded.Code
	}
	return result
}

func waitForBackendHECSlowBusy(
	t *testing.T,
	ctx context.Context,
	address string,
	rootCAs *x509.CertPool,
	credential string,
	channel string,
	results <-chan backendHECSlowClientResult,
) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, code, err := backendHECSlowBusyProbe(
			ctx,
			address,
			rootCAs,
			credential,
			channel,
		)
		if err == nil && status == http.StatusServiceUnavailable && code == 9 {
			return
		}
		select {
		case result := <-results:
			t.Fatalf(
				"slow HEC client %d completed before the production envelope was held after %s",
				result.index,
				result.duration.Round(time.Millisecond),
			)
		case <-deadline.C:
			t.Fatalf(
				"shipped HEC handler did not expose its held per-token envelope: last probe status/code/error = %d/%d/%v",
				status,
				code,
				err,
			)
		case <-ticker.C:
		}
	}
}

func backendHECSlowBusyProbe(
	ctx context.Context,
	address string,
	rootCAs *x509.CertPool,
	credential string,
	channel string,
) (int, int, error) {
	dialContext, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return 0, 0, fmt.Errorf("split shipped HEC listener address: %w", err)
	}
	dialer := &tls.Dialer{Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
		ServerName: host,
		NextProtos: []string{"http/1.1"},
	}}
	connection, err := dialer.DialContext(dialContext, "tcp", address)
	if err != nil {
		return 0, 0, err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return 0, 0, err
	}
	request := bufio.NewWriter(connection)
	if _, err := fmt.Fprintf(
		request,
		"POST /services/collector/event HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Authorization: Splunk %s\r\n"+
			"Content-Type: application/json\r\n"+
			"X-Splunk-Request-Channel: %s\r\n"+
			"Content-Length: 1\r\n"+
			"Expect: 100-continue\r\n"+
			"Connection: close\r\n\r\n",
		address,
		credential,
		channel,
	); err != nil {
		return 0, 0, err
	}
	if err := request.Flush(); err != nil {
		return 0, 0, err
	}
	response, err := http.ReadResponse(
		bufio.NewReader(connection),
		&http.Request{Method: http.MethodPost},
	)
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()
	wire, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return response.StatusCode, 0, err
	}
	var decoded backendHECResponse
	if len(wire) > 1<<20 || json.Unmarshal(wire, &decoded) != nil {
		return response.StatusCode, 0, nil
	}
	return response.StatusCode, decoded.Code, nil
}

type backendHECSlowSchedulerSnapshot struct {
	goroutines uint64
	threads    uint64
}

func backendHECSlowRetainedHeapGrowth(
	post backendHECSlowGCSample,
	baseline backendHECSlowGCSample,
) uint64 {
	if post.liveMB <= baseline.liveMB {
		return 0
	}
	return post.liveMB - baseline.liveMB
}

type backendHECSlowGCSample struct {
	maximumMB uint64
	liveMB    uint64
}

var backendHECSlowGCPattern = regexp.MustCompile(
	` ([0-9]+)->([0-9]+)->([0-9]+) MB, [0-9]+ MB goal`,
)

var backendHECSlowThreadsPattern = regexp.MustCompile(`(?:^| )threads=([0-9]+)(?: |$)`)
var backendHECSlowGoroutinePattern = regexp.MustCompile(`^\s+G[0-9]+:`)

func parseBackendHECSlowGC(logs string) []backendHECSlowGCSample {
	var result []backendHECSlowGCSample
	for line := range strings.SplitSeq(logs, "\n") {
		match := backendHECSlowGCPattern.FindStringSubmatch(line)
		if len(match) != 4 {
			continue
		}
		before, beforeErr := strconv.ParseUint(match[1], 10, 64)
		peak, peakErr := strconv.ParseUint(match[2], 10, 64)
		live, liveErr := strconv.ParseUint(match[3], 10, 64)
		if beforeErr != nil || peakErr != nil || liveErr != nil {
			continue
		}
		result = append(result, backendHECSlowGCSample{
			maximumMB: max(before, max(peak, live)),
			liveMB:    live,
		})
	}
	return result
}

func parseBackendHECSlowScheduler(logs string) []backendHECSlowSchedulerSnapshot {
	lines := strings.Split(logs, "\n")
	var result []backendHECSlowSchedulerSnapshot
	var current backendHECSlowSchedulerSnapshot
	active := false
	finish := func() {
		if active && current.threads != 0 && current.goroutines != 0 {
			result = append(result, current)
		}
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "SCHED ") {
			finish()
			active = true
			current = backendHECSlowSchedulerSnapshot{}
			if match := backendHECSlowThreadsPattern.FindStringSubmatch(line); len(match) == 2 {
				current.threads, _ = strconv.ParseUint(match[1], 10, 64)
			}
			continue
		}
		if active && backendHECSlowGoroutinePattern.MatchString(line) &&
			!strings.Contains(line, "status=6") {
			current.goroutines++
		}
	}
	// The last sample may be an in-progress buffer write. A following SCHED line
	// is the proof that every detailed G record in a sample was captured.
	return result
}

func TestParseBackendHECSlowRuntimeTrace(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		"gc 1 @0.1s 1%: 0.1+0.2+0.1 ms clock, 1+0/1/0+1 ms cpu, 4->7->3 MB, 8 MB goal, 1 MB stacks, 0 MB globals, 8 P",
		"SCHED 2000ms: gomaxprocs=8 idleprocs=7 threads=12 spinningthreads=0 needspinning=0 idlethreads=6 runqueue=0 gcwaiting=false",
		"  G1: status=4(chan receive) m=nil lockedm=nil",
		"  G2: status=4(select) m=nil lockedm=nil",
		"  G3: status=6() m=nil lockedm=nil",
		"gc 2 @2.1s 1%: 0.1+0.2+0.1 ms clock, 1+0/1/0+1 ms cpu, 6->9->4 MB, 10 MB goal, 1 MB stacks, 0 MB globals, 8 P",
		"SCHED 4000ms: gomaxprocs=8 idleprocs=7 threads=13 spinningthreads=0 needspinning=0 idlethreads=6 runqueue=0 gcwaiting=false",
		"  G1: status=4(chan receive) m=nil lockedm=nil",
	}, "\n")
	gc := parseBackendHECSlowGC(logs)
	if len(gc) != 2 || gc[0] != (backendHECSlowGCSample{maximumMB: 7, liveMB: 3}) ||
		gc[1] != (backendHECSlowGCSample{maximumMB: 9, liveMB: 4}) {
		t.Fatalf("parsed slow-client GC samples = %+v", gc)
	}
	scheduler := parseBackendHECSlowScheduler(logs)
	if len(scheduler) != 1 ||
		scheduler[0] != (backendHECSlowSchedulerSnapshot{goroutines: 2, threads: 12}) {
		t.Fatalf("parsed complete slow-client scheduler samples = %+v", scheduler)
	}
	if growth := backendHECSlowRetainedHeapGrowth(gc[1], gc[0]); growth != 1 {
		t.Fatalf("retained slow-client heap growth = %d MB, want 1", growth)
	}
}

func waitForBackendHECSlowRuntimeSnapshot(
	t *testing.T,
	process *managedProcess,
	after int,
	timeout time.Duration,
) backendHECSlowSchedulerSnapshot {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshots := parseBackendHECSlowScheduler(process.Logs())
		if len(snapshots) > after {
			return snapshots[len(snapshots)-1]
		}
		if process.Exited() {
			t.Fatalf("server exited before runtime scheduler sample: %v", process.Err())
		}
		select {
		case <-deadline.C:
			t.Fatalf("no complete server scheduler sample arrived within %s", timeout)
		case <-ticker.C:
		}
	}
}

func waitForBackendHECSlowCleanupSnapshot(
	t *testing.T,
	process *managedProcess,
	after int,
	maximumGoroutines uint64,
	timeout time.Duration,
	protectedValues []string,
) backendHECSlowSchedulerSnapshot {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var latest backendHECSlowSchedulerSnapshot
	seen := after
	for {
		snapshots := parseBackendHECSlowScheduler(process.Logs())
		if len(snapshots) > seen {
			latest = snapshots[len(snapshots)-1]
			seen = len(snapshots)
			if latest.goroutines <= maximumGoroutines {
				return latest
			}
		}
		if process.Exited() {
			t.Fatalf("server exited before slow-client goroutines settled: %v", process.Err())
		}
		select {
		case <-deadline.C:
			// The gate has already failed. Ask the isolated test-owned process for
			// a goroutine dump and project only symbol names into the diagnostic;
			// stack arguments, application logs, credentials, and payloads never
			// cross the test boundary.
			_ = process.command.Process.Signal(syscall.SIGQUIT)
			_ = process.Wait(5 * time.Second)
			stackSummary := summarizeBackendHECSlowStacks(process.Logs())
			for _, protected := range protectedValues {
				if protected != "" && strings.Contains(stackSummary, protected) {
					t.Fatal("slow-client stack summary contained a protected value")
				}
			}
			t.Fatalf(
				"slow-client goroutines did not settle to at most %d within %s; latest=%+v; stack_symbols=%s",
				maximumGoroutines,
				timeout,
				latest,
				stackSummary,
			)
		case <-ticker.C:
		}
	}
}

func summarizeBackendHECSlowStacks(logs string) string {
	counts := make(map[string]int)
	var symbols []string
	active := false
	finish := func() {
		if !active || len(symbols) == 0 {
			return
		}
		counts[strings.Join(symbols, " <- ")]++
	}
	for line := range strings.SplitSeq(logs, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "goroutine ") {
			finish()
			active = true
			symbols = symbols[:0]
			continue
		}
		if !active || trimmed == "" || strings.HasPrefix(trimmed, "created by ") ||
			strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, "runtime.") ||
			strings.HasPrefix(trimmed, "[originating from goroutine ") ||
			strings.HasPrefix(trimmed, "gp=") || strings.HasPrefix(trimmed, "r1 ") {
			continue
		}
		open := strings.IndexByte(trimmed, '(')
		if open <= 0 || strings.Contains(trimmed[:open], " ") {
			continue
		}
		symbols = append(symbols, trimmed[:open])
		if len(symbols) == 8 {
			finish()
			active = false
		}
	}
	finish()
	type entry struct {
		signature string
		count     int
	}
	entries := make([]entry, 0, len(counts))
	for signature, count := range counts {
		entries = append(entries, entry{signature: signature, count: count})
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].count != entries[right].count {
			return entries[left].count > entries[right].count
		}
		return entries[left].signature < entries[right].signature
	})
	var summary strings.Builder
	for index, item := range entries[:min(len(entries), 12)] {
		if index != 0 {
			summary.WriteString("; ")
		}
		_, _ = fmt.Fprintf(&summary, "%dx[%s]", item.count, item.signature)
	}
	if summary.Len() == 0 {
		return "unavailable"
	}
	return summary.String()
}

func backendHECSlowLatestGC(t *testing.T, logs string) backendHECSlowGCSample {
	t.Helper()
	samples := parseBackendHECSlowGC(logs)
	if len(samples) == 0 {
		t.Fatal("server runtime emitted no GC heap sample")
	}
	return samples[len(samples)-1]
}

func waitForBackendHECSlowGC(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	process *managedProcess,
	after int,
	results <-chan backendHECSlowClientResult,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(parseBackendHECSlowGC(process.Logs())) > after {
			return
		}
		select {
		case result := <-results:
			t.Fatalf("slow HEC client %d completed before a held heap sample", result.index)
		default:
		}
		backendHECSlowHealthRequest(t, ctx, client, baseURL)
	}
	t.Fatal("server runtime emitted no GC heap sample while slow clients were held")
}

func stimulateBackendHECSlowGC(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	process *managedProcess,
	after int,
) {
	t.Helper()
	for range 2_048 {
		backendHECSlowHealthRequest(t, ctx, client, baseURL)
		if len(parseBackendHECSlowGC(process.Logs())) > after {
			return
		}
	}
	t.Fatal("server runtime emitted no post-cleanup GC heap sample")
}

func backendHECSlowHealthRequest(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/services/collector/health",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("stimulate HEC runtime GC: %v", err)
	}
	wire, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<10))
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || len(wire) == 0 {
		t.Fatalf("HEC health stimulus = status %d read error %v", response.StatusCode, readErr)
	}
}

func minimumBackendHECSlowDuration(results []backendHECSlowClientResult) time.Duration {
	minimum := backendHECSlowReadBudget
	for _, result := range results {
		minimum = min(minimum, result.duration)
	}
	return minimum
}

func maximumBackendHECSlowDuration(results []backendHECSlowClientResult) time.Duration {
	var maximum time.Duration
	for _, result := range results {
		maximum = max(maximum, result.duration)
	}
	return maximum
}
