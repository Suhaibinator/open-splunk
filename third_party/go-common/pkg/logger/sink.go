package logger

import (
	"bytes"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// OpenSink opens the custom sink selected by the startup view. It returns a nil
// sink when the view writes to its output paths instead (stdout, none, or an
// unknown sink type). The caller owns the sink and closes it after the last write.
func OpenSink(startup config.LoggingView) (zap.Sink, error) {
	switch startup.LogSink() {
	case config.SinkTypeFile:
		// Writes logs to a file with rotation using lumberjack.
		return openFileSink(startup.LogSinkConfig())
	case config.SinkTypeGraylog:
		// Sends logs to a Graylog server.
		return openGraylogSink(startup.LogSinkConfig())
	case config.SinkTypeGrafanaAlloy:
		// Sends logs to Grafana Alloy.
		return openGrafanaAlloySink(startup.LogSinkConfig())
	case config.SinkTypeGrafanaLoki:
		// Sends logs to Grafana Loki.
		return openGrafanaLokiSink(startup.LogSinkConfig())
	default:
		return nil, nil
	}
}

func openFileSink(sinkConfig map[string]string) (zap.Sink, error) {
	filePath, ok := sinkConfig["path"]
	if !ok {
		return nil, errors.New("file sink requires 'path' configuration")
	}

	// Get optional configuration parameters with defaults for lumberjack.
	maxSize := 100 // default 100MB
	if val, ok := sinkConfig["maxSize"]; ok {
		_, err := fmt.Sscanf(val, "%d", &maxSize)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile,
				KeySetting, "maxSize",
				KeyValue, val,
				logkeys.Fallback, maxSize,
				logkeys.Error, err)
		}
	}

	maxBackups := 3 // default 3 backups
	if val, ok := sinkConfig["maxBackups"]; ok {
		_, err := fmt.Sscanf(val, "%d", &maxBackups)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile, KeySetting, "maxBackups", KeyValue, val,
				logkeys.Fallback, maxBackups, logkeys.Error, err)
		}
	}

	maxAge := 28 // default 28 days
	if val, ok := sinkConfig["maxAge"]; ok {
		_, err := fmt.Sscanf(val, "%d", &maxAge)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile, KeySetting, "maxAge", KeyValue, val,
				logkeys.Fallback, maxAge, logkeys.Error, err)
		}
	}

	compress := true // default to compress old log files.
	if val, ok := sinkConfig["compress"]; ok {
		_, err := fmt.Sscanf(val, "%t", &compress)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile, KeySetting, "compress", KeyValue, val,
				logkeys.Fallback, compress, logkeys.Error, err)
		}
	}

	// Get flush interval configuration with default for batched I/O.
	flushIntervalSec := 5 // default 5 seconds
	if val, ok := sinkConfig["flushInterval"]; ok {
		_, err := fmt.Sscanf(val, "%d", &flushIntervalSec)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile, KeySetting, "flushInterval", KeyValue, val,
				logkeys.Fallback, flushIntervalSec, logkeys.Error, err)
		}
	}
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	// Get buffer size configuration with default for batched I/O.
	bufferSize := 256 * 1024 // default 256KB
	if val, ok := sinkConfig["bufferSize"]; ok {
		_, err := fmt.Sscanf(val, "%d", &bufferSize)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeFile, KeySetting, "bufferSize", KeyValue, val,
				logkeys.Fallback, bufferSize, logkeys.Error, err)
		}
	}

	// Create a lumberjack logger instance for managing log file rotation.
	ljLogger := &lumberjack.Logger{
		Filename:   filePath,   // Path to the log file.
		MaxSize:    maxSize,    // Maximum size in megabytes of the log file before it's rotated.
		MaxBackups: maxBackups, // Maximum number of old log files to retain.
		MaxAge:     maxAge,     // Maximum number of days to retain old log files.
		Compress:   compress,   // Whether to compress old log files.
	}

	// Wrap the lumberjack logger in a LumberjackSink, which provides batched I/O.
	return NewLumberjackSink(ljLogger, flushInterval, bufferSize), nil
}

func openGraylogSink(sinkConfig map[string]string) (zap.Sink, error) {
	host, ok := sinkConfig["host"]
	if !ok {
		return nil, errors.New("graylog sink requires 'host' configuration")
	}

	port, ok := sinkConfig["port"]
	if !ok {
		return nil, errors.New("graylog sink requires 'port' configuration")
	}

	// Establish a UDP connection to the Graylog server.
	graylogAddr := net.JoinHostPort(host, port)
	conn, err := net.Dial("udp", graylogAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Graylog: %w", err)
	}

	fmt.Printf("Graylog sink configured with host=%s, port=%s\n", host, port)
	return &GraylogSink{conn: conn}, nil
}

// GraylogSink writes encoded log entries to a Graylog connection.
type GraylogSink struct {
	conn net.Conn // The network connection to the Graylog server.
}

func (s *GraylogSink) Write(p []byte) (n int, err error) {
	return s.conn.Write(p)
}

func (s *GraylogSink) Sync() error {
	return nil
}

func (s *GraylogSink) Close() error {
	return s.conn.Close()
}

// Ensure that GraylogSink correctly implements the zap.Sink interface.
var _ zap.Sink = (*GraylogSink)(nil)

// LumberjackSink buffers writes to a rotating file sink.
type LumberjackSink struct {
	Logger       *lumberjack.Logger // The underlying lumberjack logger for file rotation.
	buffer       *bytes.Buffer      // A buffer to accumulate log entries before flushing.
	bufferSize   int                // The maximum size of the buffer in bytes.
	mutex        sync.Mutex         // Mutex to protect concurrent access to the buffer.
	ticker       *time.Ticker       // Ticker for periodic flushing of the buffer.
	done         chan bool          // Channel to signal the periodic flusher to stop.
	flushes      atomic.Int64       // Atomic counter for the number of times the buffer has been flushed.
	bytesWritten atomic.Int64       // Atomic counter for the total number of bytes written to the sink.
}

func NewLumberjackSink(logger *lumberjack.Logger, flushInterval time.Duration, bufferSize int) *LumberjackSink {
	sink := &LumberjackSink{
		Logger:     logger,
		buffer:     &bytes.Buffer{},
		bufferSize: bufferSize,
		done:       make(chan bool),
	}

	// Start a goroutine that periodically flushes the buffer to disk.
	sink.ticker = time.NewTicker(flushInterval)
	go sink.flushPeriodically()

	return sink
}

func (s *LumberjackSink) flushPeriodically() {
	for {
		select {
		case <-s.ticker.C: // Triggered by the periodic ticker.
			if err := s.Sync(); err != nil {
				slog.Error("Failed to flush log sink",
					KeySink, config.SinkTypeFile,
					logkeys.Operation, "periodic_sync",
					logkeys.Error, err)
			}
		case <-s.done: // Signal to stop the goroutine.
			s.ticker.Stop()
			return
		}
	}
}

func (s *LumberjackSink) Write(p []byte) (n int, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Write the log entry to the buffer.
	n, err = s.buffer.Write(p)
	if err != nil {
		return n, err
	}

	// Atomically update the total bytes written counter.
	s.bytesWritten.Add(int64(n))

	// If the buffer's current length exceeds its capacity, flush it.
	if s.buffer.Len() >= s.bufferSize {
		err = s.flushLocked()
	}

	return n, err
}

func (s *LumberjackSink) flushLocked() error {
	// If the buffer is empty, there's nothing to flush.
	if s.buffer.Len() == 0 {
		return nil
	}

	// Write the buffered bytes to the lumberjack logger.
	_, err := s.Logger.Write(s.buffer.Bytes())
	if err != nil {
		return err
	}

	// Reset the buffer after successful write.
	s.buffer.Reset()

	// Atomically increment the flush counter.
	s.flushes.Add(1)

	return nil
}

func (s *LumberjackSink) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.flushLocked()
}

func (s *LumberjackSink) Close() error {
	// Signal the periodic flush goroutine to terminate.
	s.done <- true

	// Perform a final flush to ensure all buffered logs are written.
	if err := s.Sync(); err != nil {
		return err
	}

	// Close the underlying lumberjack logger, which handles file closing.
	return s.Logger.Close()
}

func (s *LumberjackSink) GetStats() (flushes int64, bytesWritten int64) {
	return s.flushes.Load(), s.bytesWritten.Load()
}

// Ensure that LumberjackSink correctly implements the zap.Sink interface.
var _ zap.Sink = (*LumberjackSink)(nil)

func openGrafanaAlloySink(sinkConfig map[string]string) (zap.Sink, error) {
	endpoint, ok := sinkConfig["alloy_endpoint"]
	if !ok {
		return nil, errors.New("grafana_alloy sink requires 'alloy_endpoint' configuration")
	}

	apiKey, ok := sinkConfig["alloy_apiKey"]
	if !ok {
		return nil, errors.New("grafana_alloy sink requires 'alloy_apiKey' configuration")
	}

	// Get optional configuration parameters with defaults.
	maxBufferSize := 1000 // default 1000 logs per batch
	if val, ok := sinkConfig["alloy_maxBufferSize"]; ok {
		_, err := fmt.Sscanf(val, "%d", &maxBufferSize)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaAlloy, KeySetting, "alloy_maxBufferSize", KeyValue, val,
				logkeys.Fallback, maxBufferSize, logkeys.Error, err)
		}
	}

	flushIntervalSec := 5 // default 5 seconds
	if val, ok := sinkConfig["alloy_flushInterval"]; ok {
		_, err := fmt.Sscanf(val, "%d", &flushIntervalSec)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaAlloy, KeySetting, "alloy_flushInterval", KeyValue, val,
				logkeys.Fallback, flushIntervalSec, logkeys.Error, err)
		}
	}
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	workerCount := 2 // default 2 workers
	if val, ok := sinkConfig["alloy_workerCount"]; ok {
		_, err := fmt.Sscanf(val, "%d", &workerCount)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaAlloy, KeySetting, "alloy_workerCount", KeyValue, val,
				logkeys.Fallback, workerCount, logkeys.Error, err)
		}
	}

	fmt.Printf("Grafana Alloy sink configured with endpoint=%s\n", endpoint)

	// Create a custom GrafanaAlloySink with buffering and retry logic.
	sender := NewGrafanaAlloySender(endpoint, apiKey)
	return &GrafanaAlloySink{
		BufferedHTTPSink: NewBufferedHTTPSink(
			sender,
			maxBufferSize,
			flushInterval,
			3,           // Max retries
			time.Second, // Initial retry delay
			workerCount,
		),
	}, nil
}

func openGrafanaLokiSink(sinkConfig map[string]string) (zap.Sink, error) {
	endpoint, ok := sinkConfig["loki_endpoint"]
	if !ok {
		return nil, errors.New("grafana_loki sink requires 'loki_endpoint' configuration")
	}

	// Get optional authentication and tenant configuration parameters.
	username := ""
	if val, ok := sinkConfig["loki_username"]; ok {
		username = val
	}

	password := ""
	if val, ok := sinkConfig["loki_password"]; ok {
		password = val
	}

	tenant := ""
	if val, ok := sinkConfig["loki_tenant"]; ok {
		tenant = val
	}

	// Get optional buffering and worker configuration parameters with defaults.
	maxBufferSize := 1000 // default 1000 logs per batch
	if val, ok := sinkConfig["loki_maxBufferSize"]; ok {
		_, err := fmt.Sscanf(val, "%d", &maxBufferSize)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaLoki, KeySetting, "loki_maxBufferSize", KeyValue, val,
				logkeys.Fallback, maxBufferSize, logkeys.Error, err)
		}
	}

	flushIntervalSec := 5 // default 5 seconds
	if val, ok := sinkConfig["loki_flushInterval"]; ok {
		_, err := fmt.Sscanf(val, "%d", &flushIntervalSec)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaLoki, KeySetting, "loki_flushInterval", KeyValue, val,
				logkeys.Fallback, flushIntervalSec, logkeys.Error, err)
		}
	}
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	workerCount := 2 // default 2 workers
	if val, ok := sinkConfig["loki_workerCount"]; ok {
		_, err := fmt.Sscanf(val, "%d", &workerCount)
		if err != nil {
			slog.Warn("Invalid logging sink configuration; using default",
				KeySink, config.SinkTypeGrafanaLoki, KeySetting, "loki_workerCount", KeyValue, val,
				logkeys.Fallback, workerCount, logkeys.Error, err)
		}
	}

	fmt.Printf("Grafana Loki sink configured with endpoint=%s\n", endpoint)

	// Create a custom GrafanaLokiSink with buffering and retry logic.
	sender := NewGrafanaLokiSender(endpoint, username, password, tenant)
	return &GrafanaLokiSink{
		BufferedHTTPSink: NewBufferedHTTPSink(
			sender,
			maxBufferSize,
			flushInterval,
			3,           // Max retries
			time.Second, // Initial retry delay
			workerCount,
		),
	}, nil
}

// LogEntry is the structured payload sent by an HTTP log sink.
type LogEntry struct {
	Timestamp string         `json:"timestamp"`        // The timestamp of the log entry in ISO8601 format.
	Message   string         `json:"message"`          // The main log message.
	Level     string         `json:"level"`            // The log level (e.g., "info", logkeys.Error, "debug").
	Fields    map[string]any `json:"fields,omitempty"` // Additional structured fields associated with the log entry.
}

// HTTPLogSender defines the interface for sending a batch of structured log entries
// to an HTTP endpoint. Implementations handle the specific HTTP request details
// for different logging services.
type HTTPLogSender interface {
	// SendLogs attempts to send a slice of LogEntry to the configured HTTP endpoint.
	// It returns an error if the sending process fails.
	SendLogs(logs []LogEntry) error
}

// BufferedHTTPSink batches log entries for delivery by an HTTPLogSender.
type BufferedHTTPSink struct {
	sender        HTTPLogSender
	maxBufferSize int
	flushInterval time.Duration
	maxRetries    int
	retryDelay    time.Duration

	// Channel-based worker pool
	logChan     chan []byte
	workerCount int
	pendingLogs sync.WaitGroup
	workerWG    sync.WaitGroup
	writeLock   sync.RWMutex
	closed      bool
	closeOnce   sync.Once
	closeErr    error

	// Periodic flushing
	ticker    *time.Ticker
	done      chan struct{}
	flusherWG sync.WaitGroup

	// Buffer management using circular buffer pattern
	buffer       []LogEntry
	bufferStart  int // Index of the first element
	bufferSize   int // Current number of elements
	bufferLock   sync.Mutex
	flushLock    sync.Mutex // Serializes remote sends and makes Sync a full barrier.
	flushPending int32      // Atomic flag to prevent multiple concurrent flushes
	flushWG      sync.WaitGroup

	// Metrics
	sendAttempts  atomic.Int64
	sendSuccesses atomic.Int64
	sendFailures  atomic.Int64
	droppedLogs   atomic.Int64
}

func NewBufferedHTTPSink(
	sender HTTPLogSender,
	maxBufferSize int,
	flushInterval time.Duration,
	maxRetries int,
	retryDelay time.Duration,
	workerCount int,
) *BufferedHTTPSink {
	if workerCount <= 0 {
		workerCount = 2 // Default to 2 workers
	}

	sink := &BufferedHTTPSink{
		sender:        sender,
		maxBufferSize: maxBufferSize,
		flushInterval: flushInterval,
		maxRetries:    maxRetries,
		retryDelay:    retryDelay,
		logChan:       make(chan []byte, 1000), // Buffer up to 1000 log entries
		workerCount:   workerCount,
		buffer:        make([]LogEntry, maxBufferSize), // Pre-allocate buffer with max size
		bufferStart:   0,
		bufferSize:    0,
		done:          make(chan struct{}),
		flushPending:  0,
	}

	// Start workers
	sink.workerWG.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go sink.worker()
	}

	// Start periodic flusher
	sink.ticker = time.NewTicker(flushInterval)
	sink.flusherWG.Add(1)
	go sink.flushPeriodically()

	return sink
}

func (s *BufferedHTTPSink) worker() {
	defer s.workerWG.Done()
	for logData := range s.logChan {
		// Parse the log entry
		var entry LogEntry
		entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
		entry.Message = string(logData)

		// Try to parse JSON fields if the log is in JSON format
		var jsonFields map[string]any
		if err := json.Unmarshal(logData, &jsonFields); err == nil {
			if msg, ok := jsonFields["msg"].(string); ok {
				entry.Message = msg
				delete(jsonFields, "msg")
			}
			if lvl, ok := jsonFields["level"].(string); ok {
				entry.Level = lvl
				delete(jsonFields, "level")
			}
			entry.Fields = jsonFields
		}

		// Add to the buffer before marking the channel item complete. Sync blocks
		// new writes and waits on pendingLogs, so completion here makes it a true
		// barrier for every entry accepted before Sync began.
		bufferSize := s.addToBuffer(entry)
		s.pendingLogs.Done()

		// If buffer is getting full, trigger a flush
		flushThreshold := max(s.maxBufferSize/2, 1)
		if bufferSize >= flushThreshold && atomic.CompareAndSwapInt32(&s.flushPending, 0, 1) {
			s.flushWG.Go(func() {
				if err := s.flushBuffer(); err != nil {
					slog.Error("Failed to flush buffered HTTP log sink",
						logkeys.Operation, "threshold_flush",
						logkeys.Error, err)
				}
				atomic.StoreInt32(&s.flushPending, 0)
			})
		}
	}
}

func (s *BufferedHTTPSink) addToBuffer(entry LogEntry) int {
	s.bufferLock.Lock()
	defer s.bufferLock.Unlock()

	if s.bufferSize < s.maxBufferSize {
		// Buffer has space, add the entry at the end
		index := (s.bufferStart + s.bufferSize) % s.maxBufferSize
		s.buffer[index] = entry
		s.bufferSize++
	} else {
		// Buffer is full, overwrite the oldest entry
		s.buffer[s.bufferStart] = entry
		s.bufferStart = (s.bufferStart + 1) % s.maxBufferSize
		s.droppedLogs.Add(1)
	}

	return s.bufferSize
}

func (s *BufferedHTTPSink) flushPeriodically() {
	defer s.flusherWG.Done()
	for {
		select {
		case <-s.ticker.C:
			if err := s.Sync(); err != nil {
				slog.Error("Failed to flush buffered HTTP log sink",
					logkeys.Operation, "periodic_sync",
					logkeys.Error, err)
			}
		case <-s.done:
			s.ticker.Stop()
			return
		}
	}
}

func (s *BufferedHTTPSink) flushBuffer() error {
	// A threshold flush, periodic Sync, explicit Sync, and Close can all arrive
	// concurrently. Holding flushLock through SendLogs ensures an explicit Sync
	// also waits for a batch already removed from the circular buffer.
	s.flushLock.Lock()
	defer s.flushLock.Unlock()

	s.bufferLock.Lock()
	if s.bufferSize == 0 {
		s.bufferLock.Unlock()
		return nil
	}

	// Copy logs from circular buffer
	logs := make([]LogEntry, s.bufferSize)
	for i := 0; i < s.bufferSize; i++ {
		index := (s.bufferStart + i) % s.maxBufferSize
		logs[i] = s.buffer[index]
	}

	// Reset buffer
	s.bufferStart = 0
	s.bufferSize = 0
	s.bufferLock.Unlock()

	// Send logs with retries
	var err error
	delay := s.retryDelay

	for i := 0; i <= s.maxRetries; i++ {
		s.sendAttempts.Add(1)
		err = s.sender.SendLogs(logs)
		if err == nil {
			s.sendSuccesses.Add(1)
			return nil
		}

		// If this was the last retry, give up
		if i == s.maxRetries {
			s.sendFailures.Add(1)
			slog.Error("Failed to send log batch",
				logkeys.Operation, "send_log_batch",
				KeyAttemptCount, s.maxRetries+1,
				logkeys.Error, err)
			return fmt.Errorf("failed to send logs after %d retries: %w", s.maxRetries, err)
		}

		// Wait before retrying with exponential backoff
		time.Sleep(delay)
		delay *= 2
	}

	return err
}

func (s *BufferedHTTPSink) Write(p []byte) (n int, err error) {
	// Close takes the exclusive side of this lock before closing logChan. That
	// makes the closed check and send atomic with respect to channel shutdown and
	// prevents a concurrent Write from panicking on a closed channel.
	s.writeLock.RLock()
	defer s.writeLock.RUnlock()
	if s.closed {
		return 0, errors.New("buffered HTTP log sink is closed")
	}

	// Copy the data since p might be reused
	logData := make([]byte, len(p))
	copy(logData, p)

	// Send to channel (non-blocking)
	s.pendingLogs.Add(1)
	select {
	case s.logChan <- logData:
		// Successfully queued
	default:
		s.pendingLogs.Done()
		// Channel is full, increment dropped logs counter
		s.droppedLogs.Add(1)
	}

	return len(p), nil
}

func (s *BufferedHTTPSink) Sync() error {
	// Excluding writers gives pendingLogs a stable boundary: every accepted log
	// before this lock was acquired must reach the circular buffer before the
	// final flush. Writes that begin afterward belong to the next Sync.
	s.writeLock.Lock()
	s.pendingLogs.Wait()
	s.writeLock.Unlock()
	return s.flushBuffer()
}

func (s *BufferedHTTPSink) Close() error {
	s.closeOnce.Do(func() {
		// Stop intake and close the work queue while holding the exclusive lock so
		// no writer can race the channel close.
		s.writeLock.Lock()
		s.closed = true
		close(s.logChan)
		s.writeLock.Unlock()

		// Stop the periodic flusher, then wait for workers to drain every accepted
		// channel item and for all background flushes to finish. The final flush is
		// therefore the last remote send and includes everything still buffered.
		close(s.done)
		s.workerWG.Wait()
		s.flusherWG.Wait()
		s.flushWG.Wait()
		s.closeErr = s.flushBuffer()
	})
	return s.closeErr
}

func (s *BufferedHTTPSink) GetStats() (attempts int64, successes int64, failures int64, dropped int64) {
	return s.sendAttempts.Load(),
		s.sendSuccesses.Load(),
		s.sendFailures.Load(),
		s.droppedLogs.Load()
}

// Ensure BufferedHTTPSink implements zapcore.WriteSyncer
var _ zapcore.WriteSyncer = (*BufferedHTTPSink)(nil)

// GrafanaAlloySender sends structured log batches to Grafana Alloy.
type GrafanaAlloySender struct {
	endpoint string
	apiKey   string
	client   *http.Client
}

func NewGrafanaAlloySender(endpoint, apiKey string) *GrafanaAlloySender {
	return &GrafanaAlloySender{
		endpoint: endpoint,
		apiKey:   apiKey,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *GrafanaAlloySender) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// Convert logs to JSON
	payload, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	// Create request
	req, err := http.NewRequest("POST", s.endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	// Send request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send logs: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send logs, status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GrafanaLokiSender sends structured log batches to Grafana Loki.
type GrafanaLokiSender struct {
	endpoint string
	username string
	password string
	tenant   string
	client   *http.Client
}

func NewGrafanaLokiSender(endpoint, username, password, tenant string) *GrafanaLokiSender {
	return &GrafanaLokiSender{
		endpoint: endpoint,
		username: username,
		password: password,
		tenant:   tenant,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *GrafanaLokiSender) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// Format logs for Loki
	// This is a simplified example - in a real implementation,
	// you would format the logs according to Loki's API
	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values [][]string        `json:"values"`
	}

	// lokiPush defines the top-level payload sent to Loki during buffered HTTP log delivery.
	// It wraps the grouped streams produced in this sender flow before JSON encoding and transmission.
	type lokiPush struct {
		Streams []lokiStream `json:"streams"`
	}

	// Group logs by level
	streamsByLevel := make(map[string][]string)
	for _, log := range logs {
		level := log.Level
		if level == "" {
			level = "info" // Default level
		}

		// Format log entry as JSON string
		logJSON, _ := json.Marshal(log)
		timestamp := fmt.Sprintf("%d", time.Now().UTC().UnixNano())

		streamsByLevel[level] = append(streamsByLevel[level], timestamp, string(logJSON))
	}

	// Create Loki push request
	push := lokiPush{
		Streams: make([]lokiStream, 0, len(streamsByLevel)),
	}

	for level, values := range streamsByLevel {
		// Group values into pairs [timestamp, log]
		pairs := make([][]string, 0, len(values)/2)
		for i := 0; i < len(values); i += 2 {
			if i+1 < len(values) {
				pairs = append(pairs, []string{values[i], values[i+1]})
			}
		}

		push.Streams = append(push.Streams, lokiStream{
			Stream: map[string]string{KeyLevel: level},
			Values: pairs,
		})
	}

	// Convert to JSON
	payload, err := json.Marshal(push)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	// Create request
	req, err := http.NewRequest("POST", s.endpoint+"/loki/api/v1/push", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")

	// Add authentication if provided
	if s.username != "" && s.password != "" {
		req.SetBasicAuth(s.username, s.password)
	}

	// Add tenant header if provided
	if s.tenant != "" {
		req.Header.Set("X-Scope-OrgID", s.tenant)
	}

	// Send request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send logs: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send logs, status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GrafanaAlloySink is a buffered sink backed by Grafana Alloy.
type GrafanaAlloySink struct {
	*BufferedHTTPSink
}

func NewGrafanaAlloySink(endpoint, apiKey string, maxBufferSize int, flushInterval time.Duration) *GrafanaAlloySink {
	sender := NewGrafanaAlloySender(endpoint, apiKey)
	sink := &GrafanaAlloySink{
		BufferedHTTPSink: NewBufferedHTTPSink(
			sender,
			maxBufferSize,
			flushInterval,
			3,           // Max retries
			time.Second, // Initial retry delay
			2,           // Worker count
		),
	}
	return sink
}

// GrafanaLokiSink is a buffered sink backed by Grafana Loki.
type GrafanaLokiSink struct {
	*BufferedHTTPSink
}

func NewGrafanaLokiSink(endpoint, username, password, tenant string, maxBufferSize int, flushInterval time.Duration) *GrafanaLokiSink {
	sender := NewGrafanaLokiSender(endpoint, username, password, tenant)
	sink := &GrafanaLokiSink{
		BufferedHTTPSink: NewBufferedHTTPSink(
			sender,
			maxBufferSize,
			flushInterval,
			3,           // Max retries
			time.Second, // Initial retry delay
			2,           // Worker count
		),
	}
	return sink
}
