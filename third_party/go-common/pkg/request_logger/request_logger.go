// Package request_logger provides optional SQLite-backed HTTP request logging.
package request_logger

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

// RequestLogEntry is the metadata stored for one completed HTTP request. AuthHeader
// contains the configured header value verbatim, so entries may contain credentials.
type RequestLogEntry struct {
	ID         int64
	Timestamp  time.Time
	Method     string
	Path       string
	RemoteAddr string
	UserAgent  string
	StatusCode int
	Duration   time.Duration
	UserID     string
	AuthHeader string
	TraceID    string
}

// statusRecorder wraps http.ResponseWriter to capture the response status code
// for request logging. It passes Hijack/Flush through so WebSocket upgrades and
// streaming responses keep working behind the middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *statusRecorder) Write(b []byte) (int, error) {
	if sr.status == 0 {
		sr.status = http.StatusOK
	}
	return sr.ResponseWriter.Write(b)
}

// Unwrap supports http.ResponseController-based access to the underlying writer.
func (sr *statusRecorder) Unwrap() http.ResponseWriter {
	return sr.ResponseWriter
}

func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := sr.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// requestLogChannelCapacity bounds the buffer feeding the single log-writer
// goroutine. Entries are dropped once it fills so request handling never blocks
// on the serialized SQLite insert.
// Preserve the existing component name for downstream log consumers.
const requestLogLoggerName = "common_api"

const (
	requestLogCleanupQuarantinePrefix = ".request-log-cleanup-"
	requestLogChannelCapacity         = 1024
	requestLogDirectoryMode           = 0o700
	requestLogFileMode                = 0o600
)

var requestLogSidecarSuffixes = [...]string{"-journal", "-shm", "-wal"}

// RequestLogger RequestLogger defines data/state used by this package and is consumed as part of the backend request and service integration flow.
type RequestLogger struct {
	logDBPath            string
	authHeaderName       string
	remoteAddrHeaderName string
	db                   *sql.DB
	dbConn               *sql.Conn
	logStmt              *sql.Stmt // Prepared statement for logging requests
	logArtifacts         *requestLogArtifacts
	logMutex             sync.Mutex // Mutex to protect SQLite database access
	logger               *zap.Logger
	ctx                  context.Context      // Context for managing background goroutines
	cancel               context.CancelFunc   // Function to cancel background operations
	logCh                chan RequestLogEntry // Bounded queue drained by a single writer goroutine
	wg                   sync.WaitGroup       // Tracks the writer goroutine for a clean shutdown
	enqueueLock          sync.RWMutex         // Prevents sends from racing channel shutdown
	closed               bool
	closeOnce            sync.Once
	closeErr             error
	droppedLogs          atomic.Uint64
}

// NewRequestLogger opens an opt-in SQLite request log at a regular-file path and
// starts one writer goroutine with a bounded 1,024-entry queue. The configured auth
// header is stored verbatim; callers own access controls, retention, and redaction policy.
// Plain paths are resolved to absolute during initialization. Secure path traversal
// is supported on Darwin, Linux, FreeBSD, NetBSD, and OpenBSD; unsupported platforms,
// URI/in-memory DSNs, symlinks, and non-regular targets fail construction.
// The caller must call Close to drain accepted entries and release SQLite resources.
func NewRequestLogger(logDBPath, authHeaderName, remoteAddrHeaderName string, logger *zap.Logger) (*RequestLogger, error) {
	// Create a cancellable context for managing background operations
	ctx, cancel := context.WithCancel(context.Background())

	rl := &RequestLogger{
		logDBPath:            logDBPath,
		authHeaderName:       authHeaderName,
		remoteAddrHeaderName: remoteAddrHeaderName,
		logger:               logger,
		ctx:                  ctx,
		cancel:               cancel,
		logCh:                make(chan RequestLogEntry, requestLogChannelCapacity),
	}

	// Initialize the logger
	err := rl.initialize()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize request logger: %w", err)
	}

	// Drain the queue from a single writer goroutine so the log path uses O(1)
	// goroutines regardless of request rate.
	rl.wg.Add(1)
	go rl.processLogEntries()

	return rl, nil
}

// processLogEntries drains the bounded log queue on a single goroutine, writing
// each entry synchronously so SQLite is never touched concurrently. Close closes
// logCh and this range drains every accepted entry before the database is closed.
func (rl *RequestLogger) processLogEntries() {
	defer rl.wg.Done()
	for entry := range rl.logCh {
		rl.LogRequest(rl.ctx, entry)
	}
}

func validateRequestLogPath(logDBPath string) (cleanPath string, err error) {
	if strings.TrimSpace(logDBPath) == "" {
		return "", fmt.Errorf("log database path cannot be empty")
	}
	if strings.IndexByte(logDBPath, 0) >= 0 {
		return "", fmt.Errorf("log database path cannot contain a NUL byte")
	}

	lowerPath := strings.ToLower(logDBPath)
	if lowerPath == ":memory:" || strings.HasPrefix(lowerPath, "file:") {
		return "", fmt.Errorf("SQLite URI and in-memory DSNs are not supported; provide a filesystem path")
	}
	if strings.ContainsAny(logDBPath, "?#") {
		return "", fmt.Errorf("SQLite DSN query and fragment syntax is not supported; provide a filesystem path")
	}
	if parsed, parseErr := url.Parse(logDBPath); parseErr != nil {
		return "", fmt.Errorf("invalid log database path: %w", parseErr)
	} else if parsed.Scheme != "" {
		return "", fmt.Errorf("URI schemes are not supported in the log database path")
	}
	if strings.ContainsAny(logDBPath, ";'\"") {
		return "", fmt.Errorf("invalid characters in log database path")
	}
	if strings.HasSuffix(logDBPath, string(os.PathSeparator)) {
		return "", fmt.Errorf("log database path must name a file, not a directory")
	}

	return filepath.Clean(logDBPath), nil
}

// initialize initialize supports the backend request flow by performing this package's local processing step.
func (rl *RequestLogger) initialize() error {
	return rl.initializeWithOpen(sql.Open)
}

func (rl *RequestLogger) initializeWithOpen(
	openDatabase func(driverName, dataSourceName string) (*sql.DB, error),
) (err error) {
	if err = requestLoggerPlatformError(); err != nil {
		return err
	}
	cleanPath, err := validateRequestLogPath(rl.logDBPath)
	if err != nil {
		return err
	}
	resolvedPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return fmt.Errorf("failed to resolve log database path: %w", err)
	}

	artifacts, err := prepareRequestLogArtifacts(resolvedPath)
	if err != nil {
		return err
	}
	rl.logDBPath = resolvedPath

	var db *sql.DB
	var dbConn *sql.Conn
	var stmt *sql.Stmt
	defer func() {
		if err == nil {
			return
		}
		if stmt != nil {
			err = errors.Join(err, stmt.Close())
		}
		if dbConn != nil {
			err = errors.Join(err, dbConn.Close())
		}
		if db != nil {
			err = errors.Join(err, db.Close())
		}
		err = errors.Join(err, artifacts.captureCreatedSidecars())
		err = errors.Join(err, artifacts.cleanup())
	}()

	// The package imports a CGo-free SQLite driver, so consumers do not need to
	// register one and CGO-disabled binaries retain request logging support.
	db, err = openDatabase("sqlite", artifacts.databasePath())
	if err != nil {
		return fmt.Errorf("failed to open log database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	dbConn, err = db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to connect to log database: %w", err)
	}
	if err = artifacts.verifyDatabasePath(); err != nil {
		return fmt.Errorf("log database path changed while opening: %w", err)
	}

	// Create the table if it doesn't exist
	_, err = dbConn.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS request_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME NOT NULL,
			method TEXT NOT NULL,
			path TEXT NOT NULL,
			remote_addr TEXT NOT NULL,
			user_agent TEXT NOT NULL,
			status_code INTEGER NOT NULL,
			request_body TEXT,
			response_body TEXT,
			duration INTEGER NOT NULL,
			user_id TEXT,
			auth_header TEXT,
			trace_id TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create log table: %w", err)
	}

	// Prepare the statement for logging requests
	stmt, err = dbConn.PrepareContext(context.Background(), `
		INSERT INTO request_logs
		(timestamp, method, path, remote_addr, user_agent, status_code, duration, user_id, auth_header, trace_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare log statement: %w", err)
	}

	rl.db = db
	rl.dbConn = dbConn
	rl.logStmt = stmt
	rl.logArtifacts = artifacts
	if err = artifacts.commit(); err != nil {
		return fmt.Errorf("failed to retain secure request-log path: %w", err)
	}
	return nil
}

// LoggingMiddleware LoggingMiddleware builds middleware logic that is executed as part of the HTTP request processing pipeline.
//
// It must be registered as an SRouter global middleware (inside the router's
// middleware chain, after the trace middleware) — the trace ID and user ID are
// stored on a request derived inside the router, so wrapping the router
// externally (http.ListenAndServe(addr, rl.LoggingMiddleware(router))) logs
// every request with an empty trace_id and user_id.
//
// Parameters:
// - next (http.Handler): is the downstream HTTP handler invoked after this middleware completes its checks/work.
func (rl *RequestLogger) LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Record the start time
		startTime := time.Now().UTC()

		// Call the next handler, recording the response status code
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)

		// A handler that never wrote anything (e.g. hijacked the connection)
		// leaves the status at zero; net/http would have sent 200 OK.
		statusCode := recorder.status
		if statusCode == 0 {
			statusCode = http.StatusOK
		}

		// Record the duration
		duration := time.Since(startTime)

		userID, _ := scontext.GetUserID[common_service_models.UserIdType](r.Context())

		// Get the remote address, using the configured header if specified
		remoteAddr := r.RemoteAddr
		if rl.remoteAddrHeaderName != "" {
			if headerValue := r.Header.Get(rl.remoteAddrHeaderName); headerValue != "" {
				// For headers like X-Forwarded-For that might contain multiple IPs, use the first one
				if strings.Contains(headerValue, ",") {
					remoteAddr = strings.TrimSpace(strings.Split(headerValue, ",")[0])
				} else {
					remoteAddr = headerValue
				}
			}
		}

		// Get the auth header if available (empty string if not present)
		authHeader := r.Header.Get(rl.authHeaderName)

		// Create the log entry
		entry := RequestLogEntry{
			Timestamp:  time.Now().UTC(),
			Method:     r.Method,
			Path:       r.URL.Path,
			RemoteAddr: remoteAddr,
			UserAgent:  r.UserAgent(),
			StatusCode: statusCode,
			Duration:   duration,
			UserID:     fmt.Sprint(userID),
			AuthHeader: authHeader,
			TraceID:    scontext.GetTraceID(r.Context()),
		}

		// Hand the entry to the single writer goroutine without blocking the
		// request. Overflow is counted and reported at exponentially increasing
		// intervals so audit degradation is visible without creating a log storm.
		rl.enqueueRequestLog(entry)
	})
}

func (rl *RequestLogger) enqueueRequestLog(entry RequestLogEntry) bool {
	rl.enqueueLock.RLock()
	if rl.closed {
		rl.enqueueLock.RUnlock()
		rl.recordDroppedRequestLog(entry, "logger_closed")
		return false
	}

	select {
	case rl.logCh <- entry:
		rl.enqueueLock.RUnlock()
		return true
	default:
		rl.enqueueLock.RUnlock()
		rl.recordDroppedRequestLog(entry, "queue_full")
		return false
	}
}

func (rl *RequestLogger) recordDroppedRequestLog(entry RequestLogEntry, reason string) {
	dropped := rl.droppedLogs.Add(1)
	// Warn for the first drop and powers of two thereafter. The cumulative count
	// remains queryable through DroppedRequestLogs between warning checkpoints.
	if dropped&(dropped-1) != 0 || rl.logger == nil {
		return
	}
	rl.logger.Named(requestLogLoggerName).Warn("Request log entry dropped",
		zap.String(logkeys.Reason, reason),
		zap.Uint64(appLogger.KeyDroppedCount, dropped),
		zap.Int(appLogger.KeyQueueCapacity, cap(rl.logCh)),
		zap.String(logkeys.Method, entry.Method),
		zap.String(logkeys.Path, entry.Path),
		zap.Int(logkeys.StatusCode, entry.StatusCode),
		zap.String(logkeys.TraceID, entry.TraceID))
}

// DroppedRequestLogs returns the cumulative number of entries rejected because
// the bounded queue was full or the request logger had already begun closing.
func (rl *RequestLogger) DroppedRequestLogs() uint64 {
	return rl.droppedLogs.Load()
}

// LogRequest LogRequest supports the backend request flow by performing this package's local processing step.
//
// Parameters:
// - ctx (context.Context): is the request context used for cancellation, deadlines, and scoped values.
// - entry (RequestLogEntry): provides input required by LogRequest.
func (rl *RequestLogger) LogRequest(ctx context.Context, entry RequestLogEntry) {
	if rl.db == nil || rl.logStmt == nil {
		return
	}

	// Check if context is cancelled before proceeding
	select {
	case <-ctx.Done():
		if appLogger.DebugEnabled(rl.logger) {
			rl.logger.Named(requestLogLoggerName).Debug("Request logging cancelled due to context cancellation", zap.String(logkeys.TraceID, entry.TraceID))
		}
		return
	default:
	}

	// Lock the mutex to prevent concurrent writes to SQLite
	rl.logMutex.Lock()
	defer rl.logMutex.Unlock()
	if rl.logArtifacts != nil {
		if err := rl.logArtifacts.verifyDatabasePath(); err != nil {
			if rl.logger != nil {
				rl.logger.Named(requestLogLoggerName).Warn("Request log database path identity changed",
					zap.String(logkeys.Operation, "verify_request_log_path"),
					zap.String(logkeys.TraceID, entry.TraceID),
					zap.Error(err))
			}
			return
		}
	}

	// Use the prepared statement for better efficiency
	_, err := rl.logStmt.Exec(
		entry.Timestamp, entry.Method, entry.Path, entry.RemoteAddr, entry.UserAgent,
		entry.StatusCode, entry.Duration.Milliseconds(), entry.UserID, entry.AuthHeader, entry.TraceID,
	)
	if err != nil {
		rl.logger.Named(requestLogLoggerName).Warn("Failed to log request to database",
			zap.String(logkeys.Operation, "persist_request_log"),
			zap.String(logkeys.TraceID, entry.TraceID),
			zap.Error(err))
	}
}

// Close Close handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (rl *RequestLogger) Close() error {
	rl.closeOnce.Do(func() {
		// Stop new enqueue attempts without racing a send on the channel, then let
		// the writer range drain everything already accepted.
		rl.enqueueLock.Lock()
		rl.closed = true
		if rl.logCh != nil {
			close(rl.logCh)
		}
		rl.enqueueLock.Unlock()
		rl.wg.Wait()

		// Cancellation happens only after the drain; cancelling earlier makes
		// LogRequest discard the very entries Close is intended to preserve.
		if rl.cancel != nil {
			rl.cancel()
		}

		if rl.logStmt != nil {
			if err := rl.logStmt.Close(); err != nil && rl.logger != nil {
				rl.logger.Named(requestLogLoggerName).Warn("Failed to close prepared statement",
					zap.String(logkeys.Operation, "close_request_log_statement"),
					zap.Error(err))
			}
		}

		if rl.dbConn != nil {
			if err := rl.dbConn.Close(); err != nil && rl.closeErr == nil {
				rl.closeErr = err
			}
		}
		if rl.db != nil {
			if err := rl.db.Close(); err != nil {
				if rl.logger != nil {
					rl.logger.Named(requestLogLoggerName).Warn("Failed to close request log database",
						zap.String(logkeys.Operation, "close_request_log_database"),
						zap.Error(err))
				}
				rl.closeErr = err
			}
		}
		if rl.logArtifacts != nil {
			if err := rl.logArtifacts.release(); err != nil && rl.closeErr == nil {
				rl.closeErr = err
			}
		}
	})
	return rl.closeErr
}
