package main

import (
	"bytes"
	"log"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	httpTLSHandshakeLogInterval = time.Second
	httpTLSHandshakeLogLimit    = 10
)

// newHTTPServerErrorLog bounds expected pre-request TLS noise without sampling
// other server diagnostics or the process logger. net/http supplies one complete
// record per Write; flags and prefixes must stay disabled for classification.
func newHTTPServerErrorLog(logger *zap.Logger) *log.Logger {
	serverLogger := logger.Named("http.server")
	handshakeLogger := serverLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(core, httpTLSHandshakeLogInterval, httpTLSHandshakeLogLimit, 0)
	}))
	return log.New(&httpServerErrorWriter{
		server:    serverLogger,
		handshake: handshakeLogger,
	}, "", 0)
}

type httpServerErrorWriter struct {
	server    *zap.Logger
	handshake *zap.Logger
}

func (writer *httpServerErrorWriter) Write(record []byte) (int, error) {
	logger := writer.server
	message := "HTTP server error"
	if bytes.HasPrefix(record, []byte("http: TLS handshake error from ")) {
		// Keep changing remote addresses and error text out of the sampler key.
		// The standard logger serializes writes into this one shared budget.
		logger = writer.handshake
		message = "TLS handshake failed"
	}
	if entry := logger.Check(zapcore.ErrorLevel, message); entry != nil {
		// Copy only admitted details: log.Logger reuses the Write buffer.
		entry.Write(zap.String("detail", string(bytes.TrimSuffix(record, []byte("\n")))))
	}
	return len(record), nil
}
