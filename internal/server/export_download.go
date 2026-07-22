package server

import (
	"context"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
)

const (
	maximumExportDownloadDuration = 30 * time.Minute
	exportDownloadBufferBytes     = 32 << 10
	exportDownloadRealm           = `Bearer realm="open-splunk-export"`
)

var exportDownloadBuffers = sync.Pool{New: func() any {
	buffer := make([]byte, exportDownloadBufferBytes)
	return &buffer
}}

// downloadExport redeems a one-time capability and streams its pinned
// artifact. The route deliberately bypasses protobuf decoding and the short
// synchronous API timeout, but remains behind browser-origin validation, a
// dedicated download admission bound, and an explicit transport deadline.
func (handler *apiHandler) downloadExport(response http.ResponseWriter, request *http.Request) {
	if !validExportDownloadShape(request) {
		writeDownloadError(response, http.StatusBadRequest, "download request is invalid")
		return
	}
	token, ok := exportBearerToken(request.Header.Values("Authorization"))
	if !ok {
		writeInvalidDownloadGrant(response)
		return
	}
	if err := request.Context().Err(); err != nil {
		return
	}
	downloadContext, cancelDownload := context.WithTimeout(request.Context(), maximumExportDownloadDuration)
	defer cancelDownload()
	request = request.WithContext(downloadContext)

	controller := http.NewResponseController(response)
	writeDeadline, _ := downloadContext.Deadline()
	if err := controller.SetWriteDeadline(writeDeadline); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			writeDownloadError(response, http.StatusServiceUnavailable, "download transport is unavailable")
			return
		}
	}

	download, err := handler.exports.RedeemDownload(request.Context(), token)
	if err != nil {
		switch {
		case request.Context().Err() != nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return
		case errors.Is(err, exportjobs.ErrInvalidDownloadGrant):
			writeInvalidDownloadGrant(response)
		case errors.Is(err, exportjobs.ErrDownloadGrantCapacity):
			response.Header().Set("Retry-After", "1")
			writeDownloadError(response, http.StatusTooManyRequests, "download capacity is exhausted")
		default:
			writeDownloadError(response, http.StatusServiceUnavailable, "download is unavailable")
		}
		return
	}
	if download == nil {
		writeDownloadError(response, http.StatusServiceUnavailable, "download is unavailable")
		return
	}
	defer func() { _ = download.Close() }()

	artifact := download.Artifact()
	contentDisposition, ok := validDownloadArtifact(artifact)
	if !ok {
		writeDownloadError(response, http.StatusServiceUnavailable, "download is unavailable")
		return
	}
	if err := request.Context().Err(); err != nil {
		return
	}
	response.Header().Set("Content-Type", artifact.MediaType)
	response.Header().Set("Content-Disposition", contentDisposition)
	response.Header().Set("Content-Length", strconv.FormatUint(artifact.SizeBytes, 10))
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Pragma", "no-cache")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Content-Security-Policy", "sandbox")
	response.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	response.WriteHeader(http.StatusOK)

	bufferPointer := exportDownloadBuffers.Get().(*[]byte)
	buffer := *bufferPointer
	defer exportDownloadBuffers.Put(bufferPointer)
	remaining := artifact.SizeBytes
	for remaining > 0 {
		if err := request.Context().Err(); err != nil {
			return
		}
		readBytes := uint64(len(buffer))
		if remaining < readBytes {
			readBytes = remaining
		}
		read, readErr := download.Read(buffer[:int(readBytes)])
		if read < 0 || read > int(readBytes) {
			return
		}
		if read > 0 {
			if writeErr := writeDownloadBytes(request.Context(), response, buffer[:read]); writeErr != nil {
				return
			}
			remaining -= uint64(read)
		}
		if remaining == 0 {
			break
		}
		if readErr != nil || read == 0 {
			return
		}
	}
	// Keep the download gate, pinned artifact descriptor, and write deadline
	// through net/http's buffered wire flush. Otherwise the handler could return
	// while finishRequest blocks outside the advertised concurrency bound.
	if err := controller.Flush(); err != nil {
		return
	}
}

func validExportDownloadShape(request *http.Request) bool {
	if request == nil || request.Method != http.MethodGet || request.URL == nil ||
		request.URL.RawQuery != "" || request.URL.ForceQuery || request.URL.Opaque != "" || request.URL.User != nil ||
		len(request.Header.Values("Range")) != 0 || len(request.TransferEncoding) != 0 || request.ContentLength != 0 {
		return false
	}
	return true
}

func exportBearerToken(values []string) (string, bool) {
	if len(values) != 1 {
		return "", false
	}
	value := values[0]
	if strings.Count(value, " ") != 1 || strings.Contains(value, ",") {
		return "", false
	}
	scheme, token, found := strings.Cut(value, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" ||
		strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return "", false
	}
	return token, true
}

func validDownloadArtifact(artifact exportjobs.Artifact) (string, bool) {
	if artifact.SizeBytes > math.MaxInt64 || artifact.FileName == "" || artifact.FileName == "." ||
		len(artifact.FileName) > 255 || !utf8.ValidString(artifact.FileName) ||
		strings.ContainsAny(artifact.FileName, "/\\\r\n") || strings.IndexFunc(artifact.FileName, unicode.IsControl) >= 0 {
		return "", false
	}
	if _, _, err := mime.ParseMediaType(artifact.MediaType); err != nil {
		return "", false
	}
	disposition := mime.FormatMediaType("attachment", map[string]string{"filename": artifact.FileName})
	return disposition, disposition != ""
}

func writeDownloadBytes(ctx context.Context, response http.ResponseWriter, payload []byte) error {
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := response.Write(payload)
		if written < 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeInvalidDownloadGrant(response http.ResponseWriter) {
	response.Header().Set("WWW-Authenticate", exportDownloadRealm)
	writeDownloadError(response, http.StatusUnauthorized, "download grant is invalid")
}

func writeDownloadError(response http.ResponseWriter, status int, message string) {
	writeAPIError(response, status, message)
}
