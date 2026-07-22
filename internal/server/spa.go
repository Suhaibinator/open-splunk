package server

import (
	"errors"
	"io/fs"
	"net/http"
	"path"
	"regexp"
	"strings"
)

var nextHexContentHash = regexp.MustCompile(`(?:^|[._-])[0-9a-fA-F]{8,}(?:[._-]|$)`)

type spaHandler struct {
	filesystem fs.FS
	files      http.Handler
	index      []byte
}

func newSPAHandler(filesystem fs.FS) (http.Handler, error) {
	index, err := fs.ReadFile(filesystem, "index.html")
	if err != nil {
		return nil, errors.New("read web UI index.html")
	}
	return &spaHandler{
		filesystem: filesystem,
		files:      http.FileServerFS(filesystem),
		index:      index,
	}, nil
}

func (handler *spaHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", "GET, HEAD")
		http.Error(response, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	cleanPath := path.Clean("/" + request.URL.Path)
	relativePath := strings.TrimPrefix(cleanPath, "/")
	if relativePath == "" {
		handler.serveIndex(response, request)
		return
	}
	fileInfo, err := fs.Stat(handler.filesystem, relativePath)
	if err == nil {
		if fileInfo.Mode().IsRegular() {
			handler.serveFile(response, request, relativePath)
			return
		}
		if fileInfo.IsDir() {
			indexPath := path.Join(relativePath, "index.html")
			indexInfo, indexErr := fs.Stat(handler.filesystem, indexPath)
			if indexErr != nil || !indexInfo.Mode().IsRegular() {
				http.NotFound(response, request)
				return
			}
			// FileServerFS resolves an index only from a trailing-slash path. We
			// verified the index first, so it can never emit a directory listing.
			setStaticCacheHeaders(response, indexPath)
			clone := request.Clone(request.Context())
			clone.URL.Path = "/" + strings.TrimSuffix(relativePath, "/") + "/"
			handler.files.ServeHTTP(response, clone)
			return
		}
		http.NotFound(response, request)
		return
	}
	// Missing asset-like paths must remain a real 404. Extensionless paths are
	// browser routes and receive the static application's entry document.
	if path.Ext(relativePath) != "" {
		http.NotFound(response, request)
		return
	}
	handler.serveIndex(response, request)
}

func (handler *spaHandler) serveFile(response http.ResponseWriter, request *http.Request, relativePath string) {
	setStaticCacheHeaders(response, relativePath)
	clone := request.Clone(request.Context())
	clone.URL.Path = "/" + relativePath
	handler.files.ServeHTTP(response, clone)
}

func (handler *spaHandler) serveIndex(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	response.Header().Set("Cache-Control", "no-cache")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.WriteHeader(http.StatusOK)
	if request.Method == http.MethodGet {
		_, _ = response.Write(handler.index)
	}
}

func setStaticCacheHeaders(response http.ResponseWriter, relativePath string) {
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Cache-Control", "no-cache")
	if isContentHashedNextAsset(relativePath) {
		response.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
}

func isContentHashedNextAsset(relativePath string) bool {
	const prefix = "_next/static/"
	if !strings.HasPrefix(relativePath, prefix) {
		return false
	}
	remainder := strings.TrimPrefix(relativePath, prefix)
	base := path.Base(remainder)
	if nextHexContentHash.MatchString(base) {
		return true
	}
	stem := strings.TrimSuffix(base, path.Ext(base))
	// Turbopack emits names such as turbopack-3bepg-s46cnic.js. Both
	// generated suffixes are needed; a human-readable version suffix alone
	// must not turn an asset immutable.
	if strings.HasPrefix(stem, "turbopack-") {
		parts := strings.Split(stem, "-")
		if len(parts) >= 3 && looksLikeGeneratedHash(parts[len(parts)-2], 5) && looksLikeGeneratedHash(parts[len(parts)-1], 5) {
			return true
		}
	}
	// Current Next/Turbopack chunk names are compact base-36-like hashes,
	// often including '_' (for example 0cz1d0mv5g_q7.js).
	compactStem := strings.TrimSuffix(stem, "-")
	if !strings.Contains(compactStem, "-") && looksLikeGeneratedHash(compactStem, 10) {
		return true
	}
	// Build manifests use stable human-readable filenames inside a generated
	// build-ID directory. The directory, rather than the basename, provides
	// their immutable identity.
	segments := strings.Split(remainder, "/")
	for _, segment := range segments[:max(0, len(segments)-1)] {
		if segment != "chunks" && looksLikeGeneratedHash(segment, 16) {
			return true
		}
	}
	return false
}

func looksLikeGeneratedHash(value string, minimumLength int) bool {
	if len(value) < minimumLength {
		return false
	}
	hasLetter := false
	hasDigit := false
	for index := range len(value) {
		character := value[index]
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z':
			hasLetter = true
		case character >= '0' && character <= '9':
			hasDigit = true
		case character == '_', character == '-':
		default:
			return false
		}
	}
	return hasLetter && hasDigit
}
