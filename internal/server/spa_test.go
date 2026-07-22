package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSPAStaticServingIsCacheSafeAndNeverListsDirectories(t *testing.T) {
	t.Parallel()

	handler, err := newSPAHandler(fstest.MapFS{
		"index.html":                                              &fstest.MapFile{Data: []byte("root index")},
		"route.txt":                                               &fstest.MapFile{Data: []byte("route payload")},
		"docs/index.html":                                         &fstest.MapFile{Data: []byte("docs index")},
		"private/secret.txt":                                      &fstest.MapFile{Data: []byte("must not list")},
		"_next/static/runtime.js":                                 &fstest.MapFile{Data: []byte("runtime")},
		"_next/static/runtime-v2-bundle.js":                       &fstest.MapFile{Data: []byte("versioned but unhashed")},
		"_next/static/chunks/chunk.0123456789ab.js":               &fstest.MapFile{Data: []byte("hex hashed")},
		"_next/static/chunks/0aidbw1urd4t-.css":                   &fstest.MapFile{Data: []byte("Turbopack CSS")},
		"_next/static/chunks/0cz1d0mv5g_q7.js":                    &fstest.MapFile{Data: []byte("Turbopack chunk")},
		"_next/static/chunks/turbopack-3bepg-s46cnic.js":          &fstest.MapFile{Data: []byte("Turbopack runtime")},
		"_next/static/e1SfZS-eqoiqae17OZdyM/_buildManifest.js":    &fstest.MapFile{Data: []byte("build manifest")},
		"_next/static/e1SfZS-eqoiqae17OZdyM/_clientMiddleware.js": &fstest.MapFile{Data: []byte("client manifest")},
	})
	if err != nil {
		t.Fatalf("newSPAHandler: %v", err)
	}

	tests := []struct {
		name      string
		path      string
		wantCode  int
		wantBody  string
		immutable bool
	}{
		{name: "hex hashed Next asset", path: "/_next/static/chunks/chunk.0123456789ab.js", wantCode: http.StatusOK, wantBody: "hex hashed", immutable: true},
		{name: "Turbopack chunk", path: "/_next/static/chunks/0cz1d0mv5g_q7.js", wantCode: http.StatusOK, wantBody: "Turbopack chunk", immutable: true},
		{name: "Turbopack CSS", path: "/_next/static/chunks/0aidbw1urd4t-.css", wantCode: http.StatusOK, wantBody: "Turbopack CSS", immutable: true},
		{name: "Turbopack runtime", path: "/_next/static/chunks/turbopack-3bepg-s46cnic.js", wantCode: http.StatusOK, wantBody: "Turbopack runtime", immutable: true},
		{name: "build manifest", path: "/_next/static/e1SfZS-eqoiqae17OZdyM/_buildManifest.js", wantCode: http.StatusOK, wantBody: "build manifest", immutable: true},
		{name: "unhashed Next asset", path: "/_next/static/runtime.js", wantCode: http.StatusOK, wantBody: "runtime"},
		{name: "human version is not a hash", path: "/_next/static/runtime-v2-bundle.js", wantCode: http.StatusOK, wantBody: "versioned but unhashed"},
		{name: "route payload", path: "/route.txt", wantCode: http.StatusOK, wantBody: "route payload"},
		{name: "directory index", path: "/docs", wantCode: http.StatusOK, wantBody: "docs index"},
		{name: "directory listing denied", path: "/private", wantCode: http.StatusNotFound},
		{name: "Next directory listing denied", path: "/_next/static", wantCode: http.StatusNotFound},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantCode {
				t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
			}
			if test.wantBody != "" && response.Body.String() != test.wantBody {
				t.Fatalf("body = %q, want %q", response.Body.String(), test.wantBody)
			}
			cache := response.Header().Get("Cache-Control")
			if test.immutable && !strings.Contains(cache, "immutable") {
				t.Fatalf("cache = %q, want immutable", cache)
			}
			if !test.immutable && test.wantCode == http.StatusOK && cache != "no-cache" {
				t.Fatalf("cache = %q, want no-cache", cache)
			}
		})
	}
}

func TestSPAHeadAndMethodHandling(t *testing.T) {
	t.Parallel()
	handler, err := newSPAHandler(fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("index")},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodHead, "/browser-route", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.Len() != 0 || response.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("HEAD response = %d body %q cache %q", response.Code, response.Body.String(), response.Header().Get("Cache-Control"))
	}

	request = httptest.NewRequest(http.MethodPost, "/", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST response = %d Allow %q", response.Code, response.Header().Get("Allow"))
	}
}
