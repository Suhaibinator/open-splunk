//go:build !windows

package integration_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This deliberately crosses both compiled process boundaries. Timestamps alone
// are rebased to the live index retention window; bodies and expected decoded
// projections are specified independently of the production decoders.
func TestBackendNativeFormats(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run native format integration")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker is required: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	work := t.TempDir()
	image, err := testsupport.ResolvePinnedClickHouseImage(os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatal(err)
	}
	clickHouse, err := testsupport.StartClickHouseWithServicePrincipals(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if err := clickHouse.Close(cleanup); err != nil {
			t.Errorf("ClickHouse cleanup: %v", err)
		}
	})
	serverBinary := filepath.Join(work, "server")
	collectorBinary := filepath.Join(work, "collector")
	buildBinary(t, ctx, buildBackendFrontend(t, ctx, repository), serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")
	httpAddress, collectorAddress := unusedLoopbackAddress(t), unusedLoopbackAddress(t)
	identity, err := testsupport.WriteServerTLSIdentity(filepath.Join(work, "tls"), "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	adminPath, adminToken := provisionAdministratorToken(t, work)
	args := []string{serverBinary,
		"-http-listen-address=" + httpAddress,
		"-http-tls-certificate-file=" + identity.CertificateFile,
		"-http-tls-private-key-file=" + identity.PrivateKeyFile,
		"-control-database-file=" + filepath.Join(work, "control.sqlite"),
		"-master-key-file=" + filepath.Join(work, "master.key"),
		"-administrator-token-file=" + adminPath,
		"-collector-grpc-listen-address=" + collectorAddress,
		"-collector-grpc-plaintext-enabled",
		"-tenant-id=" + verticalTenantID,
	}
	args = append(args, clickHouseServerArguments(clickHouse)...)
	server := startProcess(t, t.TempDir(), args, clickHouseServerEnvironment(os.Environ(), clickHouse))
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: identity.RootCAs}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	baseURL := "https://" + httpAddress
	waitForHealth(t, ctx, client, baseURL, server, adminToken)
	var created opensplunk.CreateIndexResponse
	postAdministratorProto(t, ctx, client, baseURL+"/api/indexes/create", adminToken,
		&opensplunk.CreateIndexRequest{Definition: &opensplunk.IndexDefinition{
			Name: verticalIndexName, DisplayName: "Native format integration",
			RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		}}, &created)
	stamp := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	rfcStamp, accessStamp := stamp.Format(time.RFC3339Nano), stamp.Format("02/Jan/2006:15:04:05 -0700")
	fixtures := []struct{ format, options, raw, message string }{
		{"docker-json-file", "", `{"log":"docker\nsecond line\n","stream":"stderr","time":"` + rfcStamp + `"}`, "docker\nsecond line\n"},
		{"nginx-combined", "", `2001:db8::1 - alice [` + accessStamp + `] "GET /nginx HTTP/1.1" 201 17 "-" "quoted\x22agent"`, "GET /nginx HTTP/1.1"},
		{"apache-common", "", `example.test - - [` + accessStamp + `] "GET /common HTTP/1.0" 204 -`, "GET /common HTTP/1.0"},
		{"apache-combined", "", `127.0.0.1 - alice [` + accessStamp + `] "POST /combined HTTP/1.1" 503 0 "" "agent\tvalue"`, "POST /combined HTTP/1.1"},
		{"logfmt", "", `timestamp=` + rfcStamp + ` message="logfmt 日本語" count=9007199254740993 flag=true`, "logfmt 日本語"},
		{"log4j2-pattern", "    parser:\n      pattern: '%{timestamp}|%{level}|%{message}'\n      timestamp_layout: '2006-01-02T15:04:05Z07:00'\n", rfcStamp + "|ERROR|log4j failure", "log4j failure"},
		{"logback-pattern", "    parser:\n      pattern: '%{timestamp}|%{level}|%{message}'\n      timestamp_layout: '2006-01-02T15:04:05Z07:00'\n", rfcStamp + "|WARN|logback failure", "logback failure"},
	}
	stateDir := filepath.Join(work, "collector-state")
	tokenPath := filepath.Join(work, "collector.token")
	configPath := filepath.Join(work, "collector.yaml")
	var yaml strings.Builder
	fmt.Fprintf(&yaml, "server:\n  address: %q\n  transport: grpc\n  token_file: %q\n  tls:\n    enabled: false\nstate:\n  directory: %q\n  max_queue_bytes: 8MiB\ninputs:\n", collectorAddress, tokenPath, stateDir)
	for _, fixture := range fixtures {
		path := filepath.Join(work, fixture.format+".log")
		// An incomplete timestamp is malformed under every configured grammar.
		writePrivateFile(t, path, []byte(fixture.raw+"\nprivate-malformed-native-sentinel\n"+fixture.raw+"\n"))
		fmt.Fprintf(&yaml, "  - id: %s\n    type: file\n    include: [%q]\n    format: %s\n%s    start_at: beginning\n    index: %s\n    source: native-log\n    host: native-host\n    poll_interval: 15ms\n", fixture.format, path, fixture.format, fixture.options, verticalIndexName)
	}
	loadPath := filepath.Join(work, "ndjson.log")
	writePrivateFile(t, loadPath, nil)
	fmt.Fprintf(&yaml, "  - id: native-load-ndjson\n    type: file\n    include: [%q]\n    format: ndjson\n    start_at: beginning\n    index: %s\n    source: native-log\n    host: native-host\n    poll_interval: 15ms\n", loadPath, verticalIndexName)
	writePrivateFile(t, configPath, []byte(yaml.String()))
	collectorID := initializeCollectorIdentity(t, ctx, repository, collectorBinary, configPath, os.Environ(), stateDir)
	validateCollectorConfigurationWithInput(t, ctx, repository, collectorBinary, configPath, os.Environ(), "", "native formats", "docker-json-file")
	token := createIndexScopedIngestionToken(t, ctx, client, baseURL, adminToken, "native-formats", verticalIndexName, collectorID, []string{"^native-host$"}, []string{"^native-log$"})
	writePrivateFile(t, tokenPath, []byte(token+"\n"))
	collector := startProcess(t, repository, []string{collectorBinary, "run", "-config", configPath}, os.Environ())
	storage, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickHouse.Address}, Auth: clickhousedriver.Auth{Database: clickHouse.Database, Username: clickHouse.RuntimeUsername, Password: clickHouse.RuntimePassword}, DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	waitForStoredEventCount(t, ctx, storage, collector, token, uint64(len(fixtures)*2))
	for _, fixture := range fixtures {
		var count, unique uint64
		var body, source, host string
		var raw []byte
		var eventTime time.Time
		err := storage.QueryRow(ctx, `SELECT count(), uniqExact(event_id), any(ifNull(body, '')), any(raw), any(host), any(source), any(event_time) FROM open_splunk.events WHERE tenant_id = ? AND index_name = ? AND sourcetype = ?`, verticalTenantID, verticalIndexName, fixture.format).Scan(&count, &unique, &body, &raw, &host, &source, &eventTime)
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 || unique != 2 || body != fixture.message || string(raw) != fixture.raw || host != "native-host" || source != "native-log" || !eventTime.Equal(stamp) {
			t.Fatalf("%s projection count=%d distinct=%d body=%q raw=%q host=%q source=%q time=%s", fixture.format, count, unique, body, raw, host, source, eventTime)
		}
	}
	var integer int64
	var flag bool
	if err := storage.QueryRow(ctx, `SELECT any(dynamicElement(fields.count, 'Int64')), any(dynamicElement(fields.flag, 'Bool')) FROM open_splunk.events WHERE tenant_id = ? AND index_name = ? AND sourcetype = 'logfmt'`, verticalTenantID, verticalIndexName).Scan(&integer, &flag); err != nil {
		t.Fatal(err)
	}
	if integer != 9007199254740993 || !flag {
		t.Fatalf("logfmt typed projection integer=%d flag=%t", integer, flag)
	}
	assertProcessLogsDoNotLeak(t, collector.Logs(), token, "private-malformed-native-sentinel")
	if os.Getenv(backendLoadIntegrationFlag) == "1" {
		backendNativeMixedLoad(t, ctx, storage, collector, token, stateDir, work, stamp, uint64(len(fixtures)*2))
	}
}
