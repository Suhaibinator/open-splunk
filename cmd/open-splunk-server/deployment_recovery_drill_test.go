//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestDeploymentRecoveryDrill exercises the shipped Compose files and actual
// release binary. The only substituted boundary is the test child that pauses
// after receipt publication so the parent can kill its process before SQLite
// publication. No production command accepts a crash flag.
func TestDeploymentRecoveryDrill(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_DEPLOYMENT_RECOVERY_DRILL") != "1" {
		t.Skip("set OPEN_SPLUNK_DEPLOYMENT_RECOVERY_DRILL=1; see integration/README.md")
	}
	image := os.Getenv("OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE")
	helper := os.Getenv("OPEN_SPLUNK_RECOVERY_DRILL_HELPER_BINARY")
	if image == "" || !filepath.IsAbs(helper) {
		t.Fatal("an exact server image and absolute static test helper binary are required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	fixture := newRecoveryDrill(t, ctx, image, helper)
	fixture.compose(t, ctx, "config", "--quiet")
	fixture.compose(t, ctx, "run", "--rm", "prepare-recovery-volume")
	fixture.compose(t, ctx, "run", "--rm", "recovery", "provision-administrator-token",
		"-source", "/run/recovery/administrator/seed", "-destination", "/var/lib/open-splunk/state/private/administrator.token")
	fixture.compose(t, ctx, "up", "-d", "clickhouse", "server")
	fixture.waitReady(t, ctx)

	var index opensplunk.CreateIndexResponse
	fixture.post(t, ctx, "/api/indexes/create", fixture.administrator,
		&opensplunk.CreateIndexRequest{Definition: &opensplunk.IndexDefinition{
			Name: "recovery-drill", DisplayName: "Recovery drill", RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		}}, &index)
	var app opensplunk.CreateAppResponse
	fixture.post(t, ctx, "/api/apps/create", fixture.administrator,
		&opensplunk.CreateAppRequest{Definition: &opensplunk.AppDefinition{
			Slug: "recovery-drill", DisplayName: "Recovery drill", DefaultIndexNames: []string{"recovery-drill"},
		}}, &app)
	definition := &opensplunk.SearchDefinition{Spl: "index=recovery-drill | sort _raw | table _raw", AppId: new(app.GetApp().GetAppId()),
		TimeRange: &opensplunk.TimeRangeSpec{Earliest: new("-24h"), Latest: new("now")}}
	var saved opensplunk.CreateSavedSearchResponse
	fixture.post(t, ctx, "/api/saved-searches/create", fixture.administrator,
		&opensplunk.CreateSavedSearchRequest{Definition: &opensplunk.SavedSearchDefinition{
			Name: "Recovery retained events", Search: definition, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		}}, &saved)
	var token opensplunk.CreateIngestionTokenResponse
	fixture.post(t, ctx, "/api/ingestion-tokens/create", fixture.administrator,
		&opensplunk.CreateIngestionTokenRequest{Definition: &opensplunk.IngestionTokenDefinition{
			Name: "Recovery HEC", Purpose: opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunk.IngestionTokenConstraints{AllowedIndexNames: []string{"recovery-drill"}},
			HecProfile:  &opensplunk.IngestionTokenHecProfile{DefaultIndexName: new("recovery-drill"), IndexerAcknowledgment: true},
		}}, &token)
	if token.GetPlaintextToken() == "" {
		t.Fatal("token creation returned no credential")
	}
	fixture.diagnosticSecrets = append(fixture.diagnosticSecrets, token.GetPlaintextToken())
	for _, event := range []string{"recovery-event-one", "recovery-event-two", "recovery-event-three"} {
		fixture.hec(t, ctx, "/services/collector/event", token.GetPlaintextToken(), `{"event":"`+event+`"}`)
	}
	fixture.waitEvents(t, ctx, 3)
	job := fixture.search(t, ctx, definition)
	var retained opensplunk.GetSearchResultsResponse
	fixture.post(t, ctx, "/api/search/jobs/results", "", &opensplunk.GetSearchResultsRequest{SearchJobId: job}, &retained)
	if len(retained.GetResultPage().GetRows()) != 3 {
		t.Fatal("source retained search must contain all three seeded events")
	}
	t.Log("seeded app, index, saved search, HEC credential, three events and retained terminal result through the real API")

	fixture.compose(t, ctx, "stop", "server")
	fixture.child(t, ctx, "seed-pending")
	backup := append([]string{"run", "--rm", "recovery", "backup-deployment-recovery-set"}, recoveryDrillControlFlags()...)
	backup = append(backup, "-destination", recoveryDrillSet, "-archive-root", nativeRecoveryIntegrationArchiveRoot)
	backup = append(backup, recoveryDrillConnectionFlags("backup")...)
	fixture.compose(t, ctx, backup...)
	fixture.compose(t, ctx, "run", "--rm", "recovery", "verify-deployment-recovery-set", "-source", recoveryDrillSet, "-archive-root", nativeRecoveryIntegrationArchiveRoot)
	fixture.compose(t, ctx, "stop", "clickhouse")
	fixture.restore = true
	fixture.compose(t, ctx, "config", "--quiet")
	fixture.compose(t, ctx, "up", "-d", "clickhouse")
	fixture.waitClickHouse(t, ctx)

	// A real helper process stops at the receipt -> control publication boundary.
	childName := fixture.project + "-crash"
	fixture.children = append(fixture.children, childName)
	arguments := fixture.composeArguments("run", "--name", childName, "--no-deps",
		"-e", "OPEN_SPLUNK_RECOVERY_DRILL_CHILD=crash-restore", "--entrypoint", "/run/drill/helper", "recovery",
		"-test.run=^TestDeploymentRecoveryDrillChild$", "-test.v")
	child := exec.CommandContext(ctx, "docker", arguments...)
	child.Env = fixture.environment
	logFile, err := os.Create(filepath.Join(fixture.work, "crash.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	fixture.wait(t, ctx, "receipt publication boundary", func() bool {
		contents, readErr := os.ReadFile(logFile.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		select {
		case err := <-childDone:
			t.Fatalf("crash helper exited before boundary: %v\n%s", err, contents)
		default:
		}
		return strings.Contains(string(contents), "RECOVERY_RECEIPT_PUBLISHED")
	})
	before := fixture.restoreIdentity(t, ctx)
	fixture.docker(t, ctx, "kill", "--signal", "KILL", childName)
	select {
	case err := <-childDone:
		if err == nil {
			t.Fatal("killed restore helper unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Prove the crash happened before SQLite, key, token or artifacts appeared.
	fixture.child(t, ctx, "assert-target-absent")
	restore := append([]string{"run", "--rm", "recovery", "restore-deployment-recovery-set", "-source", recoveryDrillSet,
		"-archive-root", nativeRecoveryIntegrationArchiveRoot}, recoveryDrillControlFlags()...)
	restore = append(restore, recoveryDrillConnectionFlags("restore")...)
	fixture.compose(t, ctx, restore...)
	after := fixture.restoreIdentity(t, ctx)
	if before != after {
		t.Fatalf("exact retry changed native restore count or canonical identity: %s -> %s", before, after)
	}
	t.Log("killed restore after canonical receipt; exact release CLI retry published control plane without a second native RESTORE")

	fixture.compose(t, ctx, "up", "-d", "server")
	fixture.waitReady(t, ctx)
	var recoveredApp opensplunk.GetAppResponse
	fixture.post(t, ctx, "/api/apps/get", fixture.administrator, &opensplunk.GetAppRequest{Selector: &opensplunk.AppSelector{Selector: &opensplunk.AppSelector_AppId{AppId: app.GetApp().GetAppId()}}}, &recoveredApp)
	if !proto.Equal(app.GetApp(), recoveredApp.GetApp()) {
		t.Fatal("app changed across restore")
	}
	var recoveredIndex opensplunk.GetIndexResponse
	fixture.post(t, ctx, "/api/indexes/get", fixture.administrator, &opensplunk.GetIndexRequest{Selector: &opensplunk.IndexSelector{Selector: &opensplunk.IndexSelector_IndexName{IndexName: "recovery-drill"}}}, &recoveredIndex)
	if !proto.Equal(index.GetIndex(), recoveredIndex.GetIndex()) {
		t.Fatal("index changed across restore")
	}
	var recoveredSaved opensplunk.GetSavedSearchResponse
	fixture.post(t, ctx, "/api/saved-searches/get", fixture.administrator, &opensplunk.GetSavedSearchRequest{SavedSearchId: saved.GetSavedSearch().GetSavedSearchId()}, &recoveredSaved)
	if !proto.Equal(saved.GetSavedSearch(), recoveredSaved.GetSavedSearch()) {
		t.Fatal("saved search changed across restore")
	}
	var recoveredToken opensplunk.GetIngestionTokenResponse
	fixture.post(t, ctx, "/api/ingestion-tokens/get", fixture.administrator, &opensplunk.GetIngestionTokenRequest{IngestionTokenId: token.GetIngestionToken().GetIngestionTokenId()}, &recoveredToken)
	if recoveredToken.GetIngestionToken().GetTokenPrefix() != token.GetIngestionToken().GetTokenPrefix() {
		t.Fatal("token identity changed across restore")
	}
	fixture.hec(t, ctx, "/services/collector/health", token.GetPlaintextToken(), "")
	fixture.rejectAdministrator(t, ctx)
	var recoveredRows opensplunk.GetSearchResultsResponse
	fixture.post(t, ctx, "/api/search/jobs/results", "", &opensplunk.GetSearchResultsRequest{SearchJobId: job}, &recoveredRows)
	if !recoveryDrillResultsEqual(retained.GetResultPage(), recoveredRows.GetResultPage()) {
		t.Fatal("retained immutable result changed across restore")
	}
	var recoveredPatterns opensplunk.ListSearchPatternsResponse
	recoveredReference := recoveredRows.GetResultPage().GetSnapshotRef()
	fixture.post(t, ctx, "/api/search/jobs/patterns/list", "", &opensplunk.ListSearchPatternsRequest{
		SearchJobId: job, SnapshotRef: recoveredReference,
		Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE,
	}, &recoveredPatterns)
	if recoveredPatterns.GetSnapshotRef() != recoveredReference ||
		recoveredPatterns.GetRetainedEventCount() != 3 || recoveredPatterns.GetEligibleEventCount() != 3 ||
		recoveredPatterns.GetExcludedEventCount() != 0 || !recoveredPatterns.GetSnapshotComplete() ||
		recoveredPatterns.GetRetainedTruncated() {
		t.Fatal("restored snapshot reference did not resolve the complete retained event relation")
	}
	freshJob := fixture.search(t, ctx, definition)
	var freshRows opensplunk.GetSearchResultsResponse
	fixture.post(t, ctx, "/api/search/jobs/results", "", &opensplunk.GetSearchResultsRequest{SearchJobId: freshJob}, &freshRows)
	if len(freshRows.GetResultPage().GetRows()) != 3 {
		t.Fatal("restored authoritative ClickHouse query lost events")
	}
	for index, row := range freshRows.GetResultPage().GetRows() {
		original := retained.GetResultPage().GetRows()[index]
		if len(row.GetCells()) != 1 || len(original.GetCells()) != 1 || !proto.Equal(row.GetCells()[0], original.GetCells()[0]) {
			t.Fatal("restored authoritative event bytes differ from the source retained result")
		}
	}
	var history opensplunk.GetSearchHistoryEntryResponse
	fixture.post(t, ctx, "/api/search/history/get", "", &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: "recovery-pending"}, &history)
	if history.GetHistoryEntry().GetFinalState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED {
		t.Fatal("restored pending attempt was not Interrupted")
	}
	t.Log("verified recovered administrator and HEC authentication, catalog identities, retained rows, authoritative event query, and Interrupted pending attempt")
	connection := fixture.connection(t, ctx)
	var recoverySetID string
	if err := connection.QueryRow(ctx, "SELECT any(recovery_set_id) FROM open_splunk.recovery_sets").Scan(&recoverySetID); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	archiveName := recoverySetID + deploymentRecoveryArchiveSuffix
	fixture.compose(t, ctx, "stop", "server", "clickhouse")
	for range 2 {
		fixture.compose(t, ctx, "run", "--rm", "delete-recovery-archive", "delete-deployment-recovery-archive",
			"-archive-root", nativeRecoveryIntegrationArchiveRoot, "-archive-name", archiveName, "-confirm-archive-name", archiveName)
	}
	t.Log("deleted only the retired drill archive with exact confirmation; absent retry succeeded")
}

const recoveryDrillSet = "/var/lib/open-splunk/recovery/private/rehearsal-001"

const recoveryDrillAdministratorSeedMode os.FileMode = 0o444

func TestRecoveryDrillAdministratorSeedProvisioningContract(t *testing.T) {
	t.Parallel()
	token := nativeRecoveryIntegrationRandomHex(t, 32)
	private := filepath.Join(t.TempDir(), "administrator")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(private, "seed")
	if err := os.WriteFile(source, []byte(token), recoveryDrillAdministratorSeedMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(source, recoveryDrillAdministratorSeedMode); err != nil {
		t.Fatal(err)
	}
	assertRecoverySeedProvisioning(t, source, token)
}

func TestPreparedRecoveryAdministratorSeedProvisioningContract(t *testing.T) {
	t.Parallel()
	identity, err := testsupport.WriteServerTLSIdentity(filepath.Join(t.TempDir(), "tls"), "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "config")
	// Substitute only host root privileges and chown. The real operator script
	// still verifies certificates, creates exclusive files, and applies modes.
	const launcher = `import os, runpy, sys
os.geteuid = lambda: 0
ownership = {}
os.chown = lambda path, uid, gid: ownership.__setitem__(os.fspath(path), (uid, gid))
script = sys.argv[1]
sys.argv = sys.argv[1:]
directory = sys.argv[sys.argv.index('--directory') + 1]
runpy.run_path(script, run_name="__main__")
for path in (os.path.join(directory, 'administrator'), os.path.join(directory, 'administrator', 'seed')):
    if ownership.get(path) != (65532, 65532):
        raise RuntimeError('administrator seed and private parent must belong to the server UID/GID')
`
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", "-c", launcher,
		"../../deploy/recovery/prepare-config.py", "--directory", config,
		"--ca-cert", identity.CertificateFile, "--server-cert", identity.CertificateFile,
		"--server-key", identity.PrivateKeyFile)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("prepare operator recovery configuration: %v\n%s", err, output)
	}
	source := filepath.Join(config, "administrator", "seed")
	token, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(token)
	assertRecoverySeedProvisioning(t, source, string(token))
}

func assertRecoverySeedProvisioning(t *testing.T, source, token string) {
	t.Helper()
	parent, err := os.Lstat(filepath.Dir(source))
	if err != nil {
		t.Fatal(err)
	}
	if !parent.IsDir() || parent.Mode().Perm() != 0o700 {
		t.Fatalf("administrator seed must have a private 0700 parent, got %v", parent.Mode())
	}
	destination := filepath.Join(secureProvisioningDirectory(t), "administrator.token")
	if err := runProvisionAdministratorTokenSubcommand([]string{"-source", source, "-destination", destination}); err != nil {
		t.Fatalf("provision drill administrator seed: %v", err)
	}
	credential, err := readAdministratorToken(destination)
	if err != nil {
		t.Fatalf("load provisioned drill runtime credential: %v", err)
	}
	defer clear(credential)
	if string(credential) != token {
		t.Fatal("provisioned runtime credential differs from drill seed")
	}
	info, err := os.Lstat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("provisioned runtime credential mode = %#o, want 0600", info.Mode().Perm())
	}
}

func recoveryDrillControlFlags() []string {
	return []string{"-control-db", "/var/lib/open-splunk/state/private/open-splunk.db", "-master-key", "/var/lib/open-splunk/state/private/master.key",
		"-administrator-token-file", "/var/lib/open-splunk/state/private/administrator.token", "-search-artifact-directory", "/var/lib/open-splunk/state/private/search-artifacts"}
}

func recoveryDrillConnectionFlags(role string) []string {
	return []string{"-address", "clickhouse:9440", "-password-file", "/run/recovery/" + role + ".password", "-ca-cert", "/run/recovery/ca.crt", "-server-name", "clickhouse"}
}

type recoveryDrill struct {
	project, repository, work, overlay, administrator, operatorPassword, baseURL string
	environment, children                                                        []string
	client                                                                       *http.Client
	tls                                                                          *tls.Config
	restore                                                                      bool
	diagnosticSecrets                                                            []string
	lastReadiness                                                                string
}

func newRecoveryDrill(t *testing.T, ctx context.Context, image, helper string) *recoveryDrill {
	t.Helper()
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	fixture := &recoveryDrill{project: "recovery-drill-" + nativeRecoveryIntegrationRandomHex(t, 6), repository: repository, work: t.TempDir(),
		administrator: nativeRecoveryIntegrationRandomHex(t, 32), operatorPassword: nativeRecoveryIntegrationRandomHex(t, 32)}
	fixture.diagnosticSecrets = []string{fixture.administrator}
	config := filepath.Join(fixture.work, "config")
	identity, err := testsupport.WriteServerTLSIdentity(config, "clickhouse", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(config, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(config, "administrator"), 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.tls = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: identity.RootCAs, ServerName: "clickhouse"}
	fixture.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: fixture.tls.Clone()}}
	template, err := os.ReadFile(filepath.Join(repository, "deploy", "recovery", "users.xml.template"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(template)
	for role, password := range map[string]string{"operator": fixture.operatorPassword, "backup": nativeRecoveryIntegrationRandomHex(t, 32), "restore": nativeRecoveryIntegrationRandomHex(t, 32)} {
		digest := sha256.Sum256([]byte(password))
		fixture.diagnosticSecrets = append(fixture.diagnosticSecrets, password, hex.EncodeToString(digest[:]))
		contents = strings.ReplaceAll(contents, "@"+strings.ToUpper(role)+"_SHA256@", hex.EncodeToString(digest[:]))
		recoveryDrillWrite(t, filepath.Join(config, role+".password"), []byte(password))
	}
	certificate, err := os.ReadFile(identity.CertificateFile)
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(identity.PrivateKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"users.xml": []byte(contents), "ca.crt": certificate, "server.crt": certificate,
		"server.key": key, "http.key": key, "administrator/seed": []byte(fixture.administrator)} {
		recoveryDrillWrite(t, filepath.Join(config, name), data)
	}
	fixture.overlay = filepath.Join(fixture.work, "drill.yaml")
	overlay := fmt.Sprintf(`services:
  server:
    environment:
      OPEN_SPLUNK_SERVER_HEC_ENABLED: "true"
      OPEN_SPLUNK_SERVER_HTTP_TLS_CERTIFICATE_FILE: /run/recovery/server.crt
      OPEN_SPLUNK_SERVER_HTTP_TLS_PRIVATE_KEY_FILE: /run/recovery/http.key
    healthcheck:
      test: [CMD, /usr/local/bin/open-splunk-server, healthcheck, -url, "https://127.0.0.1:8080/readyz", -ca-cert, /run/recovery/ca.crt, -server-name, clickhouse]
  clickhouse:
    ports: ["127.0.0.1:0:9440"]
  recovery:
    volumes:
      - %q
`, helper+":/run/drill/helper:ro")
	recoveryDrillWrite(t, fixture.overlay, []byte(overlay))
	fixture.environment = append(os.Environ(), "OPEN_SPLUNK_DEPLOY_SERVER_IMAGE="+image, "OPEN_SPLUNK_RECOVERY_CONFIG_DIRECTORY="+config,
		"OPEN_SPLUNK_DEPLOY_HTTP_PORT=0", "OPEN_SPLUNK_RECOVERY_TARGET_STATE_VOLUME="+fixture.project+"-restored-state",
		"OPEN_SPLUNK_RECOVERY_TARGET_CLICKHOUSE_VOLUME="+fixture.project+"-restored-clickhouse")
	t.Cleanup(func() { fixture.close(t) })
	// Docker performs numeric ownership changes on files this fixture just created.
	initializer := fixture.project + "-config"
	fixture.children = append(fixture.children, initializer)
	fixture.docker(t, ctx, "run", "--rm", "--name", initializer, "--user", "0:0", "--volume", config+":/config", "--entrypoint", "sh", testsupport.DefaultClickHouseImage, "-c",
		"chown 65532:65532 /config/*.password /config/http.key && chmod 400 /config/*.password /config/http.key && chown 101:101 /config/server.key && chmod 400 /config/server.key && chmod 444 /config/*.crt /config/users.xml && "+recoveryDrillAdministratorSeedInitializationCommand())
	return fixture
}

func recoveryDrillAdministratorSeedInitializationCommand() string {
	return fmt.Sprintf("chown 65532:65532 /config/administrator /config/administrator/seed && chmod 700 /config/administrator && chmod %o /config/administrator/seed", recoveryDrillAdministratorSeedMode)
}

const recoveryDrillAdministratorSeedCleanupCommand = "rm -f /config/administrator/seed && rmdir /config/administrator"

func recoveryDrillWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (fixture *recoveryDrill) composeArguments(arguments ...string) []string {
	result := []string{"compose", "--project-name", fixture.project, "-f", filepath.Join(fixture.repository, "deploy", "docker-compose.recovery.yaml")}
	if fixture.restore {
		result = append(result, "-f", filepath.Join(fixture.repository, "deploy", "docker-compose.recovery-restore.yaml"))
	}
	return append(append(result, "-f", fixture.overlay), arguments...)
}

func (fixture *recoveryDrill) compose(t *testing.T, ctx context.Context, arguments ...string) string {
	t.Helper()
	return fixture.docker(t, ctx, fixture.composeArguments(arguments...)...)
}

func (fixture *recoveryDrill) docker(t *testing.T, ctx context.Context, arguments ...string) string {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Env = fixture.environment
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("recovery drill docker %s: %v\n%s", arguments[0], err, nativeRecoveryIntegrationBoundedOutput(output))
	}
	return string(output)
}

func (fixture *recoveryDrill) wait(t *testing.T, ctx context.Context, label string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			fixture.reportDiagnostics(t)
			t.Fatalf("wait for %s: %v", label, ctx.Err())
		case <-deadline.C:
			fixture.reportDiagnostics(t)
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func (fixture *recoveryDrill) waitReady(t *testing.T, ctx context.Context) {
	t.Helper()
	fixture.wait(t, ctx, "server readiness", func() bool {
		return fixture.readinessAttempt(ctx, func(ctx context.Context) (string, error) {
			command := exec.CommandContext(ctx, "docker", fixture.composeArguments("port", "server", "8080")...)
			command.Env = fixture.environment
			output, err := command.Output()
			return strings.TrimSpace(string(output)), err
		})
	})
}

func (fixture *recoveryDrill) readinessAttempt(ctx context.Context, resolve func(context.Context) (string, error)) bool {
	// Docker can assign a different ephemeral host port when startup retries
	// restart the server. Only a verified ready endpoint becomes the API base.
	address, err := resolve(ctx)
	if err != nil {
		fixture.lastReadiness = "resolve server port: " + err.Error()
		return false
	}
	baseURL := "https://" + address
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/readyz", nil)
	if err != nil {
		fixture.lastReadiness = "create readiness request: " + err.Error()
		return false
	}
	response, err := fixture.client.Do(request)
	if err != nil {
		fixture.lastReadiness = "readiness request: " + err.Error()
		return false
	}
	defer response.Body.Close()
	fixture.lastReadiness = fmt.Sprintf("GET %s/readyz: HTTP %d", baseURL, response.StatusCode)
	if response.StatusCode != http.StatusOK {
		return false
	}
	fixture.baseURL = baseURL
	return true
}

func (fixture *recoveryDrill) connection(t *testing.T, ctx context.Context) clickhousedriver.Conn {
	t.Helper()
	address := strings.TrimSpace(fixture.compose(t, ctx, "port", "clickhouse", "9440"))
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{Addr: []string{address}, Protocol: clickhousedriver.Native,
		Auth: clickhousedriver.Auth{Database: "default", Username: "open_splunk_operator", Password: fixture.operatorPassword},
		TLS:  fixture.tls.Clone(), DialTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func (fixture *recoveryDrill) waitClickHouse(t *testing.T, ctx context.Context) {
	t.Helper()
	connection := fixture.connection(t, ctx)
	defer connection.Close()
	fixture.wait(t, ctx, "ClickHouse readiness", func() bool { return connection.Ping(ctx) == nil })
}

func (fixture *recoveryDrill) waitEvents(t *testing.T, ctx context.Context, count uint64) {
	t.Helper()
	connection := fixture.connection(t, ctx)
	defer connection.Close()
	fixture.wait(t, ctx, "durable ingested events", func() bool {
		var got uint64
		return connection.QueryRow(ctx, "SELECT count() FROM open_splunk.events WHERE index_name = 'recovery-drill'").Scan(&got) == nil && got == count
	})
}

func (fixture *recoveryDrill) restoreIdentity(t *testing.T, ctx context.Context) string {
	t.Helper()
	connection := fixture.connection(t, ctx)
	defer connection.Close()
	var operations, receipts uint64
	var databaseID, digest string
	if err := connection.QueryRow(ctx, "SELECT count() FROM system.backups WHERE status = 'RESTORED'").Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if operations != 1 {
		t.Fatalf("native restore count = %d, want exactly one", operations)
	}
	if err := connection.QueryRow(ctx, "SELECT count(), any(toString(database_uuid)), any(deployment_manifest_sha256) FROM open_splunk.recovery_sets").Scan(&receipts, &databaseID, &digest); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || len(digest) != 64 {
		t.Fatal("canonical receipt missing or invalid")
	}
	return fmt.Sprintf("%d/%s/%s", operations, databaseID, digest)
}

func (fixture *recoveryDrill) child(t *testing.T, ctx context.Context, mode string) {
	t.Helper()
	fixture.compose(t, ctx, "run", "--rm", "--no-deps", "-e", "OPEN_SPLUNK_RECOVERY_DRILL_CHILD="+mode,
		"--entrypoint", "/run/drill/helper", "recovery", "-test.run=^TestDeploymentRecoveryDrillChild$", "-test.v")
}

func (fixture *recoveryDrill) post(t *testing.T, ctx context.Context, path, token string, input, output proto.Message) {
	t.Helper()
	body, err := proto.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fixture.baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := fixture.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err = io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s returned %d", path, response.StatusCode)
	}
	if err := proto.Unmarshal(body, output); err != nil {
		t.Fatal(err)
	}
}

func (fixture *recoveryDrill) hec(t *testing.T, ctx context.Context, path, token, body string) {
	t.Helper()
	method := http.MethodPost
	if body == "" {
		method = http.MethodGet
	}
	request, err := http.NewRequestWithContext(ctx, method, fixture.baseURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Splunk "+token)
	request.Header.Set("Content-Type", "application/json")
	if body != "" {
		request.Header.Set("X-Splunk-Request-Channel", recoveryDrillHECChannel)
	}
	response, err := fixture.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Code  int    `json:"code"`
		AckID *int64 `json:"ackId"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	expectedCode := 0
	if body == "" {
		expectedCode = 17
	}
	if response.StatusCode != http.StatusOK || result.Code != expectedCode {
		t.Fatalf("HEC %s returned status=%d code=%d", path, response.StatusCode, result.Code)
	}
	if body != "" {
		if result.AckID == nil {
			t.Fatal("HEC accepted event without durable acknowledgment identity")
		}
		fixture.waitHECAcknowledgment(t, ctx, token, *result.AckID)
	}
}

func (fixture *recoveryDrill) search(t *testing.T, ctx context.Context, definition *opensplunk.SearchDefinition) string {
	t.Helper()
	var created opensplunk.CreateSearchJobResponse
	fixture.post(t, ctx, "/api/search/jobs/create", "", &opensplunk.CreateSearchJobRequest{Definition: definition,
		Source: &opensplunk.SearchJobSource{Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC}}, &created)
	job := created.GetSearchJob().GetSearchJobId()
	fixture.wait(t, ctx, "terminal search", func() bool {
		var got opensplunk.GetSearchJobResponse
		fixture.post(t, ctx, "/api/search/jobs/get", "", &opensplunk.GetSearchJobRequest{SearchJobId: job}, &got)
		state := got.GetSearchJob().GetState()
		if state >= opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED {
			t.Fatalf("search ended in %s", state)
		}
		return state == opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED
	})
	return job
}

func (fixture *recoveryDrill) rejectAdministrator(t *testing.T, ctx context.Context) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fixture.baseURL+"/api/indexes/list", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 64))
	response, err := fixture.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong administrator token returned %d", response.StatusCode)
	}
}

func (fixture *recoveryDrill) close(t *testing.T) {
	t.Helper()
	fixture.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, name := range fixture.children {
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", name).CombinedOutput()
		if err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove owned recovery child %s: %v", name, err)
		}
	}
	command := exec.CommandContext(ctx, "docker", fixture.composeArguments("down", "--volumes", "--remove-orphans")...)
	command.Env = fixture.environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Errorf("remove recovery project: %v\n%s", err, output)
	}
	// Restore changes two volume names; their original source volumes remain
	// outside the active Compose model and are removed explicitly by exact name.
	for _, name := range []string{fixture.project + "_server-state", fixture.project + "_clickhouse-data", fixture.project + "-restored-state", fixture.project + "-restored-clickhouse"} {
		output, err := exec.CommandContext(ctx, "docker", "volume", "rm", name).CombinedOutput()
		if err != nil && !strings.Contains(strings.ToLower(string(output)), "no such volume") {
			t.Errorf("remove owned volume %s: %v", name, err)
		}
	}
	// The container-owned private parent is intentionally inaccessible to the
	// host test user. Remove only its seed and empty directory before TempDir's
	// cleanup; never make the administrator credential more broadly readable.
	command = exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "none", "--user", "0:0",
		"--volume", filepath.Join(fixture.work, "config")+":/config", "--entrypoint", "sh",
		testsupport.DefaultClickHouseImage, "-ec", recoveryDrillAdministratorSeedCleanupCommand)
	if output, err := command.CombinedOutput(); err != nil {
		t.Errorf("remove owned private administrator seed: %v\n%s", err, output)
	}
}

const recoveryDrillHECChannel = "c1f7dbba-8c6c-48c7-9ebd-df7f0cb73418"

func (fixture *recoveryDrill) waitHECAcknowledgment(t *testing.T, ctx context.Context, token string, ackID int64) {
	t.Helper()
	fixture.wait(t, ctx, "HEC durable publication", func() bool {
		body := fmt.Sprintf(`{"acks":[%d]}`, ackID)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, fixture.baseURL+"/services/collector/ack", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Splunk "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Splunk-Request-Channel", recoveryDrillHECChannel)
		response, err := fixture.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result struct {
			Acks map[string]bool `json:"acks"`
		}
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("HEC acknowledgment returned %d", response.StatusCode)
		}
		return result.Acks[fmt.Sprint(ackID)]
	})
}
