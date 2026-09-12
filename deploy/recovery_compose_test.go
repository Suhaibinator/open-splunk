package deploy_test

import (
	"encoding/xml"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type recoveryCompose struct {
	Services map[string]struct {
		Image       string            `yaml:"image"`
		User        string            `yaml:"user"`
		Environment map[string]string `yaml:"environment"`
		Volumes     []string          `yaml:"volumes"`
		Command     []string          `yaml:"command"`
		Ports       []string          `yaml:"ports"`
	} `yaml:"services"`
	Volumes map[string]struct {
		Name string `yaml:"name"`
	} `yaml:"volumes"`
}

func TestRecoveryComposeSharesPersistentLockAndVerifiesTLS(t *testing.T) {
	contents, err := os.ReadFile("docker-compose.recovery.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var config recoveryCompose
	if err := yaml.Unmarshal(contents, &config); err != nil {
		t.Fatal(err)
	}
	const lock = "/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock"
	for _, name := range []string{"server", "recovery"} {
		service := config.Services[name]
		if service.User != "65532:65532" || service.Environment["OPEN_SPLUNK_SERVER_LOCK_FILE"] != lock {
			t.Fatalf("%s must share the persistent lock and server UID", name)
		}
		for key, want := range map[string]string{
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_ENABLED":             "true",
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS":                 "clickhouse:9440",
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_CA_CERTIFICATE_FILE": "/run/recovery/ca.crt",
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_SERVER_NAME":         "clickhouse",
			"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE":           "/var/lib/open-splunk/state/private/administrator.token",
		} {
			if service.Environment[key] != want {
				t.Errorf("%s %s = %q, want %q", name, key, service.Environment[key], want)
			}
		}
		for _, key := range []string{"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN", "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD", "OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH"} {
			if _, exists := service.Environment[key]; exists {
				t.Errorf("%s contains incompatible credential/lock setting %s", name, key)
			}
		}
		for _, mount := range []string{"server-lock:/var/lib/open-splunk/lock", "server-state:/var/lib/open-splunk/state", "recovery-sets:/var/lib/open-splunk/recovery", "recovery-archives:/var/lib/open-splunk-clickhouse-backups:ro"} {
			if !containsMount(service.Volumes, mount) {
				t.Errorf("%s missing mount %s", name, mount)
			}
		}
	}
	if config.Services["delete-recovery-archive"].User != "101:65532" ||
		config.Services["prepare-recovery-volume"].User != "0:0" {
		t.Fatal("volume preparation/deletion must use their exact required UIDs")
	}
	clickhouse := config.Services["clickhouse"]
	if !strings.Contains(clickhouse.Image, ":26.7.5.10-alpine@sha256:") || len(clickhouse.Ports) != 0 {
		t.Fatal("ClickHouse must be pinned and unpublished")
	}
	contents, err = os.ReadFile("docker-compose.recovery-restore.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var restore recoveryCompose
	if err := yaml.Unmarshal(contents, &restore); err != nil {
		t.Fatal(err)
	}
	if !containsMount(restore.Services["clickhouse"].Volumes, "recovery-archives:/var/lib/open-splunk-clickhouse-backups:ro") {
		t.Fatal("ClickHouse and helper must use the same read-only restore archive")
	}
	for _, name := range []string{"server-state", "clickhouse-data"} {
		if !strings.Contains(restore.Volumes[name].Name, ":?name a new empty") {
			t.Errorf("restore %s must explicitly select a fresh target", name)
		}
	}
	if _, exists := restore.Volumes["server-lock"]; exists {
		t.Fatal("restore must preserve the lock volume")
	}
}

func TestRecoveryXMLGrantsMatchQualifiedPrincipals(t *testing.T) {
	contents, err := os.ReadFile("recovery/users.xml.template")
	if err != nil {
		t.Fatal(err)
	}
	var users struct {
		Backup struct {
			Queries []string `xml:"grants>query"`
		} `xml:"users>open_splunk_backup"`
		Restore struct {
			Queries []string `xml:"grants>query"`
		} `xml:"users>open_splunk_restore"`
	}
	if err := xml.Unmarshal(contents, &users); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("../cmd/open-splunk-server/deployment_recovery_clickhouse_integration_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for role, queries := range map[string][]string{"Backup": users.Backup.Queries, "Restore": users.Restore.Queries} {
		if len(queries) == 0 {
			t.Fatalf("%s grants missing", role)
		}
		want := 0
		for line := range strings.SplitSeq(string(fixture), "\n") {
			if strings.Contains(line, `"GRANT `) && strings.HasSuffix(strings.TrimSpace(line), "deploymentRecovery"+role+"Username,") {
				want++
			}
		}
		if len(queries) != want {
			t.Errorf("%s grant count = %d, want %d", role, len(queries), want)
		}
		for _, query := range queries {
			if !strings.Contains(string(fixture), `"`+query+` TO " + deploymentRecovery`+role+`Username`) {
				t.Errorf("%s has unqualified grant %q", role, query)
			}
		}
	}
}

func containsMount(mounts []string, want string) bool {
	return slices.Contains(mounts, want)
}

// The wrapper executes a compiled test binary directly; unlike `go test`, that
// does not switch into the package directory. Stub only external build/Docker
// tools and check the actual helper working directory through a subprocess.
func TestRecoveryDrillWrapperUsesPackageDirectory(t *testing.T) {
	repository := t.TempDir()
	for _, directory := range []string{"scripts", "cmd/open-splunk-server", "tools"} {
		if err := os.MkdirAll(filepath.Join(repository, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source, err := os.ReadFile("../scripts/test-deployment-recovery.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repository, "scripts", "test-deployment-recovery.sh")
	if err := os.WriteFile(script, source, 0o700); err != nil {
		t.Fatal(err)
	}
	const revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	stubs := map[string]string{
		"node":   "#!/bin/sh\nprintf 'source_revision=" + revision + "\\n'\n",
		"git":    "#!/bin/sh\nif [ \"$1\" = rev-parse ]; then printf '" + revision + "\\n'; fi\n",
		"docker": "#!/bin/sh\nif [ \"$1\" = image ]; then printf 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\n'; else printf 'source_revision=" + revision + "\\n'; fi\n",
		"go": `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then
    shift
    cat > "$1" <<'HELPER'
#!/bin/sh
if [ "$PWD" != "$RECOVERY_EXPECTED_CWD" ]; then
  echo 'compiled drill helper started outside its package directory' >&2
  exit 1
fi
HELPER
    chmod 700 "$1"
    exit 0
  fi
  shift
done
exit 1
`,
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(repository, "tools", name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "bash", script)
	command.Dir = repository
	command.Env = append(os.Environ(), "PATH="+filepath.Join(repository, "tools")+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OPEN_SPLUNK_DEPLOYMENT_RECOVERY_DRILL=1", "OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE=drill:local",
		"RECOVERY_EXPECTED_CWD="+filepath.Join(repository, "cmd", "open-splunk-server"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run recovery wrapper: %v\n%s", err, output)
	}
}
