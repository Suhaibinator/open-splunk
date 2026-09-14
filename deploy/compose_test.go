package deploy_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type composeWorkspaceConfig struct {
	Services map[string]struct {
		Environment map[string]string `yaml:"environment"`
		NetworkMode string            `yaml:"network_mode"`
		Ports       []struct {
			HostIP    string `yaml:"host_ip"`
			Published string `yaml:"published"`
			Target    uint16 `yaml:"target"`
			Protocol  string `yaml:"protocol"`
		} `yaml:"ports"`
	} `yaml:"services"`
}

func workspaceComposeSources(t *testing.T) map[string][]byte {
	t.Helper()
	compose, err := os.ReadFile("docker-compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	documentation, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, examples, found := strings.Cut(string(documentation), "For an existing Compose project, paste")
	if !found {
		t.Fatal("standalone deployment instructions are missing")
	}
	_, example, found := strings.Cut(examples, "```yaml\n")
	if !found {
		t.Fatal("standalone deployment example is missing")
	}
	example, _, found = strings.Cut(example, "\n```")
	if !found {
		t.Fatal("standalone deployment example is unterminated")
	}
	return map[string][]byte{
		"compose": compose,
		// The README service joins the surrounding project's existing network.
		"example": []byte(example + "\nnetworks:\n  per-obs-network:\n    driver: bridge\n"),
	}
}

func assertWorkspacePublication(t *testing.T, source []byte, published string) {
	t.Helper()
	var config composeWorkspaceConfig
	if err := yaml.Unmarshal(source, &config); err != nil {
		t.Fatalf("decode Compose configuration: %v", err)
	}
	server, found := config.Services["server"]
	if !found {
		t.Fatal("server service is missing")
	}
	if server.NetworkMode != "" {
		t.Fatalf("server must not bypass port publication with network_mode %q", server.NetworkMode)
	}
	if server.Environment["OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS"] != "0.0.0.0:8080" {
		t.Fatal("container listener must remain reachable by Docker publication")
	}
	if len(server.Ports) != 1 {
		t.Fatalf("server has %d published ports, want one", len(server.Ports))
	}
	port := server.Ports[0]
	if port.HostIP != "127.0.0.1" || port.Target != 8080 ||
		port.Published != published || port.Protocol != "tcp" {
		t.Fatalf("workspace publication = %+v, want loopback TCP %s -> 8080", port, published)
	}
}

func TestWorkspaceComposePublication(t *testing.T) {
	for name, source := range workspaceComposeSources(t) {
		t.Run(name, func(t *testing.T) {
			assertWorkspacePublication(t, source, "${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}")
		})
	}
}

func TestRenderedWorkspaceComposePublication(t *testing.T) {
	compose, err := exec.LookPath("docker-compose")
	var prefix []string
	if err != nil {
		compose, err = exec.LookPath("docker")
		prefix = []string{"compose"}
	}
	if err != nil {
		t.Skip("Docker Compose is unavailable; install it to run rendered publication contracts")
	}
	versionArguments := append(append([]string{}, prefix...), "version", "--short")
	if output, err := exec.CommandContext(t.Context(), compose, versionArguments...).CombinedOutput(); err != nil {
		t.Skipf("Docker Compose is unavailable: %v\n%s", err, output)
	}
	for name, source := range workspaceComposeSources(t) {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "compose.yaml")
			if err := os.WriteFile(file, source, 0o600); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name      string
				port      string
				published string
			}{
				{name: "default", published: "8080"},
				{name: "empty", port: "", published: "8080"},
				{name: "custom", port: "18080", published: "18080"},
				// The rendered port string must remain separate from the host
				// binding, including when it contains an address.
				{name: "address cannot change binding", port: "0.0.0.0:18080", published: "0.0.0.0:18080"},
				{name: "IPv6 address cannot change binding", port: "[::]:18080", published: "[::]:18080"},
			} {
				t.Run(test.name, func(t *testing.T) {
					arguments := append(append([]string{}, prefix...), "--env-file", os.DevNull, "-f", file, "config")
					command := exec.CommandContext(t.Context(), compose, arguments...)
					var stderr bytes.Buffer
					command.Stderr = &stderr
					command.Env = []string{
						"PATH=" + os.Getenv("PATH"),
						"OPEN_SPLUNK_DEPLOY_SERVER_IMAGE=open-splunk-test:local",
						"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=test-administrator-token",
						"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD=test-clickhouse-password",
						"OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS=localhost,127.0.0.1",
					}
					if test.name != "default" {
						command.Env = append(command.Env, "OPEN_SPLUNK_DEPLOY_HTTP_PORT="+test.port)
					}
					output, err := command.Output()
					if err != nil {
						t.Fatalf("render Compose: %v\n%s", err, stderr.String())
					}
					assertWorkspacePublication(t, output, test.published)
				})
			}
		})
	}
}
