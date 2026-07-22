package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// validYAML is a minimal, fully-valid config used as a base for mutation tests.
const validYAML = `
server:
  address: 127.0.0.1:8443
  transport: grpc
  token_file: ./collector.token
  tls:
    enabled: false
state:
  directory: ./data/collector
  max_queue_bytes: 1GiB
inputs:
  - id: app
    type: file
    include:
      - /var/log/app/*.log
    format: ndjson
    start_at: end
`

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := writeFile(t, dir, "c.yaml", validYAML)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Address != "127.0.0.1:8443" {
		t.Errorf("address = %q", cfg.Server.Address)
	}
	if cfg.State.MaxQueueBytes != 1<<30 {
		t.Errorf("max_queue_bytes = %d, want %d", cfg.State.MaxQueueBytes, 1<<30)
	}
	if len(cfg.Inputs) != 1 || cfg.Inputs[0].ID != "app" {
		t.Fatalf("inputs = %+v", cfg.Inputs)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	const y = `
server:
  address: h:1
  token_file: ./t
state:
  directory: ./d
inputs:
  - id: app
    include: [/var/log/*.log]
`
	dir := t.TempDir()
	cfg, err := Load(writeFile(t, dir, "c.yaml", y))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Transport != "grpc" {
		t.Errorf("transport default = %q", cfg.Server.Transport)
	}
	in := cfg.Inputs[0]
	if in.Type != "file" || in.Format != "ndjson" || in.StartAt != "end" {
		t.Errorf("input defaults = type:%q format:%q start_at:%q", in.Type, in.Format, in.StartAt)
	}
}

func TestLoadUnknownFieldRejected(t *testing.T) {
	t.Parallel()
	y := validYAML + "\nnot_a_real_key: 1\n"
	dir := t.TempDir()
	_, err := Load(writeFile(t, dir, "c.yaml", y))
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
	if !strings.Contains(err.Error(), "not_a_real_key") {
		t.Errorf("error should name the unknown field: %v", err)
	}
}

func TestLoadUnknownNestedFieldRejected(t *testing.T) {
	t.Parallel()
	y := strings.Replace(validYAML, "    enabled: false", "    enabled: false\n    bogus: 1", 1)
	dir := t.TempDir()
	_, err := Load(writeFile(t, dir, "c.yaml", y))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("expected nested unknown-field error, got: %v", err)
	}
}

func TestEnvSubstitution(t *testing.T) {
	// No t.Parallel: t.Setenv is incompatible with parallel tests.
	t.Setenv("OS_ADDR", "example.com:9000")
	const y = `
server:
  address: ${OS_ADDR}
  token_file: ./t
state:
  directory: ./d
inputs:
  - id: app
    include: [/var/log/*.log]
`
	dir := t.TempDir()
	cfg, err := Load(writeFile(t, dir, "c.yaml", y))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Address != "example.com:9000" {
		t.Errorf("address = %q", cfg.Server.Address)
	}
}

func TestEnvUndefinedIsError(t *testing.T) {
	t.Parallel()
	const y = `
server:
  address: ${OS_DEFINITELY_UNDEFINED_VAR}
  token_file: ./t
state:
  directory: ./d
inputs:
  - id: app
    include: [/var/log/*.log]
`
	dir := t.TempDir()
	_, err := Load(writeFile(t, dir, "c.yaml", y))
	if err == nil || !strings.Contains(err.Error(), "OS_DEFINITELY_UNDEFINED_VAR") {
		t.Fatalf("expected undefined-var error naming the var, got: %v", err)
	}
}

func TestEnvDollarEscape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"escape", "a$$b", "a$b"},
		{"escaped brace", "$${NOT_A_VAR}", "${NOT_A_VAR}"},
		{"bare dollar", "cost is $5", "cost is $5"},
		{"regex anchor", `ERROR$`, `ERROR$`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := substituteEnv([]byte(tc.in))
			if err != nil {
				t.Fatalf("substituteEnv: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	base := func() *Config {
		return &Config{
			Server: ServerConfig{Address: "h:1", Transport: "grpc", TokenFile: "./t"},
			State:  StateConfig{Directory: "./d"},
			Inputs: []InputConfig{{
				ID: "app", Type: "file", Include: []string{"/var/log/*.log"},
				Format: "ndjson", StartAt: "end",
			}},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantSub string // "" means expect success
	}{
		{"valid", func(*Config) {}, ""},
		{"no address", func(c *Config) { c.Server.Address = "" }, "server.address"},
		{"bad transport", func(c *Config) { c.Server.Transport = "http" }, "server.transport"},
		{"no token_file", func(c *Config) { c.Server.TokenFile = "" }, "server.token_file"},
		{"tls incoherent", func(c *Config) { c.Server.TLS.CAFile = "/ca.pem" }, "server.tls"},
		{"no directory", func(c *Config) { c.State.Directory = "" }, "state.directory"},
		{"no inputs", func(c *Config) { c.Inputs = nil }, "at least one input"},
		{"no id", func(c *Config) { c.Inputs[0].ID = "" }, "inputs[0].id"},
		{"dup id", func(c *Config) { c.Inputs = append(c.Inputs, c.Inputs[0]) }, "duplicate input id"},
		{"bad type", func(c *Config) { c.Inputs[0].Type = "socket" }, "type must be"},
		{"bad format", func(c *Config) { c.Inputs[0].Format = "csv" }, "format must be"},
		{"bad start_at", func(c *Config) { c.Inputs[0].StartAt = "middle" }, "start_at must be"},
		{"no include", func(c *Config) { c.Inputs[0].Include = nil }, "at least one include glob"},
		{"bad include glob", func(c *Config) { c.Inputs[0].Include = []string{"[bad"} }, "invalid include glob"},
		{"bad exclude glob", func(c *Config) { c.Inputs[0].Exclude = []string{"[bad"} }, "invalid exclude glob"},
		{"multiline empty pattern", func(c *Config) {
			c.Inputs[0].Multiline = &MultilineConfig{}
		}, "line_start_pattern is required"},
		{"multiline bad pattern", func(c *Config) {
			c.Inputs[0].Multiline = &MultilineConfig{LineStartPattern: "("}
		}, "line_start_pattern"},
		{"multiline ok", func(c *Config) {
			c.Inputs[0].Multiline = &MultilineConfig{LineStartPattern: `^\d{4}`}
		}, ""},
		{"allow no fields", func(c *Config) {
			c.Processors = []ProcessorConfig{{Type: "allow"}}
		}, "at least one field"},
		{"rename no from", func(c *Config) {
			c.Processors = []ProcessorConfig{{Type: "rename", To: "x"}}
		}, "from is required"},
		{"rename no to", func(c *Config) {
			c.Processors = []ProcessorConfig{{Type: "rename", From: "x"}}
		}, "to is required"},
		{"unknown processor", func(c *Config) {
			c.Processors = []ProcessorConfig{{Type: "wat"}}
		}, "unknown type"},
		{"empty processor type", func(c *Config) {
			c.Processors = []ProcessorConfig{{}}
		}, "type is required"},
		{"redact ok", func(c *Config) {
			c.Processors = []ProcessorConfig{{Type: "redact", Fields: []string{"pw"}}}
		}, ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := base()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantSub == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestByteSizeUnmarshal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    ByteSize
		wantErr bool
	}{
		{"10GiB", 10 * (1 << 30), false},
		{"10GB", 10 * 1000 * 1000 * 1000, false},
		{"512MB", 512 * 1000 * 1000, false},
		{"1KiB", 1024, false},
		{"1024", 1024, false},
		{"0", 0, false},
		{"64B", 64, false},
		{"1.5GiB", ByteSize(1.5 * float64(1<<30)), false},
		{"5mib", 5 << 20, false}, // case-insensitive unit
		{"-5", 0, true},
		{"-5GiB", 0, true},
		{"1.5", 0, true},            // fractional without unit
		{"12ZB", 0, true},           // unknown unit
		{"99999999999GiB", 0, true}, // overflow (~1e20 > 2^64)
		{"", 0, false},              // empty -> zero
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			var bs ByteSize
			err := yaml.Unmarshal([]byte(tc.in), &bs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %d", tc.in, bs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.in, err)
			}
			if bs != tc.want {
				t.Errorf("%q = %d, want %d", tc.in, bs, tc.want)
			}
		})
	}
}

func TestDurationUnmarshal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    Duration
		wantErr bool
	}{
		{"5s", Duration(5 * time.Second), false},
		{"250ms", Duration(250 * time.Millisecond), false},
		{"0", 0, false},
		{"1h30m", Duration(90 * time.Minute), false},
		{"5", 0, true},   // bare int rejected
		{"abc", 0, true}, // garbage
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			var d Duration
			err := yaml.Unmarshal([]byte(tc.in), &d)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %v", tc.in, d)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.in, err)
			}
			if d != tc.want {
				t.Errorf("%q = %v, want %v", tc.in, d, tc.want)
			}
		})
	}
}

func TestLoadToken(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	t.Run("trims trailing newline", func(t *testing.T) {
		t.Parallel()
		p := writeFile(t, dir, "tok_nl", "secret-abc\n")
		got, err := LoadToken(p)
		if err != nil {
			t.Fatal(err)
		}
		if got != "secret-abc" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("trims crlf", func(t *testing.T) {
		t.Parallel()
		p := writeFile(t, dir, "tok_crlf", "secret-abc\r\n")
		got, err := LoadToken(p)
		if err != nil {
			t.Fatal(err)
		}
		if got != "secret-abc" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("empty errors", func(t *testing.T) {
		t.Parallel()
		p := writeFile(t, dir, "tok_empty", "\n")
		_, err := LoadToken(p)
		if err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("want empty error, got %v", err)
		}
	})
	t.Run("too large errors", func(t *testing.T) {
		t.Parallel()
		p := writeFile(t, dir, "tok_big", strings.Repeat("x", maxTokenBytes+1))
		_, err := LoadToken(p)
		if err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("want size error, got %v", err)
		}
	})
	t.Run("missing file errors with path", func(t *testing.T) {
		t.Parallel()
		p := filepath.Join(dir, "does-not-exist")
		_, err := LoadToken(p)
		if err == nil || !strings.Contains(err.Error(), p) {
			t.Fatalf("want path in error, got %v", err)
		}
	})
}

func TestLoadTokenErrorNeverLeaksContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Oversized file whose content is a secret; error must not include it.
	secret := strings.Repeat("SUPERSECRET", 1000)
	p := writeFile(t, dir, "tok_secret_big", secret)
	_, err := LoadToken(p)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "SUPERSECRET") {
		t.Fatalf("error leaked token content: %v", err)
	}
}

// TestSecretNeverInDiagnostics plants a secret in a token file and asserts that
// neither String() nor a Validate() error ever contains the secret value.
func TestSecretNeverInDiagnostics(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const secret = "PLANTED-TOKEN-9f8a7b6c-DO-NOT-LEAK"
	tokenPath := writeFile(t, dir, "collector.token", secret+"\n")

	cfg := &Config{
		Server: ServerConfig{Address: "h:1", Transport: "grpc", TokenFile: tokenPath},
		State:  StateConfig{Directory: "./d", MaxQueueBytes: 1 << 30},
		Inputs: []InputConfig{{
			ID: "app", Type: "file", Include: []string{"/var/log/*.log"},
			Format: "csv", StartAt: "end", // invalid on purpose -> Validate error
		}},
	}

	s := cfg.String()
	if strings.Contains(s, secret) {
		t.Fatalf("String() leaked the token value:\n%s", s)
	}
	if !strings.Contains(s, tokenPath) {
		t.Errorf("String() should include the token_file path")
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Validate() error leaked the token value: %v", err)
	}

	// Sanity: LoadToken does read the real value (proving the secret was live).
	got, lerr := LoadToken(tokenPath)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if got != secret {
		t.Fatalf("LoadToken returned %q", got)
	}
}

func TestStringDoesNotReadTokenFile(t *testing.T) {
	t.Parallel()
	// Point token_file at a nonexistent path; String must not error/panic and
	// must include the path verbatim.
	missing := filepath.Join(t.TempDir(), "nope.token")
	cfg := &Config{Server: ServerConfig{Address: "h:1", TokenFile: missing}}
	s := cfg.String()
	if !strings.Contains(s, missing) {
		t.Errorf("String() should include token_file path %q:\n%s", missing, s)
	}
}

// TestRoundTripExample loads the committed example config unmodified.
func TestRoundTripExample(t *testing.T) {
	t.Parallel()
	path := filepath.FromSlash("../../../configs/examples/collector.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("example config not found at %s: %v", path, err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load example: %v", err)
	}
	if len(cfg.Inputs) == 0 {
		t.Fatal("example should define inputs")
	}
	if cfg.State.MaxQueueBytes != 1<<30 {
		t.Errorf("example max_queue_bytes = %d, want 1GiB", cfg.State.MaxQueueBytes)
	}
}
