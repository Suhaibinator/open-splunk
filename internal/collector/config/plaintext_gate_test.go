package config

import (
	"strings"
	"testing"
)

// TestPlaintextTokenGate covers the cleartext-token guard (FIX 7): plaintext to
// loopback is allowed, plaintext to a remote host is rejected, and the
// allow_insecure_remote override re-permits it.
func TestPlaintextTokenGate(t *testing.T) {
	t.Parallel()
	base := func() *Config {
		return &Config{
			Server: ServerConfig{Address: "127.0.0.1:8443", Transport: "grpc", TokenFile: "./t"},
			State:  StateConfig{Directory: "./d"},
			Inputs: []InputConfig{{
				ID: "app", Type: "file", Include: []string{"/var/log/*.log"},
				Format: "ndjson", StartAt: "end", Index: "main",
			}},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantSub string // "" means expect success
	}{
		{"loopback plaintext ok", func(*Config) {}, ""},
		{"loopback name plaintext ok", func(c *Config) { c.Server.Address = "localhost:8443" }, ""},
		{"remote plaintext rejected", func(c *Config) { c.Server.Address = "logs.example.com:443" }, "cleartext"},
		{"remote plaintext override ok", func(c *Config) {
			c.Server.Address = "logs.example.com:443"
			c.Server.TLS.AllowInsecureRemote = true
		}, ""},
		{"remote tls ok", func(c *Config) {
			c.Server.Address = "logs.example.com:443"
			c.Server.TLS.Enabled = true
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
