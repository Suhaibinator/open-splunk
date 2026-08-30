package main

import (
	"flag"
	"io"
	"maps"
	"os"
	"strings"
	"testing"
	"time"
)

type runtimeEnvironmentFixture struct {
	values  map[string]string
	removed map[string]bool
}

func newRuntimeEnvironmentFixture(values map[string]string) *runtimeEnvironmentFixture {
	return &runtimeEnvironmentFixture{
		values:  maps.Clone(values),
		removed: make(map[string]bool),
	}
}

func (fixture *runtimeEnvironmentFixture) environment() runtimeEnvironment {
	return runtimeEnvironment{
		lookup: func(name string) (string, bool) {
			value, ok := fixture.values[name]
			return value, ok
		},
		unset: func(name string) error {
			fixture.removed[name] = true
			delete(fixture.values, name)
			return nil
		},
	}
}

func parseRuntimeOptionsForTest(
	t *testing.T,
	args []string,
	environmentValues map[string]string,
) (options, *runtimeEnvironmentFixture, error) {
	t.Helper()
	flags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	fixture := newRuntimeEnvironmentFixture(environmentValues)
	result, err := parseRuntimeOptions(flags, args, fixture.environment())
	return result, fixture, err
}

func TestRuntimeOptionRegistryIsCompleteAndUnique(t *testing.T) {
	flags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
	var config options
	registerRuntimeFlags(flags, &config)
	if err := validateRuntimeOptionRegistry(flags, runtimeOptionBindings); err != nil {
		t.Fatal(err)
	}
	if got, want := len(runtimeOptionBindings), 36; got != want {
		t.Fatalf("runtime option binding count = %d, want %d", got, want)
	}

	for _, binding := range runtimeOptionBindings {
		want := "OPEN_SPLUNK_SERVER_" + strings.ToUpper(strings.ReplaceAll(binding.flagName, "-", "_"))
		if binding.flagName == "server-lock-file" {
			want = "OPEN_SPLUNK_SERVER_LOCK_FILE"
		}
		if binding.environmentName != want {
			t.Errorf("-%s environment = %s, want %s", binding.flagName, binding.environmentName, want)
		}
	}
}

func TestRuntimeOptionDocumentationCoversRegistry(t *testing.T) {
	documentation, err := os.ReadFile("../../deploy/README.md")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(documentation)
	if !strings.Contains(
		contents,
		"| Environment variable | CLI flag | Built-in default | Purpose | Accepted values and constraints |",
	) {
		t.Fatal("server configuration reference is missing its explanatory columns")
	}
	for _, binding := range runtimeOptionBindings {
		rowPrefix := "| `" + binding.environmentName + "` | `-" + binding.flagName + "` |"
		if count := strings.Count(contents, rowPrefix); count != 1 {
			t.Errorf("documentation rows for %s = %d, want 1", binding.environmentName, count)
		}
	}
	for _, name := range []string{
		"OPEN_SPLUNK_DEPLOY_SERVER_IMAGE",
		"OPEN_SPLUNK_DEPLOY_HTTP_PORT",
		"OPEN_SPLUNK_DEPLOY_CLICKHOUSE_NATIVE_PORT",
	} {
		if rowPrefix := "| `" + name + "` |"; strings.Count(contents, rowPrefix) != 1 {
			t.Errorf("deployment documentation row for %s is missing or duplicated", name)
		}
	}
}

func TestRuntimeOptionRegistryRejectsInvalidEntries(t *testing.T) {
	flags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
	var config options
	registerRuntimeFlags(flags, &config)

	for name, bindings := range map[string][]runtimeOptionBinding{
		"incomplete":   {{ /* intentionally empty */ }},
		"unknown flag": {{flagName: "not-registered", environmentName: "OPEN_SPLUNK_SERVER_NOT_REGISTERED"}},
		"duplicate flag": {
			{flagName: "tenant-id", environmentName: "OPEN_SPLUNK_SERVER_TENANT_ID"},
			{flagName: "tenant-id", environmentName: "OPEN_SPLUNK_SERVER_OTHER_TENANT_ID"},
		},
		"duplicate environment": {
			{flagName: "tenant-id", environmentName: "OPEN_SPLUNK_SERVER_SHARED"},
			{flagName: "http-listen-address", environmentName: "OPEN_SPLUNK_SERVER_SHARED"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRuntimeOptionRegistry(flags, bindings); err == nil {
				t.Fatal("validateRuntimeOptionRegistry() succeeded")
			}
		})
	}
}

func TestParseRuntimeOptionsPrecedenceAndTypes(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		config, _, err := parseRuntimeOptionsForTest(t, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if config.httpAddress != "127.0.0.1:8080" ||
			config.tenantID != "default" || config.hecEnabled ||
			config.masterKeyPath != "open-splunk.db.key" ||
			config.searchArtifactDir != "open-splunk.db.search-artifacts" ||
			config.logLevel != "info" || config.logFormat != "json" {
			t.Fatalf("default runtime configuration = %#v", config)
		}
	})

	t.Run("environment", func(t *testing.T) {
		config, _, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
			"OPEN_SPLUNK_SERVER_LOG_LEVEL":                                      "DEBUG",
			"OPEN_SPLUNK_SERVER_LOG_FORMAT":                                     "CONSOLE",
			"OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS":                            "0.0.0.0:8081",
			"OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO":                   "true",
			"OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE":                          "/state/control.db",
			"OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_AGE":                     "6h",
			"OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_ENTRIES_PER_OWNER":       "37",
			"OPEN_SPLUNK_SERVER_SEARCH_ATTEMPT_AUDIT_MAXIMUM_RETAINED_ATTEMPTS": "41",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.logLevel != "DEBUG" || config.logFormat != "CONSOLE" ||
			config.httpAddress != "0.0.0.0:8081" || !config.trustForwardedProto ||
			config.controlDBPath != "/state/control.db" ||
			config.masterKeyPath != "/state/control.db.key" ||
			config.searchArtifactDir != "/state/control.db.search-artifacts" ||
			config.searchHistoryMaximumAge != 6*time.Hour ||
			config.searchHistoryMaximumEntriesPerOwner != 37 ||
			config.searchAttemptAuditMaximumRetainedAttempts != 41 {
			t.Fatalf("environment runtime configuration = %#v", config)
		}
	})

	t.Run("environment false", func(t *testing.T) {
		config, _, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
			"OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO": "false",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.trustForwardedProto {
			t.Fatal("false environment value enabled forwarded-proto trust")
		}
	})

	t.Run("explicit CLI wins including zero values", func(t *testing.T) {
		config, _, err := parseRuntimeOptionsForTest(t, []string{
			"-log-level=error",
			"-log-format=json",
			"-tenant-id=",
			"-http-trust-x-forwarded-proto=false",
			"-search-history-maximum-age=0s",
			"-search-history-maximum-entries-per-owner=0",
		}, map[string]string{
			"OPEN_SPLUNK_SERVER_LOG_LEVEL":                                "debug",
			"OPEN_SPLUNK_SERVER_LOG_FORMAT":                               "console",
			"OPEN_SPLUNK_SERVER_TENANT_ID":                                "environment-tenant",
			"OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO":             "true",
			"OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_AGE":               "6h",
			"OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_ENTRIES_PER_OWNER": "37",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.logLevel != "error" || config.logFormat != "json" ||
			config.tenantID != "" || config.trustForwardedProto ||
			config.searchHistoryMaximumAge != 0 ||
			config.searchHistoryMaximumEntriesPerOwner != 0 {
			t.Fatalf("CLI-overridden runtime configuration = %#v", config)
		}
	})
}

func TestParseRuntimeOptionsRejectsInvalidEnvironmentValue(t *testing.T) {
	_, _, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
		"OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO": "not-a-boolean",
	})
	if err == nil || !strings.Contains(err.Error(), "OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO") {
		t.Fatalf("invalid environment error = %v", err)
	}
}

func TestParseRuntimeOptionsRejectsInvalidLoggingValues(t *testing.T) {
	t.Parallel()
	for name, environment := range map[string]map[string]string{
		"level":  {"OPEN_SPLUNK_SERVER_LOG_LEVEL": "trace"},
		"format": {"OPEN_SPLUNK_SERVER_LOG_FORMAT": "text"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := parseRuntimeOptionsForTest(t, nil, environment); err == nil {
				t.Fatal("parseRuntimeOptions() succeeded")
			}
		})
	}
}

func TestParseRuntimeOptionsCredentialGroupPrecedence(t *testing.T) {
	t.Run("environment raw", func(t *testing.T) {
		config, environment, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
			"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN": "environment-secret",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.administratorToken != "environment-secret" ||
			!environment.removed["OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN"] {
			t.Fatalf("administrator environment result = (%#v, %#v)", config, environment)
		}
	})

	t.Run("environment raw and file conflict", func(t *testing.T) {
		const secret = "environment-secret-must-not-leak"
		_, environment, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD":      secret,
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD_FILE": "/run/secrets/clickhouse",
		})
		if err == nil || strings.Contains(err.Error(), secret) ||
			!environment.removed["OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD"] {
			t.Fatalf("credential conflict = (%v, %#v)", err, environment)
		}
	})

	t.Run("CLI raw overrides both environment forms", func(t *testing.T) {
		config, environment, err := parseRuntimeOptionsForTest(t, []string{
			"-clickhouse-password=cli-secret",
		}, map[string]string{
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD":      "environment-secret",
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD_FILE": "/run/secrets/clickhouse",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.clickhousePassword != "cli-secret" || config.clickhousePasswordFile != "" ||
			!environment.removed["OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD"] {
			t.Fatalf("CLI credential result = (%#v, %#v)", config, environment)
		}
	})

	t.Run("CLI file overrides both environment forms", func(t *testing.T) {
		config, environment, err := parseRuntimeOptionsForTest(t, []string{
			"-administrator-token-file=/cli/admin-token",
		}, map[string]string{
			"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN":      "environment-secret",
			"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE": "/environment/admin-token",
		})
		if err != nil {
			t.Fatal(err)
		}
		if config.administratorToken != "" || config.administratorTokenFile != "/cli/admin-token" ||
			!environment.removed["OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN"] {
			t.Fatalf("CLI credential-file result = (%#v, %#v)", config, environment)
		}
	})

	t.Run("CLI raw and file conflict", func(t *testing.T) {
		const secret = "cli-secret-must-not-leak"
		_, _, err := parseRuntimeOptionsForTest(t, []string{
			"-administrator-token=" + secret,
			"-administrator-token-file=/run/secrets/admin",
		}, nil)
		if err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("CLI credential conflict error = %v", err)
		}
	})

	t.Run("secret removed when another environment value is invalid", func(t *testing.T) {
		const secret = "secret-must-not-leak"
		_, environment, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
			"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN":          secret,
			"OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO": "invalid",
		})
		if err == nil || strings.Contains(err.Error(), secret) ||
			!environment.removed["OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN"] {
			t.Fatalf("invalid environment cleanup = (%v, %#v)", err, environment)
		}
	})

	t.Run("secret removed when CLI parsing fails", func(t *testing.T) {
		const secret = "secret-must-not-leak"
		_, environment, err := parseRuntimeOptionsForTest(t, []string{
			"-not-a-runtime-flag=value",
		}, map[string]string{
			"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD": secret,
		})
		if err == nil || strings.Contains(err.Error(), secret) ||
			!environment.removed["OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD"] {
			t.Fatalf("CLI parse cleanup = (%v, %#v)", err, environment)
		}
	})
}

func TestParseRuntimeOptionsRejectsLegacyEnvironment(t *testing.T) {
	for _, legacy := range legacyRuntimeEnvironments {
		t.Run(legacy.name, func(t *testing.T) {
			const secret = "legacy-secret-must-not-leak"
			_, environment, err := parseRuntimeOptionsForTest(t, nil, map[string]string{
				legacy.name: secret,
			})
			if err == nil || !strings.Contains(err.Error(), legacy.name) ||
				!strings.Contains(err.Error(), legacy.replacement) ||
				strings.Contains(err.Error(), secret) {
				t.Fatalf("legacy environment error = %v", err)
			}
			if legacy.sensitive && !environment.removed[legacy.name] {
				t.Fatalf("sensitive legacy environment %s was not removed", legacy.name)
			}
		})
	}
}

func TestParseRuntimeOptionsRejectsRenamedFlags(t *testing.T) {
	for _, name := range []string{
		"http-address",
		"http-tls-cert",
		"http-tls-key",
		"control-db",
		"master-key",
		"export-artifact-dir",
		"clickhouse-secure",
		"clickhouse-ca-cert",
		"clickhouse-server-name",
		"collector-grpc-address",
		"collector-grpc-insecure",
		"collector-tls-cert",
		"collector-tls-key",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := parseRuntimeOptionsForTest(t, []string{"-" + name + "=value"}, nil)
			if err == nil {
				t.Fatalf("legacy flag -%s was accepted", name)
			}
		})
	}
}
