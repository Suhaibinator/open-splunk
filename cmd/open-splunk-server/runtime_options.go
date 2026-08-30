package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/logging"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

type runtimeCredentialGroup string

const (
	administratorCredentialGroup runtimeCredentialGroup = "administrator"
	clickHouseCredentialGroup    runtimeCredentialGroup = "clickhouse"
)

type runtimeOptionBinding struct {
	flagName        string
	environmentName string
	credentialGroup runtimeCredentialGroup
	sensitive       bool
}

var runtimeOptionBindings = []runtimeOptionBinding{
	{flagName: "log-level", environmentName: "OPEN_SPLUNK_SERVER_LOG_LEVEL"},
	{flagName: "log-format", environmentName: "OPEN_SPLUNK_SERVER_LOG_FORMAT"},
	{flagName: "verify-embedded-release", environmentName: "OPEN_SPLUNK_SERVER_VERIFY_EMBEDDED_RELEASE"},
	{flagName: "tenant-id", environmentName: "OPEN_SPLUNK_SERVER_TENANT_ID"},
	{flagName: "http-listen-address", environmentName: "OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS"},
	{flagName: "http-allowed-hosts", environmentName: "OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS"},
	{flagName: "http-tls-certificate-file", environmentName: "OPEN_SPLUNK_SERVER_HTTP_TLS_CERTIFICATE_FILE"},
	{flagName: "http-tls-private-key-file", environmentName: "OPEN_SPLUNK_SERVER_HTTP_TLS_PRIVATE_KEY_FILE"},
	{flagName: "http-trust-x-forwarded-proto", environmentName: "OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO"},
	{flagName: "control-database-file", environmentName: "OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE"},
	{flagName: "master-key-file", environmentName: "OPEN_SPLUNK_SERVER_MASTER_KEY_FILE"},
	{flagName: "server-lock-file", environmentName: "OPEN_SPLUNK_SERVER_LOCK_FILE"},
	{flagName: "export-artifact-directory", environmentName: "OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY"},
	{flagName: "search-artifact-directory", environmentName: "OPEN_SPLUNK_SERVER_SEARCH_ARTIFACT_DIRECTORY"},
	{flagName: "alert-public-base-url", environmentName: "OPEN_SPLUNK_SERVER_ALERT_PUBLIC_BASE_URL"},
	{flagName: "alert-private-webhook-hosts", environmentName: "OPEN_SPLUNK_SERVER_ALERT_PRIVATE_WEBHOOK_HOSTS"},
	{flagName: "administrator-token", environmentName: "OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN", credentialGroup: administratorCredentialGroup, sensitive: true},
	{flagName: "administrator-token-file", environmentName: "OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE", credentialGroup: administratorCredentialGroup},
	{flagName: "clickhouse-address", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS"},
	{flagName: "clickhouse-database", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_DATABASE"},
	{flagName: "clickhouse-username", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME"},
	{flagName: "clickhouse-password", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD", credentialGroup: clickHouseCredentialGroup, sensitive: true},
	{flagName: "clickhouse-password-file", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD_FILE", credentialGroup: clickHouseCredentialGroup},
	{flagName: "clickhouse-tls-enabled", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_ENABLED"},
	{flagName: "clickhouse-tls-ca-certificate-file", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_CA_CERTIFICATE_FILE"},
	{flagName: "clickhouse-tls-server-name", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_SERVER_NAME"},
	{flagName: "clickhouse-skip-migrations", environmentName: "OPEN_SPLUNK_SERVER_CLICKHOUSE_SKIP_MIGRATIONS"},
	{flagName: "collector-grpc-listen-address", environmentName: "OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_LISTEN_ADDRESS"},
	{flagName: "collector-grpc-plaintext-enabled", environmentName: "OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_PLAINTEXT_ENABLED"},
	{flagName: "collector-grpc-tls-certificate-file", environmentName: "OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_CERTIFICATE_FILE"},
	{flagName: "collector-grpc-tls-private-key-file", environmentName: "OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_PRIVATE_KEY_FILE"},
	{flagName: "hec-enabled", environmentName: "OPEN_SPLUNK_SERVER_HEC_ENABLED"},
	{flagName: "default-index-retention", environmentName: "OPEN_SPLUNK_SERVER_DEFAULT_INDEX_RETENTION"},
	{flagName: "search-history-maximum-age", environmentName: "OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_AGE"},
	{flagName: "search-history-maximum-entries-per-owner", environmentName: "OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_ENTRIES_PER_OWNER"},
	{flagName: "search-attempt-audit-maximum-retained-attempts", environmentName: "OPEN_SPLUNK_SERVER_SEARCH_ATTEMPT_AUDIT_MAXIMUM_RETAINED_ATTEMPTS"},
}

type legacyRuntimeEnvironment struct {
	name        string
	replacement string
	sensitive   bool
}

var legacyRuntimeEnvironments = []legacyRuntimeEnvironment{
	{name: "OPEN_SPLUNK_ADMINISTRATOR_TOKEN", replacement: "OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN", sensitive: true},
	{name: "OPEN_SPLUNK_CLICKHOUSE_PASSWORD", replacement: "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD", sensitive: true},
	{name: "OPEN_SPLUNK_HTTP_ALLOWED_HOSTS", replacement: "OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS"},
	{name: "OPEN_SPLUNK_HTTP_TRUST_X_FORWARDED_PROTO", replacement: "OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO"},
	{name: "OPEN_SPLUNK_CLICKHOUSE_ADDRESS", replacement: "OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS"},
	{name: "OPEN_SPLUNK_CLICKHOUSE_USERNAME", replacement: "OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME"},
	{name: "OPEN_SPLUNK_HEC_ENABLED", replacement: "OPEN_SPLUNK_SERVER_HEC_ENABLED"},
	{name: "OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH", replacement: "OPEN_SPLUNK_SERVER_LOCK_FILE"},
}

type runtimeEnvironment struct {
	lookup func(string) (string, bool)
	unset  func(string) error
}

type capturedRuntimeEnvironment struct {
	binding runtimeOptionBinding
	value   string
}

func parseFlags() (options, error) {
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	return parseRuntimeOptions(
		flags,
		os.Args[1:],
		runtimeEnvironment{lookup: os.LookupEnv, unset: os.Unsetenv},
	)
}

func parseRuntimeOptions(
	flags *flag.FlagSet,
	args []string,
	environment runtimeEnvironment,
) (options, error) {
	var result options
	registerRuntimeFlags(flags, &result)
	if err := flags.Parse(args); err != nil {
		return options{}, errors.Join(err, discardSensitiveRuntimeEnvironment(environment))
	}
	if err := applyRuntimeEnvironment(flags, environment); err != nil {
		return options{}, err
	}
	if strings.TrimSpace(result.masterKeyPath) == "" {
		result.masterKeyPath = result.controlDBPath + ".key"
	}
	if strings.TrimSpace(result.searchArtifactDir) == "" {
		result.searchArtifactDir = result.controlDBPath + ".search-artifacts"
	}
	if _, err := logging.ParseLevel(result.logLevel); err != nil {
		return options{}, fmt.Errorf("configure -log-level: %w", err)
	}
	if _, err := logging.ParseFormat(result.logFormat); err != nil {
		return options{}, fmt.Errorf("configure -log-format: %w", err)
	}
	return result, nil
}

func discardSensitiveRuntimeEnvironment(environment runtimeEnvironment) error {
	if environment.lookup == nil || environment.unset == nil {
		return errors.New("runtime configuration environment is unavailable")
	}
	var result error
	for _, binding := range runtimeOptionBindings {
		if !binding.sensitive {
			continue
		}
		if _, configured := environment.lookup(binding.environmentName); !configured {
			continue
		}
		if err := environment.unset(binding.environmentName); err != nil {
			result = errors.Join(result, fmt.Errorf("discard %s: %w", binding.environmentName, err))
		}
	}
	for _, legacy := range legacyRuntimeEnvironments {
		if !legacy.sensitive {
			continue
		}
		if _, configured := environment.lookup(legacy.name); !configured {
			continue
		}
		if err := environment.unset(legacy.name); err != nil {
			result = errors.Join(result, fmt.Errorf("discard %s: %w", legacy.name, err))
		}
	}
	return result
}

func registerRuntimeFlags(flags *flag.FlagSet, result *options) {
	flags.StringVar(&result.logLevel, "log-level", logging.DefaultLevel, "minimum log level: debug, info, warn, or error")
	flags.StringVar(&result.logFormat, "log-format", logging.DefaultFormat, "log encoding: json or console")
	flags.BoolVar(&result.verifyEmbeddedRelease, "verify-embedded-release", false, "verify the embedded release payload and exit before opening runtime resources")
	flags.StringVar(&result.tenantID, "tenant-id", "default", "single-node tenant identifier")
	flags.StringVar(&result.httpAddress, "http-listen-address", "127.0.0.1:8080", "browser/API listen address")
	flags.StringVar(&result.httpAllowedHostsCSV, "http-allowed-hosts", "", "comma-separated Host names allowed to use the browser API (defaults to the specific listen host)")
	flags.StringVar(&result.httpTLSCert, "http-tls-certificate-file", "", "PEM certificate chain for HTTPS browser/API traffic (requires -http-tls-private-key-file)")
	flags.StringVar(&result.httpTLSKey, "http-tls-private-key-file", "", "PEM private key for HTTPS browser/API traffic (requires -http-tls-certificate-file)")
	flags.BoolVar(&result.trustForwardedProto, "http-trust-x-forwarded-proto", false, "trust one X-Forwarded-Proto value on plaintext browser/API connections")
	flags.StringVar(&result.controlDBPath, "control-database-file", "open-splunk.db", "SQLite control-plane database file")
	flags.StringVar(&result.masterKeyPath, "master-key-file", "", "server master-key file (default: <control-database-file>.key)")
	flags.StringVar(&result.serverLockFile, "server-lock-file", hostSingletonLockPath, "host-wide singleton lock file")
	flags.StringVar(&result.exportArtifactDir, "export-artifact-directory", "", "private export-artifact directory (default: <control-database-file>.exports)")
	flags.StringVar(&result.searchArtifactDir, "search-artifact-directory", "", "private retained-search directory (default: <control-database-file>.search-artifacts)")
	flags.StringVar(&result.alertPublicBaseURL, "alert-public-base-url", "", "absolute public base URL required before enabling webhook alerts")
	flags.StringVar(&result.alertPrivateWebhookHostsCSV, "alert-private-webhook-hosts", "", "comma-separated exact hostnames permitted to resolve to private addresses")
	flags.StringVar(&result.administratorToken, "administrator-token", "", "administrator bearer token (unsafe in process arguments; prefer -administrator-token-file)")
	flags.StringVar(&result.administratorTokenFile, "administrator-token-file", "", "owner-only administrator bearer-token file (required mode 0400 or 0600)")
	flags.StringVar(&result.clickhouseAddress, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native-protocol address")
	flags.StringVar(&result.clickhouseDatabase, "clickhouse-database", "open_splunk", "ClickHouse database")
	flags.StringVar(&result.clickhouseUsername, "clickhouse-username", "default", "ClickHouse username for migrations and application operations")
	flags.StringVar(&result.clickhousePassword, "clickhouse-password", "", "ClickHouse password (unsafe in process arguments; prefer -clickhouse-password-file)")
	flags.StringVar(&result.clickhousePasswordFile, "clickhouse-password-file", "", "owner-only ClickHouse password file")
	flags.BoolVar(&result.clickhouseSecure, "clickhouse-tls-enabled", false, "use verified TLS for ClickHouse")
	flags.StringVar(&result.clickhouseCACertFile, "clickhouse-tls-ca-certificate-file", "", "PEM trust bundle for verified ClickHouse TLS (requires -clickhouse-tls-enabled)")
	flags.StringVar(&result.clickhouseServerName, "clickhouse-tls-server-name", "", "explicit DNS name or IP SAN to verify for ClickHouse TLS (requires -clickhouse-tls-enabled)")
	registerClickHouseSkipMigrationsFlag(flags, result)
	flags.StringVar(&result.collectorAddress, "collector-grpc-listen-address", "", "collector gRPC listen address (disabled when empty)")
	flags.BoolVar(&result.collectorInsecure, "collector-grpc-plaintext-enabled", false, "explicitly allow plaintext collector gRPC on loopback only")
	flags.StringVar(&result.collectorTLSCert, "collector-grpc-tls-certificate-file", "", "PEM certificate for collector gRPC TLS")
	flags.StringVar(&result.collectorTLSKey, "collector-grpc-tls-private-key-file", "", "PEM private key for collector gRPC TLS")
	registerHECEnabledFlag(flags, result)
	flags.DurationVar(&result.indexRetention, "default-index-retention", defaultIndexRetention, "retention used when an index does not override it")
	flags.DurationVar(&result.searchHistoryMaximumAge, "search-history-maximum-age", searchhistory.DefaultMaximumAge, "maximum age of terminal search-history entries")
	flags.IntVar(&result.searchHistoryMaximumEntriesPerOwner, "search-history-maximum-entries-per-owner", searchhistory.DefaultMaximumEntriesPerOwner, "maximum terminal entries retained per owner (pending attempts are capped separately at the same value)")
	registerSearchAttemptAuditMaximumRetainedFlag(flags, result)
}

func applyRuntimeEnvironment(flags *flag.FlagSet, environment runtimeEnvironment) error {
	if flags == nil || environment.lookup == nil || environment.unset == nil {
		return errors.New("runtime configuration environment is unavailable")
	}
	if err := validateRuntimeOptionRegistry(flags, runtimeOptionBindings); err != nil {
		return err
	}

	visited := make(map[string]struct{})
	flags.Visit(func(value *flag.Flag) { visited[value.Name] = struct{}{} })
	cliGroups := make(map[runtimeCredentialGroup][]string)
	for _, binding := range runtimeOptionBindings {
		if _, ok := visited[binding.flagName]; ok && binding.credentialGroup != "" {
			cliGroups[binding.credentialGroup] = append(cliGroups[binding.credentialGroup], binding.flagName)
		}
	}

	var captured []capturedRuntimeEnvironment
	var cleanupErrors []error
	for _, binding := range runtimeOptionBindings {
		value, configured := environment.lookup(binding.environmentName)
		if !configured {
			continue
		}
		if binding.sensitive {
			if err := environment.unset(binding.environmentName); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("discard %s: %w", binding.environmentName, err))
			}
		}
		captured = append(captured, capturedRuntimeEnvironment{binding: binding, value: value})
	}
	var legacyErrors []error
	for _, legacy := range legacyRuntimeEnvironments {
		if _, configured := environment.lookup(legacy.name); !configured {
			continue
		}
		if legacy.sensitive {
			if err := environment.unset(legacy.name); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("discard %s: %w", legacy.name, err))
			}
		}
		legacyErrors = append(legacyErrors, fmt.Errorf("%s was renamed to %s", legacy.name, legacy.replacement))
	}
	if len(cleanupErrors) != 0 || len(legacyErrors) != 0 {
		return errors.Join(append(cleanupErrors, legacyErrors...)...)
	}

	for _, group := range []runtimeCredentialGroup{
		administratorCredentialGroup,
		clickHouseCredentialGroup,
	} {
		names := cliGroups[group]
		if len(names) > 1 {
			sort.Strings(names)
			return fmt.Errorf("credential flags -%s and -%s are mutually exclusive", names[0], names[1])
		}
	}
	environmentGroups := make(map[runtimeCredentialGroup][]string)
	for _, capturedValue := range captured {
		binding := capturedValue.binding
		if binding.credentialGroup != "" {
			if len(cliGroups[binding.credentialGroup]) != 0 {
				continue
			}
			if strings.TrimSpace(capturedValue.value) != "" {
				environmentGroups[binding.credentialGroup] = append(
					environmentGroups[binding.credentialGroup],
					binding.environmentName,
				)
			}
		}
		if _, explicit := visited[binding.flagName]; explicit {
			continue
		}
		if err := flags.Set(binding.flagName, capturedValue.value); err != nil {
			return fmt.Errorf("configure -%s from %s: %w", binding.flagName, binding.environmentName, err)
		}
	}
	for _, group := range []runtimeCredentialGroup{
		administratorCredentialGroup,
		clickHouseCredentialGroup,
	} {
		names := environmentGroups[group]
		if len(names) > 1 {
			sort.Strings(names)
			return fmt.Errorf("credential environment variables %s and %s are mutually exclusive", names[0], names[1])
		}
	}
	return nil
}

func validateRuntimeOptionRegistry(
	flags *flag.FlagSet,
	bindings []runtimeOptionBinding,
) error {
	registered := make(map[string]struct{})
	flags.VisitAll(func(value *flag.Flag) { registered[value.Name] = struct{}{} })
	seenFlags := make(map[string]struct{}, len(bindings))
	seenEnvironment := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.flagName == "" || binding.environmentName == "" {
			return errors.New("runtime option binding is incomplete")
		}
		if _, ok := registered[binding.flagName]; !ok {
			return fmt.Errorf("runtime environment binding references unknown flag -%s", binding.flagName)
		}
		if _, duplicate := seenFlags[binding.flagName]; duplicate {
			return fmt.Errorf("runtime flag -%s has duplicate environment bindings", binding.flagName)
		}
		if _, duplicate := seenEnvironment[binding.environmentName]; duplicate {
			return fmt.Errorf("runtime environment variable %s has duplicate flag bindings", binding.environmentName)
		}
		seenFlags[binding.flagName] = struct{}{}
		seenEnvironment[binding.environmentName] = struct{}{}
	}
	for name := range registered {
		if _, ok := seenFlags[name]; !ok {
			return fmt.Errorf("runtime flag -%s has no environment binding", name)
		}
	}
	return nil
}
