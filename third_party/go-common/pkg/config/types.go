package config

import (
	json "encoding/json/v2"
)

// OAuthSignupMode defines the behavior for OAuth account creation when no user exists.
type OAuthSignupMode int

// OAuth signup mode constants.
const (
	OAuthSignupModeLegacy OAuthSignupMode = iota
	OAuthSignupModeDisabled
	OAuthSignupModePrefixed
	OAuthSignupModeEmailLocalPart
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (o OAuthSignupMode) String() string {
	values := [...]string{"legacy", "disabled", "prefixed", "email_local_part"}
	if int(o) < 0 || int(o) >= len(values) {
		return "unknown"
	}
	return values[o]
}

// SinkType represents the destination for log output.
// It determines where logs are sent for storage or processing.
type SinkType int

const (
	// SinkTypeNone disables log output
	SinkTypeNone SinkType = iota
	// SinkTypeStdout sends logs to standard output
	SinkTypeStdout
	// SinkTypeFile writes logs to a file on disk
	SinkTypeFile
	// SinkTypeGraylog sends logs to a Graylog server
	SinkTypeGraylog
	// SinkTypeGrafanaAlloy sends logs to Grafana Alloy (formerly Grafana Agent)
	SinkTypeGrafanaAlloy
	// SinkTypeGrafanaLoki sends logs to Grafana Loki for log aggregation
	SinkTypeGrafanaLoki
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (l SinkType) String() string {
	values := [...]string{"none", "stdout", "file", "graylog", "grafana_alloy", "grafana_loki"}
	if int(l) < 0 || int(l) >= len(values) {
		return "unknown"
	}
	return values[l]
}

// Environment represents the application deployment environment.
// It determines behavior around safety checks, logging, and feature availability.
type Environment int

const (
	// EnvironmentDevelopment represents local development environments.
	// Enables debug features, relaxed security checks, and development tooling.
	EnvironmentDevelopment Environment = iota
	// EnvironmentTest represents automated testing environments.
	// Used in CI/CD pipelines with isolated test data and mock services.
	EnvironmentTest
	// EnvironmentMocking represents dedicated mock data generation environments.
	// Optimized for generating consistent mock data with deterministic services.
	// Allows mock token service and other testing utilities.
	EnvironmentMocking
	// EnvironmentStaging represents pre-production environments.
	// Mirror of production with realistic data for final validation.
	EnvironmentStaging
	// EnvironmentProduction represents live production environments.
	// Enforces strict security, disables debug features, enables full monitoring.
	EnvironmentProduction
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (e Environment) String() string {
	names := [...]string{"development", "test", "mocking", "staging", "production"}
	if int(e) < 0 || int(e) >= len(names) {
		return "unknown"
	}
	return names[e]
}

// IsProduction executes its operation in the configuration loading, defaulting, and validation flow.
func (e Environment) IsProduction() bool {
	return e == EnvironmentProduction || e == EnvironmentStaging
}

// IsDevelopment executes its operation in the configuration loading, defaulting, and validation flow.
func (e Environment) IsDevelopment() bool {
	return e == EnvironmentDevelopment || e == EnvironmentTest || e == EnvironmentMocking
}

// IsMockingAllowed executes its operation in the configuration loading, defaulting, and validation flow.
func (e Environment) IsMockingAllowed() bool {
	return e == EnvironmentDevelopment || e == EnvironmentTest || e == EnvironmentMocking
}

// EmailProvider represents the email service provider configuration.
// It determines which email service is used for sending emails.
type EmailProvider int

const (
	// EmailProviderPostal uses a self-hosted Postal email server.
	// Requires postal_api_endpoint and postal_api_key configuration.
	EmailProviderPostal EmailProvider = iota
	// EmailProviderGmail uses Gmail API for sending emails.
	// Requires gmail_credentials, gmail_token, and gmail_user_id configuration.
	EmailProviderGmail
	// EmailProviderResend uses the Resend transactional email service.
	// Requires resend_api_key configuration.
	EmailProviderResend
	// EmailProviderMockmail uses a mock email service for testing.
	// No external configuration required, used for development/testing.
	EmailProviderMockmail
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (e EmailProvider) String() string {
	values := [...]string{"postal", "gmail", "resend", "mockmail"}
	if int(e) < 0 || int(e) >= len(values) {
		return "unknown"
	}
	return values[e]
}

// ParseEmailProvider executes its operation in the configuration loading, defaulting, and validation flow.
func ParseEmailProvider(s string) EmailProvider {
	switch s {
	case "postal":
		return EmailProviderPostal
	case "gmail":
		return EmailProviderGmail
	case "resend":
		return EmailProviderResend
	case "mockmail":
		return EmailProviderMockmail
	default:
		return EmailProviderResend // Default to resend
	}
}

// MarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (e EmailProvider) MarshalJSON() ([]byte, error) {
	return []byte(`"` + e.String() + `"`), nil
}

// UnmarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (e *EmailProvider) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*e = ParseEmailProvider(s)
	return nil
}

// DBLogLevel represents the database logging verbosity level.
// It determines how much logging information GORM outputs.
type DBLogLevel int

const (
	// DBLogLevelSilent disables all database logging.
	// No SQL queries or errors are logged.
	DBLogLevelSilent DBLogLevel = iota
	// DBLogLevelError logs only database errors.
	// SQL queries are not logged, only failures.
	DBLogLevelError
	// DBLogLevelWarn logs database errors and warnings.
	// Includes slow queries and connection issues.
	DBLogLevelWarn
	// DBLogLevelInfo logs all database operations.
	// Includes SQL queries, errors, and performance information.
	DBLogLevelInfo
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (d DBLogLevel) String() string {
	values := [...]string{"silent", "error", "warn", "info"}
	if int(d) < 0 || int(d) >= len(values) {
		return "unknown"
	}
	return values[d]
}

// ParseDBLogLevel executes its operation in the configuration loading, defaulting, and validation flow.
func ParseDBLogLevel(s string) DBLogLevel {
	switch s {
	case "silent":
		return DBLogLevelSilent
	case "error":
		return DBLogLevelError
	case "warn":
		return DBLogLevelWarn
	case "info":
		return DBLogLevelInfo
	default:
		return DBLogLevelWarn // Default to warn for balanced verbosity
	}
}

// MarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (d DBLogLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.String() + `"`), nil
}

// UnmarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (d *DBLogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*d = ParseDBLogLevel(s)
	return nil
}

// SMSProvider represents the SMS service provider configuration.
// It determines which SMS service is used for sending text messages.
type SMSProvider int

const (
	// SMSProviderTwilio uses the Twilio SMS service.
	// Requires twilio_account_sid, twilio_auth_token configuration.
	SMSProviderTwilio SMSProvider = iota
	// SMSProviderVonage uses the Vonage (formerly Nexmo) SMS service.
	// Requires vonage_api_key and vonage_api_secret configuration.
	SMSProviderVonage
	// SMSProviderMocksms uses a mock SMS service for testing.
	// No external configuration required, used for development/testing.
	SMSProviderMocksms
)

// String executes its operation in the configuration loading, defaulting, and validation flow.
func (s SMSProvider) String() string {
	values := [...]string{"twilio", "vonage", "mocksms"}
	if int(s) < 0 || int(s) >= len(values) {
		return "unknown"
	}
	return values[s]
}

// ParseSMSProvider executes its operation in the configuration loading, defaulting, and validation flow.
func ParseSMSProvider(s string) SMSProvider {
	switch s {
	case "twilio":
		return SMSProviderTwilio
	case "vonage":
		return SMSProviderVonage
	case "mocksms":
		return SMSProviderMocksms
	default:
		return SMSProviderTwilio // Default to twilio
	}
}

// MarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (s SMSProvider) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// UnmarshalJSON executes its operation in the configuration loading, defaulting, and validation flow.
func (s *SMSProvider) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = ParseSMSProvider(str)
	return nil
}
