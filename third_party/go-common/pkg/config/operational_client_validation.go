package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Validate syntax before publishing a generation. Credentials and remote
// availability are checked by the affected client on use, without stale fallback.
func (c *Config) validateOperationalClients() error {
	if err := ValidateProviders(c.InferenceProviders); err != nil {
		return fmt.Errorf("InferenceProviders: %w", err)
	}

	for _, endpoint := range []struct{ name, value string }{
		{"PostalAPIEndpoint", c.PostalAPIEndpoint},
		{"MinioPublicBaseURL", c.MinioPublicBaseURL},
	} {
		if err := validateOptionalHTTPEndpoint(endpoint.name, endpoint.value); err != nil {
			return err
		}
	}
	if c.EmailProvider != nil && (*c.EmailProvider < EmailProviderPostal || *c.EmailProvider > EmailProviderMockmail) {
		return fmt.Errorf("EmailProvider must identify a supported provider")
	}
	if c.SMSProvider != nil && (*c.SMSProvider < SMSProviderTwilio || *c.SMSProvider > SMSProviderMocksms) {
		return fmt.Errorf("SMSProvider must identify a supported provider")
	}
	if c.MetricsPushEndpoint != nil {
		if err := validateOptionalHTTPEndpoint("MetricsPushEndpoint", *c.MetricsPushEndpoint); err != nil {
			return err
		}
	}
	if c.MetricsPushInterval != nil && (*c.MetricsPushInterval < 0 || int64(*c.MetricsPushInterval) > int64((1<<63-1)/time.Second)) {
		return fmt.Errorf("MetricsPushInterval must be non-negative and fit a duration in seconds")
	}
	if c.MetricsPushEnabled != nil && *c.MetricsPushEnabled {
		if c.MetricsPushEndpoint == nil || strings.TrimSpace(*c.MetricsPushEndpoint) == "" {
			return fmt.Errorf("MetricsPushEndpoint is required when pushing is enabled")
		}
		if c.MetricsPushJobName != nil && strings.TrimSpace(*c.MetricsPushJobName) == "" {
			return fmt.Errorf("MetricsPushJobName must be non-empty when pushing is enabled")
		}
	}
	if c.StreamingHashChunkSize != nil && *c.StreamingHashChunkSize <= 0 {
		return fmt.Errorf("StreamingHashChunkSize must be positive")
	}
	return nil
}

func validateOptionalHTTPEndpoint(name, value string) error {
	if value == "" {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		// Do not include URLs, which may contain credentials, in validation errors.
		return fmt.Errorf("%s must be an absolute HTTP(S) URL", name)
	}
	return nil
}
