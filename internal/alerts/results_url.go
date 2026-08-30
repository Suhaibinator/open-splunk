package alerts

import (
	"errors"
	"net/url"
	"strings"
)

// ValidatePublicBaseURL validates the externally reachable base used in
// retained-result links. A configured path prefix is permitted and preserved.
func ValidatePublicBaseURL(raw string) error {
	_, err := parsePublicBaseURL(raw)
	return err
}

// SearchJobResultsURL builds the canonical deep link for one retained job.
func SearchJobResultsURL(publicBaseURL, jobID string) (string, error) {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(jobID) != jobID {
		return "", errors.New("alerts: search job ID is invalid")
	}
	return searchResultsURL(publicBaseURL, jobID, false)
}

// TestWebhookResultsURL builds the non-runnable result link used only by an
// operator-requested test webhook.
func TestWebhookResultsURL(publicBaseURL, deliveryID string) (string, error) {
	if strings.TrimSpace(deliveryID) == "" || strings.TrimSpace(deliveryID) != deliveryID {
		return "", errors.New("alerts: delivery ID is invalid")
	}
	return searchResultsURL(publicBaseURL, "test-"+deliveryID, true)
}

func searchResultsURL(publicBaseURL, jobID string, test bool) (string, error) {
	result, err := parsePublicBaseURL(publicBaseURL)
	if err != nil {
		return "", err
	}
	result.Path = strings.TrimSuffix(result.Path, "/") + "/search/"
	result.RawPath = ""
	query := result.Query()
	query.Set("searchJobId", jobID)
	if test {
		query.Set("run", "0")
	}
	result.RawQuery = query.Encode()
	return result.String(), nil
}

func parsePublicBaseURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || parsed.Hostname() == "" ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("alerts: a public base URL is required before enabling alerts")
	}
	cloned := *parsed
	return &cloned, nil
}
