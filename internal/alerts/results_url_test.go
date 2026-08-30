package alerts

import "testing"

func TestResultsURLsPreserveConfiguredBasePath(t *testing.T) {
	t.Parallel()
	job, err := SearchJobResultsURL("https://splunk.example.test/prefix/?tenant=one", "job-1")
	if err != nil || job != "https://splunk.example.test/prefix/search/?searchJobId=job-1&tenant=one" {
		t.Fatalf("SearchJobResultsURL() = %q, %v", job, err)
	}
	test, err := TestWebhookResultsURL("https://splunk.example.test/prefix", "delivery-1")
	if err != nil || test != "https://splunk.example.test/prefix/search/?run=0&searchJobId=test-delivery-1" {
		t.Fatalf("TestWebhookResultsURL() = %q, %v", test, err)
	}
}

func TestResultsURLsRejectUnsafeBaseOrIdentity(t *testing.T) {
	t.Parallel()
	for _, base := range []string{"", "/relative", "ftp://splunk.example.test", "https://user@splunk.example.test", "https://splunk.example.test/#fragment"} {
		if err := ValidatePublicBaseURL(base); err == nil {
			t.Fatalf("ValidatePublicBaseURL(%q) unexpectedly succeeded", base)
		}
	}
	if _, err := SearchJobResultsURL("https://splunk.example.test", " job-1 "); err == nil {
		t.Fatal("SearchJobResultsURL() accepted a noncanonical ID")
	}
}
