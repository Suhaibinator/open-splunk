package collector

import "github.com/Suhaibinator/open-splunk/internal/collector/input"

// inputFileKey scopes a file key to the configured input that owns it. A
// struct pair is deliberate: joining the two strings would permit ambiguous
// keys when either value contains the chosen delimiter.
type inputFileKey struct {
	inputID string
	fileKey string
}

// inputFileTrackingKey addresses the physical file independently of content
// generation so a truncation can supersede the prior generation for this input.
func inputFileTrackingKey(inputID string, identity input.FileIdentity) inputFileKey {
	return inputFileKey{inputID: inputID, fileKey: identity.TrackingKey()}
}

// inputFileGenerationKey addresses one exact content generation and is used
// only for in-memory monotonicity checks.
func inputFileGenerationKey(inputID string, identity input.FileIdentity) inputFileKey {
	return inputFileKey{inputID: inputID, fileKey: identity.String()}
}
