package knowledgedefinition

import (
	"errors"
	"strings"
)

// IssueCode is a stable candidate-definition validation category. The three
// values deliberately mirror Normalize's existing errors.Is taxonomy rather
// than exposing lower-level implementation errors as a new public contract.
type IssueCode string

const (
	IssueCodeInvalidDefinition IssueCode = "KNOWLEDGE_DEFINITION_INVALID"
	IssueCodeUnknownField      IssueCode = "KNOWLEDGE_DEFINITION_UNKNOWN_FIELD"
	IssueCodeResourceLimit     IssueCode = "KNOWLEDGE_DEFINITION_RESOURCE_LIMIT"
)

// Issue is one deterministic, candidate-actionable normalization failure.
// FieldPath is relative to KnowledgeObjectDefinition. An empty path identifies
// the definition message itself. Issue contains no caller-owned mutable state.
//
// Normalize remains fail-fast, so one returned error carries at most one Issue.
type Issue struct {
	FieldPath string
	Code      IssueCode
	Message   string
}

// issueError keeps the legacy user-facing text and errors.Is root independent
// from the structured issue. In particular, the lower-level cause is not
// unwrapped: Normalize historically formatted those causes with %v, and making
// them newly discoverable would change its error taxonomy.
type issueError struct {
	root  error
	text  string
	issue Issue
}

func (err *issueError) Error() string {
	if err == nil {
		return ""
	}
	return err.text
}

func (err *issueError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.root
}

// IssueFromError returns a detached structured issue when err contains one.
// Infrastructure, invariant, canonical-storage, and other non-candidate
// failures deliberately return false even when they share a legacy sentinel.
func IssueFromError(err error) (Issue, bool) {
	var detailed *issueError
	if !errors.As(err, &detailed) || detailed == nil {
		return Issue{}, false
	}
	return Issue{
		FieldPath: strings.Clone(detailed.issue.FieldPath),
		Code:      IssueCode(strings.Clone(string(detailed.issue.Code))),
		Message:   strings.Clone(detailed.issue.Message),
	}, true
}

func newIssueError(root error, text string, issue Issue) error {
	return &issueError{
		root: root,
		text: text,
		issue: Issue{
			FieldPath: strings.Clone(issue.FieldPath),
			Code:      IssueCode(strings.Clone(string(issue.Code))),
			Message:   strings.Clone(issue.Message),
		},
	}
}
