package chat

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// FormatDelegationStateBlock renders a parent's delegation state as the live-state block appended to
// its prompt. It is empty for a session without children, so callers can append it unconditionally.
//
// The runtime appends the block only when its text changes, so it renders only facts whose change
// is a real state change: each child's session status, latest run id, status and reason, the time
// that run ended, and the last report with its time, plus the pending-wake count. Timestamps that
// move while nothing happens (a running run's heartbeat-driven updated_at, the session's
// updated_at) are deliberately left out; rendering them would append a new copy of the block
// before nearly every parent step. Children are listed by session id with UTC timestamps, so the
// text is deterministic for a given state.
//
// The formatter renders every non-nil child it is given and sorts a copy, never state.Children.
// Bounding the list (chat_agent_delegation_state_max_children) is the delegation state read's job:
// it selects children with active runs. When
// TotalChildren exceeds the rendered children, the block reports a lower bound on those left out.
// Each child's last report is cut to state.ReportExcerptRunes (chat_agent_delegation_report_excerpt_runes,
// sampled by the read that built the state).
func FormatDelegationStateBlock(state *DelegationState) string {
	if state == nil || len(state.Children) == 0 {
		return ""
	}
	children := make([]*DelegatedChild, 0, len(state.Children))
	for _, child := range state.Children {
		if child != nil && child.Session != nil {
			children = append(children, child)
		}
	}
	if len(children) == 0 {
		return ""
	}
	sort.SliceStable(children, func(i, j int) bool { return children[i].Session.ID < children[j].Session.ID })
	total := max(int(state.TotalChildren), len(state.Children))
	omitted := total - len(children)

	var b strings.Builder
	active := 0
	for _, child := range children {
		if delegatedChildActive(child) {
			active++
		}
	}
	if omitted > 0 {
		fmt.Fprintf(&b, "Delegated sub-agents (at least %d, %d active):\n", total, active)
	} else {
		fmt.Fprintf(&b, "Delegated sub-agents (%d, %d active):\n", total, active)
	}
	excerptRunes := state.ReportExcerptRunes
	for _, child := range children {
		writeDelegatedChildLine(&b, child, excerptRunes)
	}
	if omitted > 0 {
		fmt.Fprintf(&b, "- at least %d additional active sub-agent(s) not shown\n", omitted)
	}
	if state.PendingWakeCount > 0 {
		fmt.Fprintf(&b, "Pending wakes: %d (sub-agent updates that arrived while you were running; their messages are above)\n", state.PendingWakeCount)
	}
	return strings.TrimRight(b.String(), "\n")
}

func delegatedChildActive(child *DelegatedChild) bool {
	return child.LatestRun != nil && !persistence.IsTerminalChatAgentRunStatus(child.LatestRun.Status)
}

func writeDelegatedChildLine(b *strings.Builder, child *DelegatedChild, excerptRunes int) {
	session := child.Session
	fmt.Fprintf(b, "- thread %d | %s | session %d", session.ParentMessageID, session.AgentType, session.ID)
	if child.IsArchived() {
		b.WriteString(" | archived")
	}
	if run := child.LatestRun; run != nil {
		fmt.Fprintf(b, " | run %d %s", run.ID, child.runStatusLabel())
		if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
			fmt.Fprintf(b, " | ended %s", run.CompletedAt.UTC().Format(time.RFC3339))
		}
	} else {
		b.WriteString(" | no run")
	}
	if report := child.LastReport; report != nil {
		if excerpt := child.ReportExcerpt(excerptRunes); excerpt != "" {
			if !report.CreatedAt.IsZero() {
				fmt.Fprintf(b, " | last report %s: %q", report.CreatedAt.UTC().Format(time.RFC3339), excerpt)
			} else {
				fmt.Fprintf(b, " | last report: %q", excerpt)
			}
		}
	}
	b.WriteByte('\n')
}

// IsArchived reports whether the child session was archived (cancelled by its parent, or retired
// with it).
func (c *DelegatedChild) IsArchived() bool {
	return c != nil && c.Session != nil && c.Session.Status == sm.ChatAgentSessionStatusArchived
}

// RunStatusName is the latest run's status as the model sees it: the enum name without its prefix,
// lower case ("running", "failed"). It is empty when the child has no run.
func (c *DelegatedChild) RunStatusName() string {
	if c == nil || c.LatestRun == nil {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(c.LatestRun.Status.String(), "CHAT_AGENT_RUN_STATUS_"))
}

// RunReasonName is the latest run's status reason in the same casing ("model_step_limit"), or empty
// when the child has no run or the reason is unspecified.
func (c *DelegatedChild) RunReasonName() string {
	if c == nil || c.LatestRun == nil || c.LatestRun.StatusReason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(c.LatestRun.StatusReason.String(), "CHAT_AGENT_RUN_STATUS_REASON_"))
}

// runStatusLabel renders "running", "failed (model_step_limit)", or
// "cancelled (delegation_cancelled_by_parent)": the status plus the code or reason when one was
// recorded.
func (c *DelegatedChild) runStatusLabel() string {
	label := c.RunStatusName()
	if code := c.LatestRun.StatusReasonCode; code != "" {
		return label + " (" + code + ")"
	}
	if reason := c.RunReasonName(); reason != "" {
		return label + " (" + reason + ")"
	}
	return label
}

// ReportExcerpt is the last report on one line: whitespace collapsed and, past limit runes, cut to
// limit-1 runes plus an ellipsis. It is empty when there is no report or it is blank.
func (c *DelegatedChild) ReportExcerpt(limit int) string {
	if c == nil || c.LastReport == nil {
		return ""
	}
	joined := strings.Join(strings.FieldsFunc(c.LastReport.Content, unicode.IsSpace), " ")
	if utf8.RuneCountInString(joined) <= limit {
		return joined
	}
	return common_util.TruncateRunes(joined, limit-1) + "…"
}
