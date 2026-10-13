package common_persistence

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// chatAgentRunsOneActivePerSessionIndex allows at most one RUNNING, WAITING_FOR_APPROVAL or
// RETRYABLE run per agent session. Start paths check the active count under the session lock, so
// the index is a backstop that never aborts a seal or a cancel.
const chatAgentRunsOneActivePerSessionIndex = "idx_chat_agent_runs_one_active_per_session"

// ensureChatAgentInteractionSchema installs the constraints and indexes of agent-session input
// modes that GORM tags cannot express. It runs under the schema bootstrap lock after AutoMigrate,
// and every step is idempotent.
func chatAgentInteractionSchemaStatements() []string {
	snapshots := common_persistence_models.TableNameChatAgentSessionConfigSnapshots
	sessions := common_persistence_models.TableNameChatAgentSessions
	runs := common_persistence_models.TableNameChatAgentRuns
	messages := common_persistence_models.TableNameChatMessages
	queued := common_persistence_models.TableNameChatAgentQueuedInputs

	statements := []string{
		addCheckConstraintOnce(messages, "chk_chat_messages_kind_specified", "kind BETWEEN 1 AND 7"),
		addCheckConstraintOnce(messages, "chk_chat_messages_role_specified", "role BETWEEN 1 AND 4"),
		// Snapshots store the effective mode. Rows written before the policy existed took the
		// IMMEDIATE column default when AutoMigrate added the column.
		addCheckConstraintOnce(snapshots, "chk_chat_agent_cfg_snapshots_input_mode_specified",
			fmt.Sprintf("input_mode <> %d", int(proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_UNSPECIFIED))),
		// A suspension names when, by whom, and which run together, or not at all.
		addCheckConstraintOnce(sessions, "chk_chat_agent_sessions_suspension_complete",
			"(suspended_at IS NULL) = (suspended_by_user_id IS NULL) AND (suspended_at IS NULL) = (suspended_run_id IS NULL)"),

		// Stop authorization and the post-commit publishing of a drained run find the messages
		// queued input became when it was delivered into a run.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_messages_admitted_run ON %s (admitted_run_id) WHERE admitted_run_id IS NOT NULL", messages),
		// One ending drains the session's queued input into at most one follow-up run.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_runs_input_drained_from_run ON %s (input_drained_from_run_id) WHERE input_drained_from_run_id IS NOT NULL", runs),
		// Idempotent sends: one message per sender, channel and client key, soft-deleted ones
		// included, so a retry after a delete cannot send the message again. A queued input holds
		// its key under the same scope until delivery moves the key to its message.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_messages_client_message_id ON %s (sender_id, channel_id, client_message_id) WHERE client_message_id IS NOT NULL", messages),
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_queued_inputs_client_message_id ON %s (sender_id, channel_id, client_message_id) WHERE client_message_id IS NOT NULL", queued),
	}
	activeValues := make([]string, len(ActiveChatAgentRunStatuses()))
	for i, status := range ActiveChatAgentRunStatuses() {
		activeValues[i] = strconv.Itoa(int(status))
	}
	return append(statements, fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (agent_session_id) WHERE status IN (%s)", chatAgentRunsOneActivePerSessionIndex, runs, strings.Join(activeValues, ", ")))
}

func (h *CommonPersistenceHandler[U]) ensureChatAgentInteractionSchema(db *gorm.DB) error {
	for _, statement := range chatAgentInteractionSchemaStatements() {
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}
	return nil
}

// addCheckConstraintOnce returns a statement that adds a named CHECK constraint unless the table
// already has it, so repeated startups do not rescan the table.
func addCheckConstraintOnce(table, name, predicate string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", table, name, predicate)
}
