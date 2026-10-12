package common_persistence

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
)

type organizationOwnedTable struct {
	table           string
	checkConstraint string
	foreignKey      string
}

var chatAgentOrganizationOwnedTables = []organizationOwnedTable{
	{
		table:           common_persistence_models.TableNameChatAgentSessions,
		checkConstraint: "chk_chat_agent_sessions_organization_positive",
		foreignKey:      "fk_chat_agent_sessions_organization",
	},
	{
		table:           common_persistence_models.TableNameChatAgentRuns,
		checkConstraint: "chk_chat_agent_runs_organization_positive",
		foreignKey:      "fk_chat_agent_runs_organization",
	},
	{
		table:           common_persistence_models.TableNameChatInferenceCalls,
		checkConstraint: "chk_chat_inference_calls_organization_positive",
		foreignKey:      "fk_chat_inference_calls_organization",
	},
}

var inferenceAttemptOrganizationOwnedTables = []organizationOwnedTable{
	{
		table:           common_persistence_models.TableNameInferenceAttempts,
		checkConstraint: "chk_inference_attempts_organization_positive",
		foreignKey:      "fk_inference_attempts_organization",
	},
}

// ensureChatAgentOrganizationConstraints protects every durable agent ownership snapshot. The
// constraints validate every persisted row and enforce ownership on future writes.
func (h *CommonPersistenceHandler[U]) ensureChatAgentOrganizationConstraints(db *gorm.DB) error {
	return ensureOrganizationOwnershipConstraints(db, chatAgentOrganizationOwnedTables)
}

// ensureInferenceAttemptOrganizationConstraints protects the global per-attempt ledger using the
// same ownership invariant as the agent ledgers.
func (h *CommonPersistenceHandler[U]) ensureInferenceAttemptOrganizationConstraints(db *gorm.DB) error {
	return ensureOrganizationOwnershipConstraints(db, inferenceAttemptOrganizationOwnedTables)
}

func ensureOrganizationOwnershipConstraints(db *gorm.DB, tables []organizationOwnedTable) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, owned := range tables {
			constraints := []sharedConstraint{
				{owned.table, owned.checkConstraint, "CHECK (organization_id IS NOT NULL AND organization_id > 0)"},
				{owned.table, owned.foreignKey, "FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT"},
			}
			for _, constraint := range constraints {
				if err := executeSharedConstraint(tx, constraint); err != nil {
					return fmt.Errorf("install organization ownership constraint on %s: %w", owned.table, err)
				}
			}
		}
		return nil
	})
}
