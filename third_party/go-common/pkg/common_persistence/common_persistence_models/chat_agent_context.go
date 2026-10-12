package common_persistence_models

// ChatAgentContext stores private model context separately from operational run metadata.
// Session instructions live on ChatAgentSession, independently of configuration versions.
type ChatAgentContext struct {
	AgentSession            *ChatAgentSession `gorm:"foreignKey:AgentSessionID;constraint:OnDelete:CASCADE"`
	AgentSessionID          uint64            `gorm:"primaryKey;autoIncrement:false"`
	ConfigVersion           uint64            `gorm:"primaryKey;autoIncrement:false"`
	ToolManifestSHA256      string
	WorkflowState           string
	WorkflowStateMessageID  uint64
	ManualCompactionRunID   uint64
	ObservationRunID        uint64
	ObservedInputTokens     int
	ObservedEstimate        int
	ObservedCheckpointID    uint64
	ObservedCallID          string
	ObservationPrefixSHA256 string
}

func (ChatAgentContext) TableName() string { return "gocommon_chat_agent_contexts" }

// ChatContextOperation is the durable idempotency record for a checkpoint or context notice.
// A zero MessageID is a prepared operation; inference happens outside its transaction.
type ChatContextOperation struct {
	OmittedMessageIDsJSON  []byte        `gorm:"type:bytea"`
	AgentRun               *ChatAgentRun `gorm:"foreignKey:AgentRunID;constraint:OnDelete:CASCADE"`
	Key                    string        `gorm:"primaryKey;size:128"`
	AgentRunID             uint64        `gorm:"index"`
	PredecessorMessageID   uint64
	CoversThroughMessageID uint64
	MessageID              uint64
}

func (ChatContextOperation) TableName() string { return "gocommon_chat_context_operations" }
