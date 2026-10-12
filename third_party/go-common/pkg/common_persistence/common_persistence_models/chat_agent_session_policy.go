package common_persistence_models

// ChatAgentSessionAutoRunPolicy is the persisted policy controlling whether human main-timeline
// messages automatically create follow-up runs. It is persistence-local because agent-session
// lifecycle/configuration is a Go service contract rather than a public proto contract.
type ChatAgentSessionAutoRunPolicy uint8

const (
	// ChatAgentSessionAutoRunPolicyAutomatic is zero for backward compatibility with rows created
	// before the column existed.
	ChatAgentSessionAutoRunPolicyAutomatic ChatAgentSessionAutoRunPolicy = iota
	// ChatAgentSessionAutoRunPolicyDisabled suppresses generic follow-up-run creation.
	ChatAgentSessionAutoRunPolicyDisabled
)
