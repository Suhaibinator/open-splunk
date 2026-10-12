package common_service_models

// ChatAgentSessionAutoRunPolicy controls whether ordinary human messages on an agent session's
// main timeline automatically create and drive a follow-up run. The zero value intentionally keeps
// the historical behavior so existing callers and persisted sessions remain automatic.
type ChatAgentSessionAutoRunPolicy uint8

const (
	// ChatAgentSessionAutoRunPolicyAutomatic creates a follow-up run for each eligible human
	// main-timeline message.
	ChatAgentSessionAutoRunPolicyAutomatic ChatAgentSessionAutoRunPolicy = iota
	// ChatAgentSessionAutoRunPolicyDisabled leaves follow-up-run creation to the consuming app. The
	// optional initial message supplied to CreateAgentChatSession still creates its initial run.
	ChatAgentSessionAutoRunPolicyDisabled
)
