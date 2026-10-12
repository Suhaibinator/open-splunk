// Package delegationtest holds test fixtures shared by the packages that register go-common's
// library delegation tools without running them.
package delegationtest

import (
	"context"
	"testing"

	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/stretchr/testify/require"
)

// ToolRegistry is the registration side of an agent tool registry.
type ToolRegistry interface {
	RegisterTool(ports.AgentToolDefinition) error
}

// RegisterNoopTools registers the four library delegation tools on registry with executors that
// return an empty result, as go-common's runtime registers them before any agent is declared.
func RegisterNoopTools(t testing.TB, registry ToolRegistry) {
	t.Helper()
	noop := func(context.Context, ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
		return &ports.AgentToolResult{}, nil
	}
	for _, def := range botpkg.NewDelegationToolDefinitions(botpkg.DelegationToolExecutors{Read: noop, Send: noop, Spawn: noop, Status: noop, Cancel: noop, Report: noop}) {
		require.NoError(t, registry.RegisterTool(def))
	}
}
