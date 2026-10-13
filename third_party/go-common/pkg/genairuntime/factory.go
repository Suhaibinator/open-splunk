// Package genairuntime connects application-owned inference snapshots to runtime clients.
package genairuntime

import (
	"context"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai"
)

type CredentialBinding = config.CredentialBinding
type Snapshot = genai.RuntimeFactoryConfig
type Source func() (Snapshot, error)

// NewFactory samples source once per resolution; applications must capture their KMS generation once in source.
// Options such as genai.WithProviderTransport configure provider runtimes.
func NewFactory(ctx context.Context, source Source, opts ...genai.RuntimeFactoryOption) (*genai.ModelRuntimeFactory, error) {
	return genai.NewReloadingModelRuntimeFactory(ctx, source, opts...)
}
