package ai

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// cacheContextHydration shares successful immutable attachment reads between checkpoint
// validation and the fresh post-commit rebuild. All authorization and size bindings are keys;
// the cache lives for one maintenance operation, never across model calls or lease changes.
func cacheContextHydration(opts *AgentContextOptions) func() {
	media, state := opts.ToolMediaResolver, opts.ProviderStateResolver
	if media != nil {
		type key struct {
			attachment, owner, run, ceiling uint64
			call                            string
			index                           int
			mime, filename                  string
		}
		values := map[key]ports.ResolvedToolMedia{}
		opts.ToolMediaResolver = func(ctx context.Context, attachment, owner, run uint64, call string, index int, mime, filename string, ceiling uint64) (*ports.ResolvedToolMedia, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			k := key{attachment, owner, run, ceiling, call, index, mime, filename}
			if value, ok := values[k]; ok {
				return &value, nil
			}
			value, err := media(ctx, attachment, owner, run, call, index, mime, filename, ceiling)
			if err == nil && value != nil {
				values[k] = *value
			}
			return value, err
		}
	}
	if state != nil {
		type key struct {
			attachment, owner, run, ceiling uint64
			step                            uint32
		}
		values := map[key][]byte{}
		opts.ProviderStateResolver = func(ctx context.Context, attachment, owner, run uint64, step uint32, ceiling uint64) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			k := key{attachment, owner, run, ceiling, step}
			if value, ok := values[k]; ok {
				return append([]byte(nil), value...), nil
			}
			value, err := state(ctx, attachment, owner, run, step, ceiling)
			if err == nil {
				values[k] = append([]byte(nil), value...)
			}
			return value, err
		}
	}
	return func() { opts.ToolMediaResolver, opts.ProviderStateResolver = media, state }
}
