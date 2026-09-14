package clickhouse

import "context"

// A Dynamic cell may contain millions of members even in a single row. Poll
// by visited nodes so cancellation does not depend on the outer row count.
// The owning phase also checks its context before returning a completed value.
type relationTraversal struct {
	ctx   context.Context
	nodes uint64
	err   error
}

func (walk *relationTraversal) step() bool {
	if walk.nodes&255 == 0 {
		walk.err = walk.ctx.Err()
	}
	walk.nodes++
	return walk.err == nil
}
