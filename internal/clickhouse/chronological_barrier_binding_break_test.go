package clickhouse

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func chronologicalBarrierBreakPending(name string) *pendingChronologicalBarrier {
	return &pendingChronologicalBarrier{
		name:                    name,
		sql:                     "SELECT ? AS " + quoteIdentifier(name),
		prerequisiteDefinitions: []string{"def_" + name},
		validationColumns:       []string{"col_" + name},
		fanout:                  3,
		depth:                   2,
		ownerRange:              spl.Range{},
	}
}

// TestBindChronologicalBarrierTransfersArgumentOwnership pins the two-state
// contract the extracted helper replaced eight inline copies of: a pending
// barrier takes every argument accumulated since the previous barrier and
// leaves the caller with none, while a nil barrier is a pure pass-through. If
// the helper ever returned the arguments as well as binding them, every bind
// value since the last barrier would be sent twice.
func TestBindChronologicalBarrierTransfersArgumentOwnership(t *testing.T) {
	t.Parallel()

	base := compileState{}
	args := []any{"tenant-1", uint64(73), int64(-1)}

	passthroughState, passthroughArgs := bindChronologicalBarrier(base, nil, args)
	if !reflect.DeepEqual(passthroughArgs, args) {
		t.Fatalf("nil barrier returned %#v, want the untouched arguments %#v", passthroughArgs, args)
	}
	if len(passthroughState.chronologicalBarriers) != 0 {
		t.Fatalf("nil barrier appended %d barriers", len(passthroughState.chronologicalBarriers))
	}

	boundState, boundArgs := bindChronologicalBarrier(base, chronologicalBarrierBreakPending("first"), args)
	if boundArgs != nil {
		t.Fatalf("bound barrier left %#v with the caller", boundArgs)
	}
	if len(boundState.chronologicalBarriers) != 1 {
		t.Fatalf("bound barriers = %d, want 1", len(boundState.chronologicalBarriers))
	}
	if !reflect.DeepEqual(boundState.chronologicalBarriers[0].args, args) {
		t.Fatalf("barrier args = %#v, want %#v", boundState.chronologicalBarriers[0].args, args)
	}
	if len(base.chronologicalBarriers) != 0 {
		t.Fatalf("the caller's state was mutated in place: %#v", base.chronologicalBarriers)
	}

	// The barrier owns a copy: mutating the caller's slice afterwards must not
	// rewrite an already-sealed bind list.
	args[0] = "tenant-forged"
	if boundState.chronologicalBarriers[0].args[0] != "tenant-1" {
		t.Fatalf("barrier aliased the caller's argument slice: %#v", boundState.chronologicalBarriers[0].args)
	}
	// An empty argument run still seals a barrier rather than being skipped.
	emptyState, emptyArgs := bindChronologicalBarrier(base, chronologicalBarrierBreakPending("empty"), nil)
	if emptyArgs != nil || len(emptyState.chronologicalBarriers) != 1 {
		t.Fatalf("empty-run barrier = %#v / %#v", emptyState.chronologicalBarriers, emptyArgs)
	}
}

// TestBindChronologicalBarrierAccumulatesTheChainInOrder pins the accumulation
// the compiler's linear operator walk depends on: each bind appends exactly one
// barrier at the end, earlier barriers keep their own sealed argument runs, and
// a slice with spare capacity does not let a later bind rewrite an earlier one.
// The chain's order is the order the validation wrapper replays the stages in.
func TestBindChronologicalBarrierAccumulatesTheChainInOrder(t *testing.T) {
	t.Parallel()

	// Spare capacity is the interesting case: a wrong index would silently
	// overwrite a sealed stage instead of growing the chain.
	state := compileState{chronologicalBarriers: make([]compiledChronologicalBarrier, 0, 8)}
	names := []string{"first", "second", "third"}
	for index, name := range names {
		var args []any
		if index > 0 {
			args = []any{name + "-arg"}
		}
		var leftover []any
		state, leftover = bindChronologicalBarrier(state, chronologicalBarrierBreakPending(name), args)
		if leftover != nil {
			t.Fatalf("%q left %#v with the caller", name, leftover)
		}
		if got := len(state.chronologicalBarriers); got != index+1 {
			t.Fatalf("after %q the chain holds %d barriers, want %d", name, got, index+1)
		}
	}
	for index, name := range names {
		barrier := state.chronologicalBarriers[index]
		if barrier.name != name {
			t.Fatalf("chain[%d] = %q, want %q", index, barrier.name, name)
		}
		if index == 0 {
			if len(barrier.args) != 0 {
				t.Fatalf("chain[0] args = %#v, want the empty run", barrier.args)
			}
			continue
		}
		if !reflect.DeepEqual(barrier.args, []any{name + "-arg"}) {
			t.Fatalf("chain[%d] args = %#v, want the run sealed at %q", index, barrier.args, name)
		}
	}
	// Interleaving a nil barrier must not disturb the sealed chain.
	unchanged, carried := bindChronologicalBarrier(state, nil, []any{"carried"})
	if !reflect.DeepEqual(carried, []any{"carried"}) ||
		len(unchanged.chronologicalBarriers) != len(names) {
		t.Fatalf("a nil barrier disturbed the chain: %#v / %#v", unchanged.chronologicalBarriers, carried)
	}
}

// TestChronologicalBarrierArgumentsStayBalancedAcrossBarrierCounts compiles
// pipelines with zero, one, and several chronological barriers and requires the
// finished query to bind exactly as many values as it has placeholders. Every
// barrier is an argument-ownership transfer, so a query with more barriers is a
// query with more chances to double-count or drop a bind value.
func TestChronologicalBarrierArgumentsStayBalancedAcrossBarrierCounts(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | where status="ok"`,
		`index=gradethis | eventstats min(payload) AS low BY host`,
		`index=gradethis | eventstats max(payload) AS high BY host | where high>"a"`,
		`index=gradethis | eventstats min(payload) AS low BY host | eventstats max(payload) AS high BY host`,
		`index=gradethis | eventstats values(user) AS users BY host | eventstats dc(user) AS unique BY host | sort 0 +host`,
		`index=gradethis | streamstats count AS running BY host | eventstats max(running) AS peak BY host`,
		`index=gradethis | eval copied=status | eventstats min(payload) AS low BY copied | table copied low`,
	} {
		compiled := compileSPL(t, source)
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("%q placeholders = %d, args = %d\nargs: %#v\nSQL: %s", source, got, want, compiled.Args, compiled.SQL)
		}
		// A barrier's prerequisite stage must appear before whatever consumes
		// it; a lost ownership transfer usually shows up as an orphan CTE.
		if strings.Contains(compiled.SQL, "?)?") {
			t.Fatalf("%q emitted adjacent placeholders, a bind-list desynchronization:\n%s", source, compiled.SQL)
		}
	}
}
