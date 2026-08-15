package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

func TestEnumeratePublicationIndexORSignaturesMatchesBruteForce(t *testing.T) {
	for seed := uint64(1); seed <= 96; seed++ {
		generator := seed*0x9e3779b97f4a7c15 + 0xd1b54a32d192ed03
		atomCount := int(seed%8) + 1
		atoms := make([]publicationIndexAtom, atomCount)
		for atomIndex := range atoms {
			for ordinal := range 12 {
				generator ^= generator << 13
				generator ^= generator >> 7
				generator ^= generator << 17
				if generator&1 != 0 {
					publicationIndexTestSet(&atoms[atomIndex].before, ordinal)
				}
				if generator&2 != 0 {
					publicationIndexTestSet(&atoms[atomIndex].after, ordinal)
				}
			}
		}
		// Exercise both a real zero-membership index and duplicate atoms in
		// deterministic portions of the matrix.
		if seed%3 == 0 {
			atoms[0] = publicationIndexAtom{}
		}
		if seed%5 == 0 && len(atoms) > 1 {
			atoms[len(atoms)-1] = atoms[0]
		}

		got, err := enumeratePublicationIndexORSignatures(t.Context(), atoms)
		if err != nil {
			t.Fatalf("seed %d closure: %v", seed, err)
		}
		want := bruteForcePublicationIndexORSignatures(atoms)
		if !slices.Equal(got, want) {
			t.Fatalf(
				"seed %d closure differs from brute force\n got: %#v\nwant: %#v",
				seed,
				got,
				want,
			)
		}
	}
}

func TestEnumeratePublicationIndexORSignaturesIncludesActualZeroAndDeduplicates(t *testing.T) {
	zero := publicationIndexAtom{}
	nonzero := publicationIndexAtom{
		before: publicationIndexTestBits(0, 65, MaximumResolutionCandidates-1),
		after:  publicationIndexTestBits(1, 66),
	}
	input := []publicationIndexAtom{zero, nonzero, zero, nonzero}
	got, err := enumeratePublicationIndexORSignatures(t.Context(), input)
	if err != nil {
		t.Fatalf("enumerate closure: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("closure state count = %d, want 2: %#v", len(got), got)
	}
	if got[0] != (publicationIndexORSignature{minimumIndexes: 1}) {
		t.Fatalf("zero-membership actual index state = %#v, want zero/zero witness 1", got[0])
	}
	if got[1].before != nonzero.before || got[1].after != nonzero.after ||
		got[1].minimumIndexes != 1 {
		t.Fatalf("nonzero deduplicated state = %#v, want atom with witness 1", got[1])
	}
}

func TestEnumeratePublicationIndexORSignaturesUsesExactMinimumWitnesses(t *testing.T) {
	atoms := []publicationIndexAtom{
		{before: publicationIndexTestBits(0)},
		{after: publicationIndexTestBits(0)},
		{before: publicationIndexTestBits(1), after: publicationIndexTestBits(1)},
	}
	got, err := enumeratePublicationIndexORSignatures(t.Context(), atoms)
	if err != nil {
		t.Fatalf("enumerate closure: %v", err)
	}
	for index := 1; index < len(got); index++ {
		if comparePublicationIndexORSignatures(got[index-1], got[index]) >= 0 {
			t.Fatalf("states %d and %d are not in strict binary order", index-1, index)
		}
	}

	wantPair := publicationIndexORPair{
		before: publicationIndexTestBits(0),
		after:  publicationIndexTestBits(0),
	}
	state, found := publicationIndexTestFind(got, wantPair)
	if !found || state.minimumIndexes != 2 {
		t.Fatalf("independent before/after union = (%#v, %t), want witness 2", state, found)
	}
	fullPair := publicationIndexORPair{
		before: publicationIndexTestBits(0, 1),
		after:  publicationIndexTestBits(0, 1),
	}
	state, found = publicationIndexTestFind(got, fullPair)
	if !found || state.minimumIndexes != 3 {
		t.Fatalf("full union = (%#v, %t), want witness 3", state, found)
	}
}

func TestNextPublicationIndexWitnessAdmitsExact256AndRejects257(t *testing.T) {
	// An irredundant 256-index witness necessarily creates far more than 1,024
	// subset signatures, so the closure-state ceiling dominates end-to-end.
	// Pin the independent read-scope transition without an explosive fixture.
	maximum := uint16(indexread.MaximumIndexesPerScope)
	if got, ok := nextPublicationIndexWitness(maximum - 1); !ok || got != maximum {
		t.Fatalf("exact maximum witness = (%d, %t), want (%d, true)", got, ok, maximum)
	}
	if got, ok := nextPublicationIndexWitness(maximum); ok || got != 0 {
		t.Fatalf("over-maximum witness = (%d, %t), want (0, false)", got, ok)
	}
	if got, ok := nextPublicationIndexWitness(math.MaxUint16); ok || got != 0 {
		t.Fatalf("overflowing witness = (%d, %t), want (0, false)", got, ok)
	}
}

func TestEnumeratePublicationIndexORSignaturesEnforcesFixedBounds(t *testing.T) {
	if publicationIndexMembershipWordCount != 64 {
		t.Fatalf("fixed membership words = %d, want 64", publicationIndexMembershipWordCount)
	}
	var nilContext context.Context
	if _, err := enumeratePublicationIndexORSignatures(nilContext, nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil context error = %v, want InvalidArgument", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := enumeratePublicationIndexORSignatures(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context error = %v, want context.Canceled", err)
	}

	maximumDuplicates := make([]publicationIndexAtom, maximumPublicationIndexAtoms)
	got, err := enumeratePublicationIndexORSignatures(t.Context(), maximumDuplicates)
	if err != nil || len(got) != 1 || got[0].minimumIndexes != 1 {
		t.Fatalf("exact atom boundary = (%#v, %v), want one zero state", got, err)
	}
	overAtoms := make([]publicationIndexAtom, maximumPublicationIndexAtoms+1)
	if _, err := enumeratePublicationIndexORSignatures(t.Context(), overAtoms); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over atom boundary error = %v, want CapacityExceeded", err)
	}

	// Eleven independent atoms have 2^11-1 distinct nonempty unions and must
	// fail before retaining state 1,025.
	tooManyStates := make([]publicationIndexAtom, 11)
	for index := range tooManyStates {
		publicationIndexTestSet(&tooManyStates[index].before, index)
	}
	if _, err := enumeratePublicationIndexORSignatures(t.Context(), tooManyStates); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over state boundary error = %v, want CapacityExceeded", err)
	}
}

func TestEnumeratePublicationIndexORSignaturesRejectsJoinClosedWorkAmplification(t *testing.T) {
	// All subsets of ten independent bits form 1,024 distinct atoms whose OR
	// closure is exactly the same 1,024-element set. The state ceiling therefore
	// cannot reject this inventory, but expanding its Cartesian product must stop
	// at the independent work ceiling.
	atoms := make([]publicationIndexAtom, maximumPublicationIndexAtoms)
	for subset := range atoms {
		for bit := range 10 {
			if subset&(1<<bit) != 0 {
				publicationIndexTestSet(&atoms[subset].before, bit)
				publicationIndexTestSet(&atoms[subset].after, bit+64)
			}
		}
	}
	_, err := enumeratePublicationIndexORSignatures(t.Context(), atoms)
	if !errors.Is(err, control.ErrCapacityExceeded) ||
		!strings.Contains(err.Error(), "work limit") {
		t.Fatalf("join-closed work amplification error = %v, want CapacityExceeded work limit", err)
	}
}

func TestPublicationIndexClosureBudgetChecksProbeAndWordCeilings(t *testing.T) {
	exact := publicationIndexClosureBudget{
		probes: maximumPublicationIndexClosureProbes - 1,
		wordOperations: maximumPublicationIndexClosureWordOperations -
			publicationIndexClosureWordsPerProbe,
	}
	if err := exact.charge(); err != nil {
		t.Fatalf("exact work boundary: %v", err)
	}
	if exact.probes != maximumPublicationIndexClosureProbes ||
		exact.wordOperations != maximumPublicationIndexClosureWordOperations {
		t.Fatalf("exact work counters = %#v", exact)
	}
	if err := exact.charge(); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over probe boundary error = %v, want CapacityExceeded", err)
	}

	overWords := publicationIndexClosureBudget{
		wordOperations: maximumPublicationIndexClosureWordOperations -
			publicationIndexClosureWordsPerProbe + 1,
	}
	if err := overWords.charge(); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over word-work boundary error = %v, want CapacityExceeded", err)
	}
	if err := (*publicationIndexClosureBudget)(nil).charge(); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("nil budget error = %v, want CapacityExceeded", err)
	}
}

func TestEnumeratePublicationIndexORSignaturesObservesMidWorkCancellation(t *testing.T) {
	base, cancel := context.WithCancel(t.Context())
	ctx := &publicationIndexCancelContext{
		Context:  base,
		cancel:   cancel,
		cancelAt: 24,
	}
	atoms := make([]publicationIndexAtom, 10)
	for index := range atoms {
		publicationIndexTestSet(&atoms[index].before, index)
		publicationIndexTestSet(&atoms[index].after, index+64)
	}
	if _, err := enumeratePublicationIndexORSignatures(ctx, atoms); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-work cancellation error = %v, want context.Canceled", err)
	}
	if ctx.calls != ctx.cancelAt {
		t.Fatalf("context Err calls = %d, want cancellation at %d", ctx.calls, ctx.cancelAt)
	}
}

func TestEnumeratePublicationIndexORSignaturesDetachesIngressAndResults(t *testing.T) {
	atoms := []publicationIndexAtom{
		{before: publicationIndexTestBits(0), after: publicationIndexTestBits(64)},
		{before: publicationIndexTestBits(1), after: publicationIndexTestBits(65)},
	}
	expected, err := enumeratePublicationIndexORSignatures(t.Context(), slices.Clone(atoms))
	if err != nil {
		t.Fatalf("expected closure: %v", err)
	}
	mutation := &publicationIndexMutationContext{
		Context:  t.Context(),
		mutateAt: 2,
		mutate: func() {
			atoms[0] = publicationIndexAtom{}
			atoms[1].before = publicationIndexTestBits(MaximumResolutionCandidates - 1)
		},
	}
	actual, err := enumeratePublicationIndexORSignatures(mutation, atoms)
	if err != nil {
		t.Fatalf("closure across caller mutation: %v", err)
	}
	if !mutation.mutated || !slices.Equal(actual, expected) {
		t.Fatalf("ingress detachment = mutated %t, equal %t", mutation.mutated, slices.Equal(actual, expected))
	}

	second, err := enumeratePublicationIndexORSignatures(t.Context(), []publicationIndexAtom{
		{before: publicationIndexTestBits(0), after: publicationIndexTestBits(64)},
		{before: publicationIndexTestBits(1), after: publicationIndexTestBits(65)},
	})
	if err != nil {
		t.Fatalf("second closure: %v", err)
	}
	actual[0].before[0] = math.MaxUint64
	actual[0].after[1] = math.MaxUint64
	actual[0].minimumIndexes = math.MaxUint16
	if !slices.Equal(second, expected) {
		t.Fatal("mutating one returned closure aliased another result")
	}
}

func TestEnumeratePublicationIndexORSignaturesIsDeterministicAndConcurrent(t *testing.T) {
	atoms := []publicationIndexAtom{
		{before: publicationIndexTestBits(0, 67), after: publicationIndexTestBits(4)},
		{before: publicationIndexTestBits(1), after: publicationIndexTestBits(5, 129)},
		{},
		{before: publicationIndexTestBits(63), after: publicationIndexTestBits(4095)},
		{before: publicationIndexTestBits(1), after: publicationIndexTestBits(5, 129)},
		{before: publicationIndexTestBits(130), after: publicationIndexTestBits(2)},
	}
	expected, err := enumeratePublicationIndexORSignatures(t.Context(), atoms)
	if err != nil {
		t.Fatalf("expected closure: %v", err)
	}
	for rotation := range atoms {
		permuted := append(slices.Clone(atoms[rotation:]), atoms[:rotation]...)
		slices.Reverse(permuted)
		got, closureErr := enumeratePublicationIndexORSignatures(t.Context(), permuted)
		if closureErr != nil || !slices.Equal(got, expected) {
			t.Fatalf("rotation %d = equal %t, error %v", rotation, slices.Equal(got, expected), closureErr)
		}
	}

	const workers = 8
	errorsByWorker := make(chan error, workers)
	var wait sync.WaitGroup
	testContext := t.Context()
	for worker := range workers {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			got, closureErr := enumeratePublicationIndexORSignatures(testContext, atoms)
			if closureErr != nil {
				errorsByWorker <- closureErr
				return
			}
			if !slices.Equal(got, expected) {
				errorsByWorker <- fmt.Errorf("worker %d returned a different closure", worker)
				return
			}
			got[0].before[0] ^= uint64(worker + 1)
			fresh, closureErr := enumeratePublicationIndexORSignatures(testContext, atoms)
			if closureErr != nil {
				errorsByWorker <- closureErr
				return
			}
			if !slices.Equal(expected, fresh) {
				errorsByWorker <- fmt.Errorf("worker %d result aliased retained state", worker)
			}
		}(worker)
	}
	wait.Wait()
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		t.Error(workerErr)
	}
}

type publicationIndexCancelContext struct {
	context.Context
	cancel   context.CancelFunc
	cancelAt int
	calls    int
}

func (ctx *publicationIndexCancelContext) Err() error {
	ctx.calls++
	if ctx.calls == ctx.cancelAt {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

type publicationIndexMutationContext struct {
	context.Context
	mutateAt int
	calls    int
	mutated  bool
	mutate   func()
}

func (ctx *publicationIndexMutationContext) Err() error {
	ctx.calls++
	if !ctx.mutated && ctx.calls == ctx.mutateAt {
		ctx.mutated = true
		ctx.mutate()
	}
	return ctx.Context.Err()
}

func bruteForcePublicationIndexORSignatures(
	atoms []publicationIndexAtom,
) []publicationIndexORSignature {
	if len(atoms) > 63 {
		panic("brute-force publication index closure received too many atoms")
	}
	minimum := make(map[publicationIndexORPair]uint16)
	for subset := uint64(1); subset < uint64(1)<<len(atoms); subset++ {
		var pair publicationIndexORPair
		for atomIndex, atom := range atoms {
			if subset&(uint64(1)<<atomIndex) == 0 {
				continue
			}
			pair = orPublicationIndexPairs(pair, publicationIndexORPair(atom))
		}
		witness := uint16(bits.OnesCount64(subset))
		if previous, exists := minimum[pair]; !exists || witness < previous {
			minimum[pair] = witness
		}
	}
	result := make([]publicationIndexORSignature, 0, len(minimum))
	for pair, witness := range minimum {
		result = append(result, publicationIndexORSignature{
			before:         pair.before,
			after:          pair.after,
			minimumIndexes: witness,
		})
	}
	slices.SortFunc(result, comparePublicationIndexORSignatures)
	return result
}

func publicationIndexTestBits(ordinals ...int) publicationIndexMembership {
	var result publicationIndexMembership
	for _, ordinal := range ordinals {
		publicationIndexTestSet(&result, ordinal)
	}
	return result
}

func publicationIndexTestSet(membership *publicationIndexMembership, ordinal int) {
	if membership == nil || ordinal < 0 || ordinal >= MaximumResolutionCandidates {
		panic(fmt.Sprintf("invalid publication index test ordinal %d", ordinal))
	}
	membership[ordinal/64] |= uint64(1) << (ordinal % 64)
}

func publicationIndexTestFind(
	states []publicationIndexORSignature,
	pair publicationIndexORPair,
) (publicationIndexORSignature, bool) {
	for _, state := range states {
		if state.before == pair.before && state.after == pair.after {
			return state, true
		}
	}
	return publicationIndexORSignature{}, false
}
