package knowledgecatalog

import (
	"context"
	"fmt"
	"math/bits"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

const (
	// A publication visibility inventory may contain every ACTIVE object read
	// by the resolver preflight, including candidates later removed by index
	// pruning or precedence. Fixed-width values keep every retained state
	// comparable without caller-sized allocations.
	publicationIndexMembershipWordCount = (MaximumResolutionCandidates + 63) / 64

	maximumPublicationIndexAtoms         = control.MaximumPhysicalIndexRecords
	maximumPublicationIndexClosureStates = 1024

	// The work ceiling is deliberately independent of the atom and retained-state
	// ceilings. A join-closed inventory can remain within both structural bounds
	// while still requiring their full Cartesian product. Charge every initial
	// atom admission and attempted join so that such amplification fails closed.
	maximumPublicationIndexClosureProbes         = uint64(64 << 10)
	publicationIndexClosureWordsPerProbe         = uint64(2 * publicationIndexMembershipWordCount)
	maximumPublicationIndexClosureWordOperations = maximumPublicationIndexClosureProbes * publicationIndexClosureWordsPerProbe
)

// publicationIndexMembership is a fixed universe of object ordinals. Ordinal
// zero occupies the low bit of word zero; binary ordering compensates for that
// storage convention and compares membership from ordinal zero upward.
type publicationIndexMembership [publicationIndexMembershipWordCount]uint64

// publicationIndexAtom is the applicability inventory contributed by one real
// physical index before and after a proposed catalog mutation. Index identity
// is deliberately absent: identical atoms are interchangeable for OR closure
// and selecting a duplicate can never reduce a minimum witness.
type publicationIndexAtom struct {
	before publicationIndexMembership
	after  publicationIndexMembership
}

// publicationIndexORSignature is one distinct before/after applicability pair
// reachable by a nonempty effective index scope. minimumIndexes is the exact
// cardinality of its smallest physical-index witness.
type publicationIndexORSignature struct {
	before         publicationIndexMembership
	after          publicationIndexMembership
	minimumIndexes uint16
}

type publicationIndexORPair struct {
	before publicationIndexMembership
	after  publicationIndexMembership
}

type publicationIndexClosureBudget struct {
	probes         uint64
	wordOperations uint64
}

// enumeratePublicationIndexORSignatures returns every distinct component-wise
// OR of a nonempty subset of atoms whose minimum witness contains at most the
// read-scope index ceiling. Results are sorted by the binary before signature,
// then the binary after signature, and own all of their storage.
//
// The caller remains responsible for proving that atoms enumerate the complete
// physical-index catalog. An empty atom list therefore succeeds with an empty
// closure; it is not proof that a tenant has no indexes.
func enumeratePublicationIndexORSignatures(
	ctx context.Context,
	atoms []publicationIndexAtom,
) ([]publicationIndexORSignature, error) {
	var budget publicationIndexClosureBudget
	return enumeratePublicationIndexORSignaturesWithBudget(ctx, atoms, &budget)
}

func enumeratePublicationIndexORSignaturesWithBudget(
	ctx context.Context,
	atoms []publicationIndexAtom,
	budget *publicationIndexClosureBudget,
) ([]publicationIndexORSignature, error) {
	if ctx == nil {
		return nil, fmt.Errorf(
			"%w: publication index closure context is nil",
			control.ErrInvalidArgument,
		)
	}
	if budget == nil {
		return nil, fmt.Errorf(
			"%w: publication index closure budget is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(atoms) > maximumPublicationIndexAtoms {
		return nil, fmt.Errorf(
			"%w: publication index closure exceeds its atom limit",
			control.ErrCapacityExceeded,
		)
	}

	// Each atom contains arrays rather than slices, so cloning the outer slice
	// is a complete ingress detachment. Do it before another caller-controlled
	// context callback can mutate the source slice.
	detached := slices.Clone(atoms)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	canonicalAtoms, err := canonicalPublicationIndexAtoms(ctx, detached)
	if err != nil {
		return nil, err
	}
	if len(canonicalAtoms) == 0 {
		return []publicationIndexORSignature{}, nil
	}

	minimumWitness := make(
		map[publicationIndexORPair]uint16,
		len(canonicalAtoms),
	)
	queue := make([]publicationIndexORPair, 0, len(canonicalAtoms))
	oneIndex, ok := nextPublicationIndexWitness(0)
	if !ok {
		return nil, fmt.Errorf(
			"%w: publication index witness ceiling is invalid",
			control.ErrCapacityExceeded,
		)
	}
	for _, atom := range canonicalAtoms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := budget.charge(); err != nil {
			return nil, err
		}
		minimumWitness[atom] = oneIndex
		queue = append(queue, atom)
	}

	// FIFO traversal is breadth first because every edge adds one index. A
	// signature's first discovery is consequently its exact minimum witness;
	// repeated use of an atom cannot improve an idempotent OR.
	for head := 0; head < len(queue); head++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := queue[head]
		nextWitness, withinScope := nextPublicationIndexWitness(minimumWitness[current])
		if !withinScope {
			continue
		}
		for _, atom := range canonicalAtoms {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := budget.charge(); err != nil {
				return nil, err
			}
			candidate := orPublicationIndexPairs(current, atom)
			if _, exists := minimumWitness[candidate]; exists {
				continue
			}
			if len(queue) == maximumPublicationIndexClosureStates {
				return nil, fmt.Errorf(
					"%w: publication index closure exceeds its state limit",
					control.ErrCapacityExceeded,
				)
			}
			minimumWitness[candidate] = nextWitness
			queue = append(queue, candidate)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result := make([]publicationIndexORSignature, len(queue))
	for index, pair := range queue {
		result[index] = publicationIndexORSignature{
			before:         pair.before,
			after:          pair.after,
			minimumIndexes: minimumWitness[pair],
		}
	}
	slices.SortFunc(result, comparePublicationIndexORSignatures)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func canonicalPublicationIndexAtoms(
	ctx context.Context,
	atoms []publicationIndexAtom,
) ([]publicationIndexORPair, error) {
	seen := make(map[publicationIndexORPair]struct{}, len(atoms))
	result := make([]publicationIndexORPair, 0, len(atoms))
	for _, atom := range atoms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pair := publicationIndexORPair(atom)
		if _, duplicate := seen[pair]; duplicate {
			continue
		}
		seen[pair] = struct{}{}
		result = append(result, pair)
	}
	slices.SortFunc(result, comparePublicationIndexORPairs)
	return result, nil
}

func orPublicationIndexPairs(
	left, right publicationIndexORPair,
) publicationIndexORPair {
	var result publicationIndexORPair
	for index := 0; index < publicationIndexMembershipWordCount; index++ {
		result.before[index] = left.before[index] | right.before[index]
		result.after[index] = left.after[index] | right.after[index]
	}
	return result
}

func nextPublicationIndexWitness(current uint16) (uint16, bool) {
	if uint64(current) >= uint64(indexread.MaximumIndexesPerScope) {
		return 0, false
	}
	return current + 1, true
}

func (budget *publicationIndexClosureBudget) charge() error {
	if budget == nil ||
		budget.probes >= maximumPublicationIndexClosureProbes ||
		budget.wordOperations > maximumPublicationIndexClosureWordOperations-
			publicationIndexClosureWordsPerProbe {
		return fmt.Errorf(
			"%w: publication index closure exceeds its work limit",
			control.ErrCapacityExceeded,
		)
	}
	budget.probes++
	budget.wordOperations += publicationIndexClosureWordsPerProbe
	return nil
}

func comparePublicationIndexORSignatures(
	left, right publicationIndexORSignature,
) int {
	return comparePublicationIndexORPairs(
		publicationIndexORPair{before: left.before, after: left.after},
		publicationIndexORPair{before: right.before, after: right.after},
	)
}

func comparePublicationIndexORPairs(left, right publicationIndexORPair) int {
	if order := comparePublicationIndexMembership(left.before, right.before); order != 0 {
		return order
	}
	return comparePublicationIndexMembership(left.after, right.after)
}

func comparePublicationIndexMembership(
	left, right publicationIndexMembership,
) int {
	for index := 0; index < publicationIndexMembershipWordCount; index++ {
		// Reverse conventional low-bit-first storage so ordinal zero is the
		// first bit of the canonical binary ordering.
		leftWord := bits.Reverse64(left[index])
		rightWord := bits.Reverse64(right[index])
		switch {
		case leftWord < rightWord:
			return -1
		case leftWord > rightWord:
			return 1
		}
	}
	return 0
}
