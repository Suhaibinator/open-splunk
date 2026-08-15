package knowledge

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumSelectorDimensions is the closed trusted-metadata dimension count.
	MaximumSelectorDimensions = 4
	// MaximumSelectorPatternsPerDimension bounds one OR-list.
	MaximumSelectorPatternsPerDimension = 16
	// MaximumSelectorPatterns bounds the aggregate selector OR surface.
	MaximumSelectorPatterns = 64
	// MaximumSelectorPatternBytes bounds one normalized UTF-8 glob.
	MaximumSelectorPatternBytes = 255
	// MaximumSelectorNormalizedBytes bounds all canonical dimension and pattern
	// bytes, including the stable representation's framing.
	MaximumSelectorNormalizedBytes = 8 << 10
	// MaximumSelectorWildcardWorkUnits bounds aggregate normalized token work;
	// literal scalars cost one, '?' costs two, and '*' costs four.
	MaximumSelectorWildcardWorkUnits = 1 << 10
	// MaximumSelectorRuntimeValueBytes bounds one trusted metadata value before
	// matching.
	MaximumSelectorRuntimeValueBytes = 1 << 20
	// MaximumSelectorRuntimeEventBytes bounds aggregate inspected metadata bytes
	// for one event across selector matches.
	MaximumSelectorRuntimeEventBytes = 4 << 20
	// MaximumSelectorRuntimeQueryUnits bounds cumulative query charging. Each
	// inspected byte costs one unit and each unit in the conservative matcher-
	// transition upper bound costs eight.
	MaximumSelectorRuntimeQueryUnits = 1 << 30
	// SelectorMatcherTransitionUnits is the stable query charge per assessed
	// transition-upper-bound unit.
	SelectorMatcherTransitionUnits = 8
)

var (
	// ErrInvalidSelector identifies malformed dimensions, glob grammar, or
	// runtime values.
	ErrInvalidSelector = errors.New("invalid knowledge selector")
	// ErrRuntimeLimit identifies exhausted selector runtime input or work.
	ErrRuntimeLimit = errors.New("knowledge selector runtime limit exceeded")
)

// Dimension is one trusted canonical event-metadata selector dimension.
type Dimension uint8

const (
	DimensionIndex Dimension = iota + 1
	DimensionHost
	DimensionSource
	DimensionSourcetype
)

var dimensionNames = [...]string{"", "index", "host", "source", "sourcetype"}

// String returns the canonical lowercase dimension name.
func (dimension Dimension) String() string {
	if !dimension.valid() {
		return "unknown"
	}
	return dimensionNames[dimension]
}

func (dimension Dimension) valid() bool {
	return dimension >= DimensionIndex && dimension <= DimensionSourcetype
}

// DimensionSpec is one OR-list supplied to CompileSelector. The input slice is
// copied; subsequent caller mutation cannot affect the compiled selector.
type DimensionSpec struct {
	Dimension Dimension
	Patterns  []string
}

// SelectorSpec is a protobuf-independent selector definition. Omitted or empty
// dimensions are unrestricted. Duplicate dimensions are invalid.
type SelectorSpec struct {
	Dimensions []DimensionSpec
}

// Pattern is one immutable normalized glob.
type Pattern struct {
	canonical string
	literal   string
	wildcard  bool
	tokens    []globToken
}

// NormalizePattern implements the closed glob grammar: '*' matches zero or
// more Unicode scalar values, '?' matches exactly one Unicode scalar value,
// and backslash may escape only '*', '?', or backslash. Matching is anchored
// and binary-case-sensitive. Consecutive stars collapse canonically.
func NormalizePattern(source string) (Pattern, error) {
	value := TrimASCIIWhitespace(source)
	if value == "" {
		return Pattern{}, fmt.Errorf("%w: pattern is empty", ErrInvalidSelector)
	}
	if len(value) > MaximumSelectorPatternBytes {
		return Pattern{}, fmt.Errorf("%w: pattern exceeds %d bytes", ErrResourceLimit, MaximumSelectorPatternBytes)
	}
	if !utf8.ValidString(value) {
		return Pattern{}, fmt.Errorf("%w: pattern is not UTF-8", ErrInvalidSelector)
	}

	tokens := make([]globToken, 0, min(len(value), MaximumSelectorPatternBytes))
	literal := make([]rune, 0, min(len(value), MaximumSelectorPatternBytes))
	canonical := make([]byte, 0, len(value))
	escaped := false
	wildcard := false
	for _, character := range value {
		if escaped {
			if character != '*' && character != '?' && character != '\\' {
				return Pattern{}, fmt.Errorf("%w: unsupported escape %q", ErrInvalidSelector, character)
			}
			tokens = append(tokens, globToken{kind: globLiteral, literal: character})
			literal = append(literal, character)
			canonical = append(canonical, '\\')
			canonical = utf8.AppendRune(canonical, character)
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if IsPinnedControl(character) {
			return Pattern{}, fmt.Errorf("%w: pattern contains a pinned C0/C1 control", ErrInvalidSelector)
		}
		switch character {
		case '*':
			wildcard = true
			if len(tokens) != 0 && tokens[len(tokens)-1].kind == globMany {
				continue
			}
			tokens = append(tokens, globToken{kind: globMany})
			canonical = append(canonical, '*')
		case '?':
			wildcard = true
			tokens = append(tokens, globToken{kind: globOne})
			canonical = append(canonical, '?')
		default:
			tokens = append(tokens, globToken{kind: globLiteral, literal: character})
			literal = append(literal, character)
			canonical = utf8.AppendRune(canonical, character)
		}
	}
	if escaped {
		return Pattern{}, fmt.Errorf("%w: pattern ends with an escape", ErrInvalidSelector)
	}
	if len(canonical) > MaximumSelectorPatternBytes {
		return Pattern{}, fmt.Errorf("%w: normalized pattern exceeds %d bytes", ErrResourceLimit, MaximumSelectorPatternBytes)
	}
	return Pattern{
		canonical: string(canonical),
		literal:   string(literal),
		wildcard:  wildcard,
		tokens:    tokens,
	}, nil
}

// String returns the canonical glob spelling.
func (pattern Pattern) String() string {
	return pattern.canonical
}

// IsLiteral reports whether matching can use the exact-map fast path.
func (pattern Pattern) IsLiteral() bool {
	return !pattern.wildcard
}

// Literal returns the exact value for a literal pattern.
func (pattern Pattern) Literal() (string, bool) {
	return pattern.literal, !pattern.wildcard
}

type globTokenKind uint8

const (
	globLiteral globTokenKind = iota
	globOne
	globMany
)

type globToken struct {
	kind    globTokenKind
	literal rune
}

type compiledDimension struct {
	patterns      []string
	exact         map[string]struct{}
	exactLiterals []string
	wildcard      *globProgram
	wildcardRE2   string
	assessment    MatcherTransitionAssessment
}

// Selector is an immutable, race-safe compiled selector. Each constrained
// dimension uses one exact map and at most one combined wildcard NFA program;
// matching never loops over wildcard patterns and rescans the value for each.
type Selector struct {
	dimensions [MaximumSelectorDimensions]compiledDimension
	canonical  []byte
	stats      CompileStats
}

// CompileStats is stable publication evidence for selector resource charging.
type CompileStats struct {
	Dimensions        uint64
	Patterns          uint64
	NormalizedBytes   uint64
	WildcardWorkUnits uint64
}

// MatcherTransitionAssessment is the deterministic conservative wildcard
// charge for one constrained dimension. For B valid UTF-8 input bytes, the
// charged transition upper bound is Initial + B*PerInputByte + Final. The
// coefficients are derived only from the canonical wildcard token streams and
// are therefore reproducible by every compiler/runtime implementation.
type MatcherTransitionAssessment struct {
	Initial      uint64
	PerInputByte uint64
	Final        uint64
}

// UpperBound returns the assessed transition charge for inputBytes. A zero
// assessment denotes a literal-only or unrestricted dimension.
func (assessment MatcherTransitionAssessment) UpperBound(inputBytes uint64) (uint64, error) {
	if assessment == (MatcherTransitionAssessment{}) {
		return 0, nil
	}
	if assessment.PerInputByte != 0 && inputBytes > (math.MaxUint64-assessment.Initial)/assessment.PerInputByte {
		return 0, fmt.Errorf("%w: matcher transition assessment overflows", ErrRuntimeLimit)
	}
	bound := assessment.Initial + inputBytes*assessment.PerInputByte
	if assessment.Final > math.MaxUint64-bound {
		return 0, fmt.Errorf("%w: matcher transition assessment overflows", ErrRuntimeLimit)
	}
	return bound + assessment.Final, nil
}

// DimensionRuntimeProgram is a detached compiler-facing representation of one
// constrained dimension. ExactLiterals are sorted canonical string matches.
// WildcardRE2 is empty for a literal-only dimension; otherwise it is one
// anchored, case-sensitive, dot-all RE2 alternation for every wildcard. The
// assessment charges that combined wildcard program after an exact miss.
type DimensionRuntimeProgram struct {
	ExactLiterals []string
	WildcardRE2   string
	Assessment    MatcherTransitionAssessment
}

// CompileSelector normalizes, sorts, and deduplicates patterns, then creates a
// single combined anchored wildcard program per dimension.
func CompileSelector(spec SelectorSpec) (*Selector, error) {
	if len(spec.Dimensions) > MaximumSelectorDimensions {
		return nil, fmt.Errorf("%w: selector exceeds %d dimensions", ErrResourceLimit, MaximumSelectorDimensions)
	}
	selector := &Selector{}
	seen := [MaximumSelectorDimensions]bool{}
	for _, input := range spec.Dimensions {
		if !input.Dimension.valid() {
			return nil, fmt.Errorf("%w: unknown dimension %d", ErrInvalidSelector, input.Dimension)
		}
		position := int(input.Dimension - 1)
		if seen[position] {
			return nil, fmt.Errorf("%w: duplicate %s dimension", ErrInvalidSelector, input.Dimension)
		}
		seen[position] = true
		if len(input.Patterns) == 0 {
			continue
		}
		if len(input.Patterns) > MaximumSelectorPatternsPerDimension {
			return nil, fmt.Errorf("%w: %s exceeds %d patterns", ErrResourceLimit, input.Dimension, MaximumSelectorPatternsPerDimension)
		}
		dimension, stats, err := compileDimension(input.Patterns)
		if err != nil {
			return nil, fmt.Errorf("%s selector: %w", input.Dimension, err)
		}
		selector.dimensions[position] = dimension
		selector.stats.Dimensions++
		selector.stats.Patterns += stats.Patterns
		selector.stats.WildcardWorkUnits += stats.WildcardWorkUnits
		if selector.stats.Patterns > MaximumSelectorPatterns {
			return nil, fmt.Errorf("%w: selector exceeds %d patterns", ErrResourceLimit, MaximumSelectorPatterns)
		}
	}
	selector.canonical = marshalCanonicalSelector(selector.dimensions)
	selector.stats.NormalizedBytes = uint64(len(selector.canonical))
	if len(selector.canonical) > MaximumSelectorNormalizedBytes {
		return nil, fmt.Errorf("%w: selector canonical representation exceeds %d bytes", ErrResourceLimit, MaximumSelectorNormalizedBytes)
	}
	if selector.stats.WildcardWorkUnits > MaximumSelectorWildcardWorkUnits {
		return nil, fmt.Errorf("%w: selector wildcard work exceeds %d", ErrResourceLimit, MaximumSelectorWildcardWorkUnits)
	}
	return selector, nil
}

func compileDimension(inputs []string) (compiledDimension, CompileStats, error) {
	normalized := make(map[string]Pattern, len(inputs))
	var tokenWork uint64
	for _, input := range inputs {
		pattern, err := NormalizePattern(input)
		if err != nil {
			return compiledDimension{}, CompileStats{}, err
		}
		if _, duplicate := normalized[pattern.canonical]; duplicate {
			continue
		}
		normalized[pattern.canonical] = pattern
		tokenWork += patternWorkUnits(pattern.tokens)
	}
	patterns := make([]string, 0, len(normalized))
	for canonical := range normalized {
		patterns = append(patterns, canonical)
	}
	sort.Strings(patterns)

	dimension := compiledDimension{patterns: patterns}
	wildcards := make([]Pattern, 0, len(patterns))
	for _, canonical := range patterns {
		pattern := normalized[canonical]
		if literal, ok := pattern.Literal(); ok {
			if dimension.exact == nil {
				dimension.exact = make(map[string]struct{}, len(patterns))
			}
			dimension.exact[literal] = struct{}{}
			dimension.exactLiterals = append(dimension.exactLiterals, literal)
			continue
		}
		wildcards = append(wildcards, pattern)
	}
	sort.Strings(dimension.exactLiterals)

	stats := CompileStats{
		Patterns:          uint64(len(patterns)),
		WildcardWorkUnits: tokenWork,
	}
	if len(wildcards) == 0 {
		return dimension, stats, nil
	}
	dimension.wildcard = compileGlobProgram(wildcards)
	dimension.wildcardRE2 = wildcardPatternsRE2(wildcards)
	dimension.assessment = assessGlobProgram(wildcards)
	return dimension, stats, nil
}

func wildcardPatternsRE2(patterns []Pattern) string {
	var expression strings.Builder
	expression.WriteString("(?s)^(?:")
	for patternIndex, pattern := range patterns {
		if patternIndex != 0 {
			expression.WriteByte('|')
		}
		for _, token := range pattern.tokens {
			switch token.kind {
			case globLiteral:
				expression.WriteString(regexp.QuoteMeta(string(token.literal)))
			case globOne:
				expression.WriteByte('.')
			case globMany:
				expression.WriteString(".*")
			}
		}
	}
	expression.WriteString(")$")
	return expression.String()
}

func assessGlobProgram(patterns []Pattern) MatcherTransitionAssessment {
	var assessment MatcherTransitionAssessment
	for _, pattern := range patterns {
		tokens := uint64(len(pattern.tokens))
		assessment.Initial++
		if len(pattern.tokens) != 0 && pattern.tokens[0].kind == globMany {
			assessment.Initial++
		}
		// One scalar scans n+1 states. Every one of the n token states can
		// conservatively require two epsilon-closure inspections; canonical
		// normalization has already collapsed consecutive stars.
		assessment.PerInputByte += 3*tokens + 1
		assessment.Final += tokens + 1
	}
	return assessment
}

func patternWorkUnits(tokens []globToken) uint64 {
	var units uint64
	for _, token := range tokens {
		switch token.kind {
		case globLiteral:
			units++
		case globOne:
			units += 2
		case globMany:
			units += 4
		}
	}
	return units
}

// globProgram is one combined NFA for every wildcard alternative in a
// dimension. Match walks the input scalar stream once while advancing all
// active alternatives and reports the exact number of examined transitions.
type globProgram struct {
	patterns [][]globToken
	offsets  []int
	states   int
}

func compileGlobProgram(patterns []Pattern) *globProgram {
	program := &globProgram{
		patterns: make([][]globToken, len(patterns)),
		offsets:  make([]int, len(patterns)),
	}
	for index, pattern := range patterns {
		program.offsets[index] = program.states
		program.patterns[index] = slices.Clone(pattern.tokens)
		program.states += len(pattern.tokens) + 1
	}
	return program
}

func (program *globProgram) match(ctx context.Context, value string, maximumTransitions uint64) (bool, uint64, error) {
	counter := globTransitionCounter{ctx: ctx, maximum: maximumTransitions}
	active := make([]uint32, program.states)
	next := make([]uint32, program.states)
	activeEpoch := uint32(1)
	nextEpoch := uint32(1)
	for pattern := range program.patterns {
		if !program.addClosure(active, activeEpoch, pattern, 0, &counter) {
			return false, counter.used, counter.err
		}
	}
	for _, character := range value {
		matchedAny := false
		for pattern, tokens := range program.patterns {
			for tokenIndex := 0; tokenIndex <= len(tokens); tokenIndex++ {
				if !counter.step() {
					return false, counter.used, counter.err
				}
				state := program.offsets[pattern] + tokenIndex
				if active[state] != activeEpoch || tokenIndex == len(tokens) {
					continue
				}
				token := tokens[tokenIndex]
				switch token.kind {
				case globLiteral:
					if token.literal == character && program.addClosure(next, nextEpoch, pattern, tokenIndex+1, &counter) {
						matchedAny = true
					} else if token.literal == character {
						return false, counter.used, counter.err
					}
				case globOne:
					if !program.addClosure(next, nextEpoch, pattern, tokenIndex+1, &counter) {
						return false, counter.used, counter.err
					}
					matchedAny = true
				case globMany:
					if !program.addClosure(next, nextEpoch, pattern, tokenIndex, &counter) {
						return false, counter.used, counter.err
					}
					matchedAny = true
				}
			}
		}
		if !matchedAny {
			return false, counter.used, nil
		}
		active, next = next, active
		activeEpoch, nextEpoch = nextEpoch, nextEpoch+1
	}
	matched := false
	for pattern, tokens := range program.patterns {
		for tokenIndex := 0; tokenIndex <= len(tokens); tokenIndex++ {
			if !counter.step() {
				return false, counter.used, counter.err
			}
			state := program.offsets[pattern] + tokenIndex
			if active[state] == activeEpoch && tokenIndex == len(tokens) {
				matched = true
			}
		}
	}
	return matched, counter.used, nil
}

func (program *globProgram) addClosure(
	states []uint32,
	epoch uint32,
	pattern int,
	token int,
	counter *globTransitionCounter,
) bool {
	tokens := program.patterns[pattern]
	for {
		if !counter.step() {
			return false
		}
		state := program.offsets[pattern] + token
		if states[state] == epoch {
			return true
		}
		states[state] = epoch
		if token >= len(tokens) || tokens[token].kind != globMany {
			return true
		}
		token++
	}
}

type globTransitionCounter struct {
	ctx     context.Context
	used    uint64
	maximum uint64
	err     error
}

func (counter *globTransitionCounter) step() bool {
	if counter.used >= counter.maximum {
		counter.err = ErrRuntimeLimit
		return false
	}
	// The initial check prevents beginning work after cancellation; the bounded
	// interval avoids a context operation for every matcher state inspection.
	if counter.used&1023 == 0 {
		select {
		case <-counter.ctx.Done():
			counter.err = context.Cause(counter.ctx)
			return false
		default:
		}
	}
	counter.used++
	return true
}

const canonicalSelectorDomain = "open-splunk/knowledge-selector/v1\x00"

func marshalCanonicalSelector(dimensions [MaximumSelectorDimensions]compiledDimension) []byte {
	length := len(canonicalSelectorDomain) + MaximumSelectorDimensions*3
	for _, dimension := range dimensions {
		for _, pattern := range dimension.patterns {
			length += 4 + len(pattern)
		}
	}
	canonical := make([]byte, 0, length)
	canonical = append(canonical, canonicalSelectorDomain...)
	for index, dimension := range dimensions {
		canonical = append(canonical, byte(index+1))
		var count [2]byte
		// A compiled dimension is bounded by MaximumSelectorPatternsPerDimension.
		patternCount := uint16(len(dimension.patterns)) // #nosec G115 -- compile-time bound is far below MaxUint16.
		binary.BigEndian.PutUint16(count[:], patternCount)
		canonical = append(canonical, count[:]...)
		for _, pattern := range dimension.patterns {
			var size [4]byte
			// A normalized pattern is bounded by MaximumSelectorPatternBytes.
			patternSize := uint32(len(pattern)) // #nosec G115 -- compile-time bound is far below MaxUint32.
			binary.BigEndian.PutUint32(size[:], patternSize)
			canonical = append(canonical, size[:]...)
			canonical = append(canonical, pattern...)
		}
	}
	return canonical
}

// CanonicalBytes returns an independent stable binary representation suitable
// for snapshot hashing and equality. Callers cannot mutate selector state.
func (selector *Selector) CanonicalBytes() []byte {
	if selector == nil {
		return nil
	}
	return slices.Clone(selector.canonical)
}

// Patterns returns an independent sorted canonical OR-list for dimension.
func (selector *Selector) Patterns(dimension Dimension) []string {
	if selector == nil || !dimension.valid() {
		return nil
	}
	return slices.Clone(selector.dimensions[dimension-1].patterns)
}

// RuntimeProgram returns a detached compiler-facing program for dimension and
// whether the dimension is constrained. Callers cannot mutate selector state.
func (selector *Selector) RuntimeProgram(dimension Dimension) (DimensionRuntimeProgram, bool) {
	if selector == nil || !dimension.valid() {
		return DimensionRuntimeProgram{}, false
	}
	compiled := &selector.dimensions[dimension-1]
	if len(compiled.patterns) == 0 {
		return DimensionRuntimeProgram{}, false
	}
	return DimensionRuntimeProgram{
		ExactLiterals: slices.Clone(compiled.exactLiterals),
		WildcardRE2:   strings.Clone(compiled.wildcardRE2),
		Assessment:    compiled.assessment,
	}, true
}

// Stats returns immutable compile-time resource accounting.
func (selector *Selector) Stats() CompileStats {
	if selector == nil {
		return CompileStats{}
	}
	return selector.stats
}

// ChargeSnapshotSelectorWork incrementally aggregates selector token work for
// one immutable knowledge snapshot. Literal scalars, '?', and '*' have already
// been charged at their normative one, two, and four units respectively.
func ChargeSnapshotSelectorWork(current uint64, selector *Selector) (uint64, error) {
	if selector == nil || current > MaximumSelectorWildcardWorkUnits {
		return 0, fmt.Errorf("%w: invalid snapshot selector charge", ErrInvalidSelector)
	}
	work := selector.stats.WildcardWorkUnits
	if work > MaximumSelectorWildcardWorkUnits-current {
		return 0, fmt.Errorf("%w: snapshot selector work exceeds %d", ErrResourceLimit, MaximumSelectorWildcardWorkUnits)
	}
	return current + work, nil
}

// ValueKind preserves missing and null as distinct states rather than
// coercing either to an empty string.
type ValueKind uint8

const (
	ValueMissing ValueKind = iota
	ValueNull
	ValueString
)

// MetadataValue is one immutable trusted canonical metadata value. Its zero
// value is explicitly missing.
type MetadataValue struct {
	kind ValueKind
	text string
}

// MissingMetadata returns an explicitly missing value.
func MissingMetadata() MetadataValue { return MetadataValue{kind: ValueMissing} }

// NullMetadata returns an explicitly null value.
func NullMetadata() MetadataValue { return MetadataValue{kind: ValueNull} }

// StringMetadata returns a present string value. Runtime UTF-8 and byte bounds
// are checked when the value is charged for matching.
func StringMetadata(value string) MetadataValue {
	return MetadataValue{kind: ValueString, text: value}
}

// Kind returns the presence kind.
func (value MetadataValue) Kind() ValueKind { return value.kind }

// String returns the present text and whether the value is a string.
func (value MetadataValue) String() (string, bool) {
	return value.text, value.kind == ValueString
}

// EventMetadata contains the only trusted fields accepted by selectors.
type EventMetadata struct {
	Index      MetadataValue
	Host       MetadataValue
	Source     MetadataValue
	Sourcetype MetadataValue
}

func (metadata EventMetadata) value(dimension Dimension) MetadataValue {
	switch dimension {
	case DimensionIndex:
		return metadata.Index
	case DimensionHost:
		return metadata.Host
	case DimensionSource:
		return metadata.Source
	case DimensionSourcetype:
		return metadata.Sourcetype
	default:
		return MetadataValue{}
	}
}

// RuntimeLimits bounds cumulative selector charging for an entire query. A
// lower explicit value is useful for admission sharing and deterministic tests.
type RuntimeLimits struct {
	QueryUnits uint64
}

// RuntimeCharge reports cumulative query usage. QueryUnits equals InputBytes
// plus eight units for each MatcherTransitionUpperBound. The latter is a
// deterministic compiler-derived charge, not an observed implementation count.
type RuntimeCharge struct {
	InputBytes                  uint64
	MatcherTransitionUpperBound uint64
	QueryUnits                  uint64
}

// RuntimeRemaining reports cumulative query capacity and the current event's
// remaining input-byte capacity.
type RuntimeRemaining struct {
	QueryUnits uint64
	EventBytes uint64
}

// RuntimeBudget is an immutable cumulative query budget. ChargeInput, Match,
// and BeginEvent return successor values, so sharing an initial value is
// race-safe. Call BeginEvent only at a real event boundary; selector matches
// for the same event must share one unreset value.
type RuntimeBudget struct {
	maximumQueryUnits uint64
	eventInputBytes   uint64
	charge            RuntimeCharge
	valid             bool
}

// NewRuntimeBudget validates explicit nonzero limits against hard ceilings.
func NewRuntimeBudget(limits RuntimeLimits) (RuntimeBudget, error) {
	if limits.QueryUnits == 0 || limits.QueryUnits > MaximumSelectorRuntimeQueryUnits {
		return RuntimeBudget{}, fmt.Errorf("%w: invalid runtime budget", ErrRuntimeLimit)
	}
	return RuntimeBudget{
		maximumQueryUnits: limits.QueryUnits,
		valid:             true,
	}, nil
}

// DefaultRuntimeBudget returns the hard cumulative per-query selector budget.
func DefaultRuntimeBudget() RuntimeBudget {
	budget, err := NewRuntimeBudget(RuntimeLimits{
		QueryUnits: MaximumSelectorRuntimeQueryUnits,
	})
	if err != nil {
		panic(err)
	}
	return budget
}

// Remaining returns unconsumed query and current-event capacity.
func (budget RuntimeBudget) Remaining() RuntimeRemaining {
	if !budget.valid {
		return RuntimeRemaining{}
	}
	return RuntimeRemaining{
		QueryUnits: budget.maximumQueryUnits - budget.charge.QueryUnits,
		EventBytes: MaximumSelectorRuntimeEventBytes - budget.eventInputBytes,
	}
}

// Charge returns cumulative query usage.
func (budget RuntimeBudget) Charge() RuntimeCharge {
	if !budget.valid {
		return RuntimeCharge{}
	}
	return budget.charge
}

// BeginEvent resets only the per-event byte counter and preserves cumulative
// query charging.
func (budget RuntimeBudget) BeginEvent() (RuntimeBudget, error) {
	if !budget.valid {
		return RuntimeBudget{}, fmt.Errorf("%w: invalid runtime budget", ErrRuntimeLimit)
	}
	budget.eventInputBytes = 0
	return budget, nil
}

// ChargeInput validates and charges one runtime value once. Assessed wildcard
// charging is separate because exact literal hits do not run the matcher.
func (budget RuntimeBudget) ChargeInput(value string) (RuntimeBudget, error) {
	if !budget.valid {
		return RuntimeBudget{}, fmt.Errorf("%w: invalid runtime charge", ErrRuntimeLimit)
	}
	if len(value) > MaximumSelectorRuntimeValueBytes {
		return RuntimeBudget{}, fmt.Errorf("%w: runtime value exceeds %d bytes", ErrRuntimeLimit, MaximumSelectorRuntimeValueBytes)
	}
	if !utf8.ValidString(value) {
		return RuntimeBudget{}, fmt.Errorf("%w: runtime value is not UTF-8", ErrInvalidSelector)
	}
	inputBytes := uint64(len(value))
	if inputBytes > MaximumSelectorRuntimeEventBytes-budget.eventInputBytes ||
		inputBytes > budget.maximumQueryUnits-budget.charge.QueryUnits {
		return RuntimeBudget{}, fmt.Errorf("%w: runtime input budget exhausted", ErrRuntimeLimit)
	}
	if inputBytes > math.MaxUint64-budget.charge.InputBytes {
		return RuntimeBudget{}, fmt.Errorf("%w: runtime input overflow", ErrRuntimeLimit)
	}
	budget.eventInputBytes += inputBytes
	budget.charge.InputBytes += inputBytes
	budget.charge.QueryUnits += inputBytes
	return budget, nil
}

func (budget RuntimeBudget) chargeMatcherTransitionUpperBound(transitions uint64) (RuntimeBudget, error) {
	if !budget.valid || transitions > math.MaxUint64/SelectorMatcherTransitionUnits {
		return RuntimeBudget{}, fmt.Errorf("%w: invalid matcher transition charge", ErrRuntimeLimit)
	}
	units := transitions * SelectorMatcherTransitionUnits
	if units > budget.maximumQueryUnits-budget.charge.QueryUnits ||
		transitions > math.MaxUint64-budget.charge.MatcherTransitionUpperBound {
		return RuntimeBudget{}, fmt.Errorf("%w: matcher transition budget exhausted", ErrRuntimeLimit)
	}
	budget.charge.MatcherTransitionUpperBound += transitions
	budget.charge.QueryUnits += units
	return budget, nil
}

// Match applies AND across dimensions and OR within a dimension. Unrestricted
// dimensions accept missing/null values without inspection. A constrained
// dimension matches only a present string; missing and null remain distinct
// nonmatches. The returned budget includes all dimensions inspected before the
// result or error.
func (selector *Selector) Match(ctx context.Context, metadata EventMetadata, budget RuntimeBudget) (bool, RuntimeBudget, error) {
	if selector == nil || ctx == nil || !budget.valid {
		return false, RuntimeBudget{}, fmt.Errorf("%w: invalid compiled selector or budget", ErrInvalidSelector)
	}
	for index := range selector.dimensions {
		if err := context.Cause(ctx); err != nil {
			return false, budget, err
		}
		dimension := &selector.dimensions[index]
		if len(dimension.patterns) == 0 {
			continue
		}
		value := metadata.value(Dimension(index + 1))
		if value.kind != ValueString {
			return false, budget, nil
		}
		charged, err := budget.ChargeInput(value.text)
		if err != nil {
			return false, budget, err
		}
		budget = charged
		if _, ok := dimension.exact[value.text]; ok {
			if err := context.Cause(ctx); err != nil {
				return false, budget, err
			}
			continue
		}
		if dimension.wildcard == nil {
			if err := context.Cause(ctx); err != nil {
				return false, budget, err
			}
			return false, budget, nil
		}
		transitionBound, err := dimension.assessment.UpperBound(uint64(len(value.text)))
		if err != nil {
			return false, budget, err
		}
		charged, err = budget.chargeMatcherTransitionUpperBound(transitionBound)
		if err != nil {
			return false, budget, err
		}
		budget = charged
		matched, transitions, matchErr := dimension.wildcard.match(
			ctx,
			value.text,
			transitionBound,
		)
		if matchErr != nil {
			if errors.Is(matchErr, ErrRuntimeLimit) {
				return false, budget, fmt.Errorf("%w: matcher exceeded its assessed transition bound", ErrRuntimeLimit)
			}
			return false, budget, matchErr
		}
		if transitions > transitionBound {
			return false, budget, fmt.Errorf("%w: matcher exceeded its assessed transition bound", ErrRuntimeLimit)
		}
		if err := context.Cause(ctx); err != nil {
			return false, budget, err
		}
		if !matched {
			return false, budget, nil
		}
	}
	return true, budget, nil
}
