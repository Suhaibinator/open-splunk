package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// v6 additionally binds the stats partitions whole-query max_threads hint.
// Execution digests minted before that compiler-owned execution setting became
// authoritative must never compare equal to the stronger contract.
const compiledExecutionSealDomain = "open-splunk-compiled-query-execution-v7"

var timeType = reflect.TypeFor[time.Time]()

type compiledExecutionSeal [sha256.Size]byte

type knowledgePreludeCompilationEvidence struct {
	present     bool
	commitment  [sha256.Size]byte
	objectCount uint32
	charges     knowledgeprogram.Charges
}

type authoredKnowledgeCompilationEvidence struct {
	regexPrograms      uint32
	regexWorkUnits     uint64
	extractionOutputs  uint32
	jsonEvaluationWork uint32
	scalarPredicates   uint32
}

// knowledgeCompilationEvidence is compiler-owned whole-query evidence. The
// immutable prelude identity and its exact knowledge-only charges are sealed
// separately from exact parser-authored suffix charges. This preserves
// aggregate query ceilings without allowing equal-cost programs to substitute
// for one another.
type knowledgeCompilationEvidence struct {
	prelude           knowledgePreludeCompilationEvidence
	authored          authoredKnowledgeCompilationEvidence
	regexCaptureBytes uint64
	generatedSQLBytes uint64
}

// KnowledgeSnapshotEvidence is an opaque view of compiler-owned evidence. A
// caller can construct only its zero value, and snapshot finalization never
// accepts this type directly: it accepts the exact sealed CompiledQuery and
// opens the evidence itself.
type KnowledgeSnapshotEvidence struct {
	tenantID string
	indexes  []string
	compiled knowledgeCompilationEvidence
}

func (evidence KnowledgeSnapshotEvidence) TenantID() string {
	return strings.Clone(evidence.tenantID)
}

func (evidence KnowledgeSnapshotEvidence) EffectiveIndexes() []string {
	return slices.Clone(evidence.indexes)
}

// KnowledgeProgramPresent distinguishes a legacy compiled query from one that
// deliberately crossed the knowledge-admission boundary with a valid program,
// including a valid empty program.
func (evidence KnowledgeSnapshotEvidence) KnowledgeProgramPresent() bool {
	return evidence.compiled.prelude.present
}

func (evidence KnowledgeSnapshotEvidence) KnowledgeProgramCommitment() ([sha256.Size]byte, bool) {
	if !evidence.compiled.prelude.present {
		return [sha256.Size]byte{}, false
	}
	return evidence.compiled.prelude.commitment, true
}

func (evidence KnowledgeSnapshotEvidence) KnowledgeProgramObjectCount() uint32 {
	return evidence.compiled.prelude.objectCount
}

// KnowledgeProgramCharges returns the exact compiler-independent contribution
// of the admitted program, excluding every authored SPL suffix contribution.
func (evidence KnowledgeSnapshotEvidence) KnowledgeProgramCharges() knowledgeprogram.Charges {
	return evidence.compiled.prelude.charges
}

func (evidence KnowledgeSnapshotEvidence) AuthoredRegexPrograms() uint32 {
	return evidence.compiled.authored.regexPrograms
}

func (evidence KnowledgeSnapshotEvidence) AuthoredRegexWorkUnits() uint64 {
	return evidence.compiled.authored.regexWorkUnits
}

func (evidence KnowledgeSnapshotEvidence) AuthoredExtractionOutputs() uint32 {
	return evidence.compiled.authored.extractionOutputs
}

func (evidence KnowledgeSnapshotEvidence) AuthoredJSONEvaluationWork() uint32 {
	return evidence.compiled.authored.jsonEvaluationWork
}

func (evidence KnowledgeSnapshotEvidence) AuthoredScalarPredicates() uint32 {
	return evidence.compiled.authored.scalarPredicates
}

func (evidence KnowledgeSnapshotEvidence) GeneratedOperators() uint32 {
	return evidence.compiled.prelude.charges.GeneratedOperators
}

func (evidence KnowledgeSnapshotEvidence) GeneratedFields() uint32 {
	return evidence.compiled.prelude.charges.GeneratedFields
}

func (evidence KnowledgeSnapshotEvidence) RegexPrograms() uint32 {
	return evidence.compiled.prelude.charges.RegexPrograms +
		evidence.compiled.authored.regexPrograms
}

func (evidence KnowledgeSnapshotEvidence) RegexWorkUnits() uint64 {
	return evidence.compiled.prelude.charges.RegexWorkUnits +
		evidence.compiled.authored.regexWorkUnits
}

func (evidence KnowledgeSnapshotEvidence) RegexCaptureBytes() uint64 {
	return evidence.compiled.regexCaptureBytes
}

func (evidence KnowledgeSnapshotEvidence) ExtractionOutputs() uint32 {
	return evidence.compiled.prelude.charges.ExtractionOutputs +
		evidence.compiled.authored.extractionOutputs
}

func (evidence KnowledgeSnapshotEvidence) JSONEvaluationWork() uint32 {
	return evidence.compiled.prelude.charges.JSONEvaluationWork +
		evidence.compiled.authored.jsonEvaluationWork
}

func (evidence KnowledgeSnapshotEvidence) ScalarExpressions() uint32 {
	return evidence.compiled.prelude.charges.ScalarExpressions
}

func (evidence KnowledgeSnapshotEvidence) ScalarExpressionNodes() uint32 {
	return evidence.compiled.prelude.charges.ScalarExpressionNodes
}

func (evidence KnowledgeSnapshotEvidence) ScalarPredicates() uint32 {
	return evidence.compiled.prelude.charges.ScalarPredicates +
		evidence.compiled.authored.scalarPredicates
}

func (evidence KnowledgeSnapshotEvidence) GeneratedSQLBytes() uint64 {
	return evidence.compiled.generatedSQLBytes
}

func sealFinalCompiledQuery(
	compiled CompiledQuery,
	query *plan.Query,
	scan *plan.Scan,
	preparation preparedKnowledgeCompilation,
	prelude compiledKnowledgePrelude,
) (CompiledQuery, error) {
	if err := validateKnowledgePreludePreparation(preparation); err != nil {
		return CompiledQuery{}, err
	}
	finalPreparation, err := prepareKnowledgeCompilation(query)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !preparedKnowledgeCompilationEqual(preparation, finalPreparation) {
		return CompiledQuery{}, errors.New(
			"seal compiled ClickHouse execution: knowledge authority changed during compilation",
		)
	}
	preparation = finalPreparation
	evidence, err := compileKnowledgeCompilationEvidence(
		preparation,
		prelude,
		uint64(len(compiled.SQL)),
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	sealed, err := sealCompiledQueryReadScope(compiled, scan.TenantID, scan.Indexes)
	if err != nil {
		return CompiledQuery{}, err
	}
	if evidence != nil {
		sealed.knowledgeEvidence = evidence
	}
	return sealCompiledQueryExecution(sealed)
}

// compileKnowledgeCompilationEvidence derives the exact evidence that will be
// sealed into the final executable. A nonempty prelude contributes object and
// charge totals only through its validated physical lowering proof; the
// immutable program contributes the semantic commitment that prevents an
// equal-cost program from substituting for it. Identity preludes retain the
// legacy absent/present-empty distinction without inventing physical work.
func compileKnowledgeCompilationEvidence(
	preparation preparedKnowledgeCompilation,
	prelude compiledKnowledgePrelude,
	generatedSQLBytes uint64,
) (*knowledgeCompilationEvidence, error) {
	if err := validateKnowledgePreludePreparation(preparation); err != nil {
		return nil, err
	}
	nonempty := preparation.present && preparation.program.ObjectCount() != 0
	if nonempty {
		if err := validateCompiledKnowledgePrelude(prelude, preparation); err != nil {
			return nil, err
		}
	} else if err := validateKnowledgeRuntimeGuardIdentityPrelude(
		prelude,
		preparation,
	); err != nil {
		return nil, err
	}

	result := knowledgeCompilationEvidence{
		authored: authoredKnowledgeCompilationEvidence{
			regexPrograms:      preparation.authored.regexPrograms,
			regexWorkUnits:     preparation.authored.regexWorkUnits,
			extractionOutputs:  preparation.authored.extractionOutputs,
			jsonEvaluationWork: preparation.authored.jsonEvaluationWork,
			scalarPredicates:   preparation.authoredScalarPredicates,
		},
		generatedSQLBytes: generatedSQLBytes,
	}
	if preparation.present {
		semantic, valid := compileKnowledgePreludeEvidence(preparation.program)
		if !valid || semantic.commitment != preparation.programCommitment ||
			semantic.objectCount != preparation.program.ObjectCount() ||
			semantic.charges != preparation.programCharges {
			return nil, errors.New(
				"seal compiled ClickHouse execution: knowledge prelude is invalid",
			)
		}
		result.prelude = semantic
		if nonempty {
			result.prelude.objectCount = prelude.proof.objectCount
			result.prelude.charges = prelude.proof.charges
			if result.prelude.objectCount != semantic.objectCount ||
				result.prelude.charges != semantic.charges {
				return nil, errors.New(
					"seal compiled ClickHouse execution: physical knowledge evidence disagrees",
				)
			}
		}
	}
	if err := validateSharedKnowledgeCompilationBudgets(
		result.prelude.charges,
		preparation.authored,
		preparation.authoredScalarPredicates,
	); err != nil {
		return nil, err
	}
	if !preparation.authoredScalarPredicatesExact {
		if preparation.present {
			return nil, errors.New(
				"seal compiled ClickHouse execution: authored predicate evidence is inexact",
			)
		}
		return nil, nil
	}
	if result.prelude.charges.RegexPrograms+result.authored.regexPrograms > 0 {
		result.regexCaptureBytes = MaximumRexCapturedBytesPerRow
	}
	return &result, nil
}

func preparedKnowledgeCompilationEqual(
	left preparedKnowledgeCompilation,
	right preparedKnowledgeCompilation,
) bool {
	return left.present == right.present &&
		left.prefixLength == right.prefixLength &&
		slices.Equal(left.operatorKinds, right.operatorKinds) &&
		left.program.Equal(right.program) &&
		left.program.ObjectCount() == right.program.ObjectCount() &&
		left.programCharges == right.programCharges &&
		left.programCommitment == right.programCommitment &&
		left.authored == right.authored &&
		left.authoredScalarPredicates == right.authoredScalarPredicates &&
		left.authoredScalarPredicatesExact == right.authoredScalarPredicatesExact
}

func compileKnowledgePreludeEvidence(
	program knowledgeprogram.Program,
) (knowledgePreludeCompilationEvidence, bool) {
	commitment, ok := program.Commitment()
	if program.IsZero() || !ok {
		return knowledgePreludeCompilationEvidence{}, false
	}
	return knowledgePreludeCompilationEvidence{
		present:     true,
		commitment:  commitment,
		objectCount: program.ObjectCount(),
		charges:     program.Charges(),
	}, true
}

func sealCompiledQueryExecution(compiled CompiledQuery) (CompiledQuery, error) {
	seal, ok := compiledExecutionDigest(compiled)
	if !ok {
		return CompiledQuery{}, errors.New("seal compiled ClickHouse execution: bind argument type is unsupported")
	}
	compiled.executionSeal = &seal
	return compiled, nil
}

// KnowledgeSnapshotEvidence opens detached compiler evidence only when every
// executable field, bind argument, read-scope marker, and evidence counter is
// unchanged from the exact compiler output.
func (compiled CompiledQuery) KnowledgeSnapshotEvidence() (KnowledgeSnapshotEvidence, bool) {
	if compiled.knowledgeEvidence == nil || !compiled.hasValidExecutionSeal() {
		return KnowledgeSnapshotEvidence{}, false
	}
	tenantID, indexes, ok := compiled.ReadScope()
	if !ok {
		return KnowledgeSnapshotEvidence{}, false
	}
	return KnowledgeSnapshotEvidence{
		tenantID: strings.Clone(tenantID),
		indexes:  slices.Clone(indexes),
		compiled: *compiled.knowledgeEvidence,
	}, true
}

// KnowledgeSnapshotEvidenceFor opens compiler evidence only when it is sealed
// to the exact supplied immutable program. In particular, a legacy compiled
// query cannot satisfy a valid empty program, and an equal-cost program with a
// different semantic commitment cannot substitute for the admitted one.
func (compiled CompiledQuery) KnowledgeSnapshotEvidenceFor(
	program knowledgeprogram.Program,
) (KnowledgeSnapshotEvidence, bool) {
	evidence, ok := compiled.KnowledgeSnapshotEvidence()
	if !ok || !compiled.knowledgeEvidence.matchesProgram(program) {
		return KnowledgeSnapshotEvidence{}, false
	}
	return evidence, true
}

func (evidence knowledgeCompilationEvidence) matchesProgram(program knowledgeprogram.Program) bool {
	prelude, ok := compileKnowledgePreludeEvidence(program)
	if !ok || !evidence.prelude.present || evidence.prelude.objectCount != prelude.objectCount ||
		evidence.prelude.charges != prelude.charges {
		return false
	}
	return subtle.ConstantTimeCompare(
		evidence.prelude.commitment[:],
		prelude.commitment[:],
	) == 1
}

func (compiled CompiledQuery) hasValidExecutionSeal() bool {
	if compiled.executionSeal == nil {
		return false
	}
	if _, _, ok := compiled.ReadScope(); !ok {
		return false
	}
	expected, ok := compiledExecutionDigest(compiled)
	return ok && subtle.ConstantTimeCompare(expected[:], compiled.executionSeal[:]) == 1
}

// HasValidExecutionSeal reports whether the complete executable contract is
// still the exact compiler-produced value. Unlike HasValidSQLSeal, this binds
// every typed argument and result-shape field in addition to SQL and read
// scope. It exposes verification only; callers still cannot install a seal.
func (compiled CompiledQuery) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

// StatsPartitionsMaxThreadsHint opens the compiler-owned whole-query
// max_threads cap only while the complete execution contract remains sealed.
// It is an Open Splunk approximation of stats partitions because ClickHouse
// cannot apply a different max_threads value to each reduction stage.
func (compiled CompiledQuery) StatsPartitionsMaxThreadsHint() (uint8, bool) {
	if compiled.statsPartitionsMaxThreadsHint == 0 || !compiled.hasValidExecutionSeal() {
		return 0, false
	}
	return compiled.statsPartitionsMaxThreadsHint, true
}

// ExecutionAuthorityDigest returns an opaque commitment to the complete
// executable contract only when its compiler-owned seal is valid. The digest
// can be embedded in a wider authority commitment without exposing any API
// that can construct or repair a CompiledQuery seal.
func (compiled CompiledQuery) ExecutionAuthorityDigest() ([sha256.Size]byte, bool) {
	if !compiled.hasValidExecutionSeal() {
		return [sha256.Size]byte{}, false
	}
	return [sha256.Size]byte(*compiled.executionSeal), true
}

// EqualForExecution reports exact equality across the complete compiler-sealed
// executable surface, including SQL, every typed bind value, result contracts,
// private read scope, relational evidence, and knowledge charges. Invalid or
// tampered values are never equal, including two zero values.
func (compiled CompiledQuery) EqualForExecution(other CompiledQuery) bool {
	if !compiled.hasValidExecutionSeal() || !other.hasValidExecutionSeal() {
		return false
	}
	return subtle.ConstantTimeCompare(compiled.executionSeal[:], other.executionSeal[:]) == 1
}

func compiledExecutionDigest(compiled CompiledQuery) (compiledExecutionSeal, bool) {
	if !validResultContainerOutputs(compiled) ||
		!validResultFieldPresentations(compiled) ||
		compiled.statsPartitionsMaxThreadsHint > maximumStatsPartitionsMaxThreadsHint {
		return compiledExecutionSeal{}, false
	}
	digest := sha256.New()
	writeTokenPart(digest, compiledExecutionSealDomain)
	writeTokenPart(digest, compiled.SQL)
	writeStringSlice(digest, compiled.OutputFields)
	writeBool(digest, compiled.OutputPresentations == nil)
	writeUint64(digest, uint64(len(compiled.OutputPresentations)))
	for _, presentation := range compiled.OutputPresentations {
		writeBool(digest, presentation.HasFlatMultivalueDelimiter)
		writeTokenPart(digest, presentation.FlatMultivalueDelimiter)
		writeBool(digest, presentation.StatsSparkline)
	}
	writeBool(digest, compiled.ContainerOutputs == nil)
	writeUint64(digest, uint64(len(compiled.ContainerOutputs)))
	for _, output := range compiled.ContainerOutputs {
		writeUint64(digest, uint64(output.OutputIndex))
	}
	writeBool(digest, compiled.SparseFields)
	writeBool(digest, compiled.atomicResult)
	writeInt64(digest, int64(compiled.relationalDepth))
	writeRange(digest, compiled.relationalDepthRange)
	writeUint64(digest, compiled.sourceFanout)
	writeUint64(digest, uint64(compiled.statsPartitionsMaxThreadsHint))
	_, _ = digest.Write(compiled.readScope.seal[:])

	writeBool(digest, compiled.Args == nil)
	writeUint64(digest, uint64(len(compiled.Args)))
	for _, argument := range compiled.Args {
		if !writeCompiledArgument(digest, argument, 0) {
			return compiledExecutionSeal{}, false
		}
	}
	if compiled.Timechart == nil {
		writeBool(digest, false)
	} else {
		writeBool(digest, true)
		writeInt64(digest, int64(compiled.Timechart.Mode))
		if !writeTime(digest, compiled.Timechart.FirstBucket) {
			return compiledExecutionSeal{}, false
		}
		writeInt64(digest, int64(compiled.Timechart.Span))
		writeUint64(digest, compiled.Timechart.BucketCount)
		writeUint64(digest, uint64(compiled.Timechart.MaxSeries))
		writeUint64(digest, uint64(compiled.Timechart.MaxLabelBytes))
		writeTokenPart(digest, compiled.Timechart.ValueField)
		writeInt64(digest, int64(compiled.Timechart.ValueKind))
	}
	if compiled.Chart == nil {
		writeBool(digest, false)
	} else {
		writeBool(digest, true)
		writeTokenPart(digest, compiled.Chart.RowField)
		writeInt64(digest, int64(compiled.Chart.RowKind))
		writeTokenPart(digest, compiled.Chart.RowDatabaseType)
		writeUint64(digest, compiled.Chart.RowLimit)
		writeUint64(digest, uint64(compiled.Chart.MaxSeries))
		writeUint64(digest, uint64(compiled.Chart.MaxLabelBytes))
		writeInt64(digest, int64(compiled.Chart.ValueKind))
	}
	if compiled.knowledgeEvidence == nil {
		writeBool(digest, false)
	} else {
		writeBool(digest, true)
		writeKnowledgeEvidence(digest, *compiled.knowledgeEvidence)
	}

	var result compiledExecutionSeal
	digest.Sum(result[:0])
	return result, true
}

func writeKnowledgeEvidence(writer hash.Hash, evidence knowledgeCompilationEvidence) {
	writeBool(writer, evidence.prelude.present)
	_, _ = writer.Write(evidence.prelude.commitment[:])
	writeUint64(writer, uint64(evidence.prelude.objectCount))
	writeKnowledgeProgramCharges(writer, evidence.prelude.charges)
	writeUint64(writer, uint64(evidence.authored.regexPrograms))
	writeUint64(writer, evidence.authored.regexWorkUnits)
	writeUint64(writer, uint64(evidence.authored.extractionOutputs))
	writeUint64(writer, uint64(evidence.authored.jsonEvaluationWork))
	writeUint64(writer, uint64(evidence.authored.scalarPredicates))
	writeUint64(writer, evidence.regexCaptureBytes)
	writeUint64(writer, evidence.generatedSQLBytes)
}

func writeKnowledgeProgramCharges(writer hash.Hash, charges knowledgeprogram.Charges) {
	writeUint64(writer, uint64(charges.GeneratedOperators))
	writeUint64(writer, uint64(charges.GeneratedFields))
	writeUint64(writer, uint64(charges.RegexPrograms))
	writeUint64(writer, charges.RegexWorkUnits)
	writeUint64(writer, uint64(charges.ExtractionOutputs))
	writeUint64(writer, uint64(charges.JSONEvaluationWork))
	writeUint64(writer, uint64(charges.ScalarExpressions))
	writeUint64(writer, uint64(charges.ScalarExpressionNodes))
	writeUint64(writer, uint64(charges.ScalarPredicates))
}

func writeCompiledArgument(writer hash.Hash, argument any, depth int) bool {
	if argument == nil {
		writeTokenPart(writer, "<nil>")
		return true
	}
	value := reflect.ValueOf(argument)
	valueType := value.Type()
	writeTokenPart(writer, valueType.PkgPath())
	writeTokenPart(writer, valueType.String())
	if valueType == timeType {
		return writeTime(writer, value.Interface().(time.Time))
	}
	switch value.Kind() {
	case reflect.Bool:
		writeBool(writer, value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		writeInt64(writer, value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		writeUint64(writer, value.Uint())
	case reflect.Float32:
		writeUint64(writer, uint64(math.Float32bits(float32(value.Float()))))
	case reflect.Float64:
		writeUint64(writer, math.Float64bits(value.Float()))
	case reflect.String:
		writeTokenPart(writer, value.String())
	case reflect.Slice:
		if depth != 0 || !supportedCompiledSliceElement(valueType.Elem()) {
			return false
		}
		writeBool(writer, value.IsNil())
		// #nosec G115 -- reflect slice lengths are nonnegative native ints.
		writeUint64(writer, uint64(value.Len()))
		for index := 0; index < value.Len(); index++ {
			if !writeCompiledArgument(writer, value.Index(index).Interface(), depth+1) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func supportedCompiledSliceElement(element reflect.Type) bool {
	if element == timeType {
		return true
	}
	switch element.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return true
	default:
		return false
	}
}

func writeStringSlice(writer hash.Hash, values []string) {
	writeBool(writer, values == nil)
	writeUint64(writer, uint64(len(values)))
	for _, value := range values {
		writeTokenPart(writer, value)
	}
}

func writeRange(writer hash.Hash, sourceRange spl.Range) {
	writePosition(writer, sourceRange.Start)
	writePosition(writer, sourceRange.End)
}

func writePosition(writer hash.Hash, position spl.Position) {
	writeInt64(writer, int64(position.Offset))
	writeInt64(writer, int64(position.Line))
	writeInt64(writer, int64(position.Column))
}

func writeTime(writer hash.Hash, value time.Time) bool {
	encoded, err := value.MarshalBinary()
	if err != nil {
		return false
	}
	writeUint64(writer, uint64(len(encoded)))
	_, _ = writer.Write(encoded)
	return true
}

func writeBool(writer hash.Hash, value bool) {
	if value {
		_, _ = writer.Write([]byte{1})
		return
	}
	_, _ = writer.Write([]byte{0})
}

func writeInt64(writer hash.Hash, value int64) {
	// #nosec G115 -- the conversion deliberately preserves two's-complement
	// bits in the canonical unsigned transport.
	writeUint64(writer, uint64(value))
}

func writeUint64(writer hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

// CloneForExecution returns a fully detached executable contract only when the
// current value still has its compiler-produced execution and read-scope seal.
func (compiled CompiledQuery) CloneForExecution() (CompiledQuery, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledQuery{}, false
	}
	cloned := compiled
	cloned.SQL = strings.Clone(compiled.SQL)
	cloned.OutputFields = cloneStrings(compiled.OutputFields)
	cloned.OutputPresentations = cloneResultFieldPresentations(
		compiled.OutputPresentations,
	)
	cloned.ContainerOutputs = slices.Clone(compiled.ContainerOutputs)
	if compiled.Args == nil {
		cloned.Args = nil
	} else {
		cloned.Args = make([]any, len(compiled.Args))
		for index, argument := range compiled.Args {
			value, ok := cloneCompiledArgument(argument)
			if !ok {
				return CompiledQuery{}, false
			}
			cloned.Args[index] = value
		}
	}
	if compiled.Timechart != nil {
		output := *compiled.Timechart
		output.ValueField = strings.Clone(output.ValueField)
		cloned.Timechart = &output
	}
	if compiled.Chart != nil {
		output := *compiled.Chart
		output.RowField = strings.Clone(output.RowField)
		output.RowDatabaseType = strings.Clone(output.RowDatabaseType)
		cloned.Chart = &output
	}
	cloned.readScope.tenantID = strings.Clone(compiled.readScope.tenantID)
	cloned.readScope.indexNames = cloneStrings(compiled.readScope.indexNames)
	cloned.readScope.argumentPositions = slices.Clone(compiled.readScope.argumentPositions)
	if compiled.knowledgeEvidence != nil {
		evidence := *compiled.knowledgeEvidence
		cloned.knowledgeEvidence = &evidence
	}
	seal := *compiled.executionSeal
	cloned.executionSeal = &seal
	if !cloned.hasValidExecutionSeal() {
		return CompiledQuery{}, false
	}
	return cloned, true
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = strings.Clone(value)
	}
	return result
}

func cloneCompiledArgument(argument any) (any, bool) {
	if argument == nil {
		return nil, true
	}
	value := reflect.ValueOf(argument)
	cloned, ok := cloneCompiledValue(value, 0)
	if !ok {
		return nil, false
	}
	return cloned.Interface(), true
}

func cloneCompiledValue(value reflect.Value, depth int) (reflect.Value, bool) {
	valueType := value.Type()
	if valueType == timeType {
		return value, true
	}
	switch value.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return value, true
	case reflect.String:
		cloned := reflect.New(valueType).Elem()
		cloned.SetString(strings.Clone(value.String()))
		return cloned, true
	case reflect.Slice:
		if depth != 0 || !supportedCompiledSliceElement(valueType.Elem()) {
			return reflect.Value{}, false
		}
		if value.IsNil() {
			return reflect.Zero(valueType), true
		}
		cloned := reflect.MakeSlice(valueType, value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			element, ok := cloneCompiledValue(value.Index(index), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			cloned.Index(index).Set(element)
		}
		return cloned, true
	default:
		return reflect.Value{}, false
	}
}

// RetainedBytes returns a conservative charge for the complete sealed query,
// including slice capacity, boxed bind values, private seals, and string/slice
// payloads. Tampered or unsupported values return false.
func (compiled CompiledQuery) RetainedBytes() (uint64, bool) {
	if !compiled.hasValidExecutionSeal() {
		return 0, false
	}
	total := uint64(unsafe.Sizeof(compiled))
	var ok bool
	total, ok = retainedAdd(total, uint64(len(compiled.SQL)))
	if !ok {
		return 0, false
	}
	total, ok = retainedStringSlice(total, compiled.OutputFields)
	if !ok {
		return 0, false
	}
	total, ok = retainedAdd(
		total,
		uint64(cap(compiled.OutputPresentations))*
			uint64(unsafe.Sizeof(ResultFieldPresentation{})),
	)
	if !ok {
		return 0, false
	}
	for _, presentation := range compiled.OutputPresentations {
		total, ok = retainedAdd(
			total,
			uint64(len(presentation.FlatMultivalueDelimiter)),
		)
		if !ok {
			return 0, false
		}
	}
	total, ok = retainedAdd(
		total,
		uint64(cap(compiled.ContainerOutputs))*uint64(unsafe.Sizeof(ResultContainerOutput{})),
	)
	if !ok {
		return 0, false
	}
	total, ok = retainedAdd(total, uint64(cap(compiled.Args))*uint64(unsafe.Sizeof(any(nil))))
	if !ok {
		return 0, false
	}
	for _, argument := range compiled.Args {
		charge, supported := retainedCompiledArgument(argument)
		if !supported {
			return 0, false
		}
		total, ok = retainedAdd(total, charge)
		if !ok {
			return 0, false
		}
	}
	if compiled.Timechart != nil {
		total, ok = retainedAdd(total, uint64(unsafe.Sizeof(*compiled.Timechart))+uint64(len(compiled.Timechart.ValueField)))
		if !ok {
			return 0, false
		}
	}
	if compiled.Chart != nil {
		total, ok = retainedAdd(total, uint64(unsafe.Sizeof(*compiled.Chart))+uint64(len(compiled.Chart.RowField))+uint64(len(compiled.Chart.RowDatabaseType)))
		if !ok {
			return 0, false
		}
	}
	total, ok = retainedAdd(total, uint64(len(compiled.readScope.tenantID)))
	if !ok {
		return 0, false
	}
	total, ok = retainedStringSlice(total, compiled.readScope.indexNames)
	if !ok {
		return 0, false
	}
	total, ok = retainedAdd(total, uint64(cap(compiled.readScope.argumentPositions))*uint64(unsafe.Sizeof(int(0))))
	if !ok {
		return 0, false
	}
	if compiled.knowledgeEvidence != nil {
		total, ok = retainedAdd(total, uint64(unsafe.Sizeof(*compiled.knowledgeEvidence)))
		if !ok {
			return 0, false
		}
	}
	return retainedAdd(total, uint64(unsafe.Sizeof(*compiled.executionSeal)))
}

func retainedCompiledArgument(argument any) (uint64, bool) {
	if argument == nil {
		return 0, true
	}
	value := reflect.ValueOf(argument)
	valueType := value.Type()
	total := uint64(valueType.Size())
	if valueType == timeType {
		return total, true
	}
	switch value.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return total, true
	case reflect.String:
		// #nosec G115 -- reflect string lengths are nonnegative native ints.
		return retainedAdd(total, uint64(value.Len()))
	case reflect.Slice:
		if !supportedCompiledSliceElement(valueType.Elem()) {
			return 0, false
		}
		var ok bool
		// #nosec G115 -- reflect slice capacities are nonnegative native ints.
		total, ok = retainedAdd(total, uint64(value.Cap())*uint64(valueType.Elem().Size()))
		if !ok {
			return 0, false
		}
		if valueType.Elem().Kind() == reflect.String {
			for index := 0; index < value.Len(); index++ {
				// #nosec G115 -- reflect string lengths are nonnegative native ints.
				total, ok = retainedAdd(total, uint64(value.Index(index).Len()))
				if !ok {
					return 0, false
				}
			}
		}
		return total, true
	default:
		return 0, false
	}
}

func retainedStringSlice(total uint64, values []string) (uint64, bool) {
	var ok bool
	total, ok = retainedAdd(total, uint64(cap(values))*uint64(unsafe.Sizeof("")))
	if !ok {
		return 0, false
	}
	for _, value := range values {
		total, ok = retainedAdd(total, uint64(len(value)))
		if !ok {
			return 0, false
		}
	}
	return total, true
}

func retainedAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}
