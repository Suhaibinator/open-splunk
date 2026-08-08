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

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const compiledExecutionSealDomain = "open-splunk-compiled-query-execution-v1"

var timeType = reflect.TypeFor[time.Time]()

type compiledExecutionSeal [sha256.Size]byte

// knowledgeCompilationEvidence is compiler-owned whole-query evidence. KO-0H
// has no knowledge prelude, so generated knowledge operators, fields, and
// scalar roots remain zero; nonempty prepared authority consequently cannot be
// finalized until KO-1 supplies those operators.
type knowledgeCompilationEvidence struct {
	generatedOperators    uint32
	generatedFields       uint32
	regexPrograms         uint32
	regexWorkUnits        uint64
	regexCaptureBytes     uint64
	extractionOutputs     uint32
	jsonEvaluationWork    uint32
	scalarExpressions     uint32
	scalarExpressionNodes uint32
	scalarPredicates      uint32
	generatedSQLBytes     uint64
}

// KnowledgeSnapshotEvidence is an opaque view of compiler-owned evidence. A
// caller can construct only its zero value, and snapshot finalization never
// accepts this type directly: it accepts the exact sealed CompiledQuery and
// opens the evidence itself.
type KnowledgeSnapshotEvidence struct {
	tenantID string
	indexes  []string
	charges  knowledgeCompilationEvidence
}

func (evidence KnowledgeSnapshotEvidence) TenantID() string {
	return strings.Clone(evidence.tenantID)
}

func (evidence KnowledgeSnapshotEvidence) EffectiveIndexes() []string {
	return slices.Clone(evidence.indexes)
}

func (evidence KnowledgeSnapshotEvidence) GeneratedOperators() uint32 {
	return evidence.charges.generatedOperators
}

func (evidence KnowledgeSnapshotEvidence) GeneratedFields() uint32 {
	return evidence.charges.generatedFields
}

func (evidence KnowledgeSnapshotEvidence) RegexPrograms() uint32 {
	return evidence.charges.regexPrograms
}

func (evidence KnowledgeSnapshotEvidence) RegexWorkUnits() uint64 {
	return evidence.charges.regexWorkUnits
}

func (evidence KnowledgeSnapshotEvidence) RegexCaptureBytes() uint64 {
	return evidence.charges.regexCaptureBytes
}

func (evidence KnowledgeSnapshotEvidence) ExtractionOutputs() uint32 {
	return evidence.charges.extractionOutputs
}

func (evidence KnowledgeSnapshotEvidence) JSONEvaluationWork() uint32 {
	return evidence.charges.jsonEvaluationWork
}

func (evidence KnowledgeSnapshotEvidence) ScalarExpressions() uint32 {
	return evidence.charges.scalarExpressions
}

func (evidence KnowledgeSnapshotEvidence) ScalarExpressionNodes() uint32 {
	return evidence.charges.scalarExpressionNodes
}

func (evidence KnowledgeSnapshotEvidence) ScalarPredicates() uint32 {
	return evidence.charges.scalarPredicates
}

func (evidence KnowledgeSnapshotEvidence) GeneratedSQLBytes() uint64 {
	return evidence.charges.generatedSQLBytes
}

func sealFinalCompiledQuery(
	compiled CompiledQuery,
	query *plan.Query,
	scan *plan.Scan,
	authored authoredKnowledgeCompilation,
) (CompiledQuery, error) {
	sealed, err := sealCompiledQueryReadScope(compiled, scan.TenantID, scan.Indexes)
	if err != nil {
		return CompiledQuery{}, err
	}
	if predicates, ok := query.AuthoredScalarPredicateCount(); ok {
		captureBytes := uint64(0)
		if authored.regexPrograms > 0 {
			captureBytes = MaximumRexCapturedBytesPerRow
		}
		sealed.knowledgeEvidence = &knowledgeCompilationEvidence{
			regexPrograms:      authored.regexPrograms,
			regexWorkUnits:     authored.regexWorkUnits,
			regexCaptureBytes:  captureBytes,
			extractionOutputs:  authored.extractionOutputs,
			jsonEvaluationWork: authored.jsonEvaluationWork,
			scalarPredicates:   predicates,
			generatedSQLBytes:  uint64(len(sealed.SQL)),
		}
	}
	return sealCompiledQueryExecution(sealed)
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
		charges:  *compiled.knowledgeEvidence,
	}, true
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
	digest := sha256.New()
	writeTokenPart(digest, compiledExecutionSealDomain)
	writeTokenPart(digest, compiled.SQL)
	writeStringSlice(digest, compiled.OutputFields)
	writeBool(digest, compiled.SparseFields)
	writeInt64(digest, int64(compiled.relationalDepth))
	writeRange(digest, compiled.relationalDepthRange)
	writeUint64(digest, compiled.sourceFanout)
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
	writeUint64(writer, uint64(evidence.generatedOperators))
	writeUint64(writer, uint64(evidence.generatedFields))
	writeUint64(writer, uint64(evidence.regexPrograms))
	writeUint64(writer, evidence.regexWorkUnits)
	writeUint64(writer, evidence.regexCaptureBytes)
	writeUint64(writer, uint64(evidence.extractionOutputs))
	writeUint64(writer, uint64(evidence.jsonEvaluationWork))
	writeUint64(writer, uint64(evidence.scalarExpressions))
	writeUint64(writer, uint64(evidence.scalarExpressionNodes))
	writeUint64(writer, uint64(evidence.scalarPredicates))
	writeUint64(writer, evidence.generatedSQLBytes)
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
