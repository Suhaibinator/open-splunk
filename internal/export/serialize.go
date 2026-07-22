package export

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	maximumSafeJSONInteger = uint64(1<<53 - 1)
	// MaximumBufferedCSVCellBytes bounds CSV values that require a newly
	// materialized textual representation. Ordinary UTF-8 strings stream
	// directly and are governed only by the artifact byte limit.
	MaximumBufferedCSVCellBytes = uint64(8 << 20)
)

type columnSelection struct {
	columns []searchjobs.Column
	indexes []int
}

func selectColumns(schema searchjobs.Schema, requested []string) (columnSelection, error) {
	if len(schema.Columns) == 0 {
		return columnSelection{}, fmt.Errorf("%w: source schema is empty", ErrSourceUnavailable)
	}
	available := make(map[string]int, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return columnSelection{}, fmt.Errorf("%w: source schema is invalid", ErrSourceUnavailable)
		}
		if _, exists := available[column.Name]; exists {
			return columnSelection{}, fmt.Errorf("%w: source schema is invalid", ErrSourceUnavailable)
		}
		available[column.Name] = index
	}

	if len(requested) == 0 {
		columns := append([]searchjobs.Column(nil), schema.Columns...)
		indexes := make([]int, len(columns))
		for index := range indexes {
			indexes[index] = index
		}
		return columnSelection{columns: columns, indexes: indexes}, nil
	}

	selection := columnSelection{
		columns: make([]searchjobs.Column, 0, len(requested)),
		indexes: make([]int, 0, len(requested)),
	}
	seen := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		index, exists := available[name]
		if !exists || name == "" {
			return columnSelection{}, fmt.Errorf("%w: selected column %q is unknown", ErrInvalidColumns, name)
		}
		if _, duplicate := seen[name]; duplicate {
			return columnSelection{}, fmt.Errorf("%w: selected column %q is duplicated", ErrInvalidColumns, name)
		}
		seen[name] = struct{}{}
		selection.columns = append(selection.columns, schema.Columns[index])
		selection.indexes = append(selection.indexes, index)
	}
	return selection, nil
}

type csvSerializer struct {
	writer    *csv.Writer
	selection columnSelection
	cellLimit uint64
}

func newCSVSerializer(output io.Writer, selection columnSelection, options CSVOptions) (*csvSerializer, error) {
	return newCSVSerializerWithLimit(output, selection, options, MaximumBufferedCSVCellBytes)
}

func newCSVSerializerWithLimit(output io.Writer, selection columnSelection, options CSVOptions, cellLimit uint64) (*csvSerializer, error) {
	mode := options.HeaderMode
	if mode == CSVHeaderDefault {
		mode = CSVHeaderFieldNames
	}
	if mode != CSVHeaderFieldNames && mode != CSVHeaderDisplayNames && mode != CSVHeaderNone {
		return nil, fmt.Errorf("%w: invalid CSV header mode", ErrInvalidRequest)
	}
	serializer := &csvSerializer{writer: csv.NewWriter(output), selection: selection, cellLimit: cellLimit}
	if mode != CSVHeaderNone {
		header := make([]string, len(selection.columns))
		for index, column := range selection.columns {
			header[index] = protectSpreadsheetText(column.Name)
		}
		if err := serializer.writer.Write(header); err != nil {
			return nil, err
		}
	}
	return serializer, nil
}

func (serializer *csvSerializer) WriteRow(row searchjobs.ResultRow) error {
	record := make([]string, len(serializer.selection.indexes))
	for outputIndex, sourceIndex := range serializer.selection.indexes {
		if sourceIndex >= len(row.Values) {
			return errors.New("export source row does not match schema")
		}
		text, stringLike, err := csvValue(row.Values[sourceIndex], serializer.cellLimit)
		if err != nil {
			return err
		}
		if stringLike {
			if needsSpreadsheetProtection(text) {
				if uint64(len(text)) >= serializer.cellLimit {
					return ErrByteLimit
				}
				text = "'" + text
			}
		}
		record[outputIndex] = text
	}
	return serializer.writer.Write(record)
}

func (serializer *csvSerializer) Close() error {
	serializer.writer.Flush()
	return serializer.writer.Error()
}

func protectSpreadsheetText(value string) string {
	if needsSpreadsheetProtection(value) {
		return "'" + value
	}
	return value
}

func needsSpreadsheetProtection(value string) bool {
	if value == "" {
		return false
	}
	switch value[0] {
	case '=', '+', '-', '@', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func csvValue(value searchjobs.Value, limit uint64) (string, bool, error) {
	switch value.Kind() {
	case searchjobs.ValueKindNull:
		return "", false, nil
	case searchjobs.ValueKindString:
		result, _ := value.String()
		if !utf8.ValidString(result) {
			return "", false, errors.New("export source string is not valid UTF-8")
		}
		return result, true, nil
	case searchjobs.ValueKindSigned:
		result, _ := value.Signed()
		return strconv.FormatInt(result, 10), false, nil
	case searchjobs.ValueKindUnsigned:
		result, _ := value.Unsigned()
		return strconv.FormatUint(result, 10), false, nil
	case searchjobs.ValueKindDouble:
		result, _ := value.Double()
		return strconv.FormatFloat(result, 'g', -1, 64), false, nil
	case searchjobs.ValueKindBool:
		result, _ := value.Bool()
		return strconv.FormatBool(result), false, nil
	case searchjobs.ValueKindBytes:
		result, _ := value.Bytes()
		if uint64(base64.StdEncoding.EncodedLen(len(result))) > limit {
			return "", false, ErrByteLimit
		}
		return base64.StdEncoding.EncodeToString(result), true, nil
	case searchjobs.ValueKindTime:
		result, _ := value.Time()
		return result.UTC().Format(time.RFC3339Nano), true, nil
	case searchjobs.ValueKindDuration:
		result, _ := value.Duration()
		return result.String(), true, nil
	case searchjobs.ValueKindDecimal:
		result, _ := value.Decimal()
		return result, false, nil
	case searchjobs.ValueKindList, searchjobs.ValueKindObject:
		output := limitedMemoryBuffer{limit: limit}
		encoder := jsonValueEncoder{output: &output, integerEncoding: JSONIntegerNumberWhenSafe}
		if err := encoder.write(value); err != nil {
			return "", false, err
		}
		return output.buffer.String(), true, nil
	default:
		return "", false, errors.New("export source value kind is invalid")
	}
}

type jsonLinesSerializer struct {
	output    *bufio.Writer
	selection columnSelection
	options   JSONLinesOptions
}

func newJSONLinesSerializer(output io.Writer, selection columnSelection, options JSONLinesOptions) (*jsonLinesSerializer, error) {
	if options.IntegerEncoding == JSONIntegerDefault {
		options.IntegerEncoding = JSONIntegerNumberWhenSafe
	}
	if options.IntegerEncoding != JSONIntegerNumberWhenSafe && options.IntegerEncoding != JSONIntegerString {
		return nil, fmt.Errorf("%w: invalid JSON integer encoding", ErrInvalidRequest)
	}
	return &jsonLinesSerializer{output: bufio.NewWriterSize(output, 32<<10), selection: selection, options: options}, nil
}

func (serializer *jsonLinesSerializer) WriteRow(row searchjobs.ResultRow) error {
	if _, err := io.WriteString(serializer.output, "{"); err != nil {
		return err
	}
	for outputIndex, sourceIndex := range serializer.selection.indexes {
		if sourceIndex >= len(row.Values) {
			return errors.New("export source row does not match schema")
		}
		if outputIndex > 0 {
			if _, err := io.WriteString(serializer.output, ","); err != nil {
				return err
			}
		}
		if err := writeJSONString(serializer.output, serializer.selection.columns[outputIndex].Name); err != nil {
			return err
		}
		if _, err := io.WriteString(serializer.output, ":"); err != nil {
			return err
		}
		encoder := jsonValueEncoder{
			output:          serializer.output,
			integerEncoding: serializer.options.IntegerEncoding,
			typeMetadata:    serializer.options.IncludeTypeMetadata,
		}
		if err := encoder.write(row.Values[sourceIndex]); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(serializer.output, "}\n"); err != nil {
		return err
	}
	return nil
}

func (serializer *jsonLinesSerializer) Close() error { return serializer.output.Flush() }

type limitedMemoryBuffer struct {
	buffer bytes.Buffer
	limit  uint64
}

func (buffer *limitedMemoryBuffer) Write(payload []byte) (int, error) {
	retained := uint64(buffer.buffer.Len())
	if retained > buffer.limit || uint64(len(payload)) > buffer.limit-retained {
		return 0, ErrByteLimit
	}
	return buffer.buffer.Write(payload)
}

func (buffer *limitedMemoryBuffer) WriteString(value string) (int, error) {
	retained := uint64(buffer.buffer.Len())
	if retained > buffer.limit || uint64(len(value)) > buffer.limit-retained {
		return 0, ErrByteLimit
	}
	return buffer.buffer.WriteString(value)
}

type jsonContainerKind uint8

const (
	jsonList jsonContainerKind = iota + 1
	jsonObject
)

type jsonContainer struct {
	kind        jsonContainerKind
	first       bool
	expectValue bool
}

type jsonValueEncoder struct {
	output          io.Writer
	integerEncoding JSONIntegerEncoding
	typeMetadata    bool
	stack           []jsonContainer
}

func (encoder *jsonValueEncoder) write(value searchjobs.Value) error {
	return value.VisitDetached(encoder.visit)
}

func (encoder *jsonValueEncoder) visit(token searchjobs.ValueVisitToken) error {
	switch token.Kind {
	case searchjobs.ValueVisitListEnd:
		if len(encoder.stack) == 0 || encoder.stack[len(encoder.stack)-1].kind != jsonList {
			return errors.New("export source list is malformed")
		}
		encoder.stack = encoder.stack[:len(encoder.stack)-1]
		if err := encoder.writeRaw("]"); err != nil {
			return err
		}
		if encoder.typeMetadata {
			return encoder.writeRaw("}")
		}
		return nil
	case searchjobs.ValueVisitObjectEnd:
		if len(encoder.stack) == 0 || encoder.stack[len(encoder.stack)-1].kind != jsonObject {
			return errors.New("export source object is malformed")
		}
		frame := encoder.stack[len(encoder.stack)-1]
		if frame.expectValue {
			return errors.New("export source object is malformed")
		}
		encoder.stack = encoder.stack[:len(encoder.stack)-1]
		if err := encoder.writeRaw("}"); err != nil {
			return err
		}
		if encoder.typeMetadata {
			return encoder.writeRaw("}")
		}
		return nil
	case searchjobs.ValueVisitObjectField:
		if len(encoder.stack) == 0 || encoder.stack[len(encoder.stack)-1].kind != jsonObject {
			return errors.New("export source object field is malformed")
		}
		frame := &encoder.stack[len(encoder.stack)-1]
		if frame.expectValue {
			return errors.New("export source object field is missing a value")
		}
		if !frame.first {
			if err := encoder.writeRaw(","); err != nil {
				return err
			}
		}
		frame.first = false
		frame.expectValue = true
		if err := writeJSONString(encoder.output, token.StringValue); err != nil {
			return err
		}
		return encoder.writeRaw(":")
	}

	if err := encoder.beforeValue(); err != nil {
		return err
	}
	switch token.Kind {
	case searchjobs.ValueVisitNull:
		return encoder.writeTypedScalar("null", "null")
	case searchjobs.ValueVisitString:
		return encoder.writeStringScalar("string", token.StringValue)
	case searchjobs.ValueVisitSigned:
		return encoder.writeTypedScalar("signed", encodeSignedJSON(token.SignedValue, encoder.integerEncoding))
	case searchjobs.ValueVisitUnsigned:
		return encoder.writeTypedScalar("unsigned", encodeUnsignedJSON(token.UnsignedValue, encoder.integerEncoding))
	case searchjobs.ValueVisitDouble:
		return encoder.writeTypedScalar("double", encodeDoubleJSON(token.DoubleValue))
	case searchjobs.ValueVisitBool:
		return encoder.writeTypedScalar("bool", strconv.FormatBool(token.BoolValue))
	case searchjobs.ValueVisitBytes:
		return encoder.writeBytesScalar(token.BytesValue)
	case searchjobs.ValueVisitTime:
		return encoder.writeStringScalar("time", token.TimeValue.UTC().Format(time.RFC3339Nano))
	case searchjobs.ValueVisitDuration:
		return encoder.writeStringScalar("duration", token.DurationValue.String())
	case searchjobs.ValueVisitDecimal:
		return encoder.writeStringScalar("decimal", token.StringValue)
	case searchjobs.ValueVisitListBegin:
		if err := encoder.writeTypedContainerStart("list", "["); err != nil {
			return err
		}
		encoder.stack = append(encoder.stack, jsonContainer{kind: jsonList, first: true})
	case searchjobs.ValueVisitObjectBegin:
		if err := encoder.writeTypedContainerStart("object", "{"); err != nil {
			return err
		}
		encoder.stack = append(encoder.stack, jsonContainer{kind: jsonObject, first: true})
	default:
		return errors.New("export source value token is invalid")
	}
	return nil
}

func (encoder *jsonValueEncoder) beforeValue() error {
	if len(encoder.stack) == 0 {
		return nil
	}
	frame := &encoder.stack[len(encoder.stack)-1]
	switch frame.kind {
	case jsonList:
		if !frame.first {
			if err := encoder.writeRaw(","); err != nil {
				return err
			}
		}
		frame.first = false
	case jsonObject:
		if !frame.expectValue {
			return errors.New("export source object value has no field")
		}
		frame.expectValue = false
	default:
		return errors.New("export source value container is invalid")
	}
	return nil
}

func (encoder *jsonValueEncoder) writeTypedScalar(kind, value string) error {
	if encoder.typeMetadata {
		if err := encoder.writeRaw(`{"$type":`); err != nil {
			return err
		}
		if err := writeJSONString(encoder.output, kind); err != nil {
			return err
		}
		if err := encoder.writeRaw(`,"$value":`); err != nil {
			return err
		}
	}
	if err := encoder.writeRaw(value); err != nil {
		return err
	}
	if encoder.typeMetadata {
		return encoder.writeRaw("}")
	}
	return nil
}

func (encoder *jsonValueEncoder) writeStringScalar(kind, value string) error {
	if !utf8.ValidString(value) {
		return errors.New("export source string is not valid UTF-8")
	}
	if encoder.typeMetadata {
		if err := encoder.writeRaw(`{"$type":`); err != nil {
			return err
		}
		if err := writeJSONString(encoder.output, kind); err != nil {
			return err
		}
		if err := encoder.writeRaw(`,"$value":`); err != nil {
			return err
		}
	}
	if err := writeJSONString(encoder.output, value); err != nil {
		return err
	}
	if encoder.typeMetadata {
		return encoder.writeRaw("}")
	}
	return nil
}

func (encoder *jsonValueEncoder) writeBytesScalar(value []byte) error {
	if encoder.typeMetadata {
		if err := encoder.writeRaw(`{"$type":"bytes","$value":`); err != nil {
			return err
		}
	}
	if err := encoder.writeRaw(`"`); err != nil {
		return err
	}
	encoded := base64.NewEncoder(base64.StdEncoding, encoder.output)
	if _, err := encoded.Write(value); err != nil {
		_ = encoded.Close()
		return err
	}
	if err := encoded.Close(); err != nil {
		return err
	}
	if err := encoder.writeRaw(`"`); err != nil {
		return err
	}
	if encoder.typeMetadata {
		return encoder.writeRaw("}")
	}
	return nil
}

func (encoder *jsonValueEncoder) writeTypedContainerStart(kind, delimiter string) error {
	if encoder.typeMetadata {
		if err := encoder.writeRaw(`{"$type":`); err != nil {
			return err
		}
		if err := writeJSONString(encoder.output, kind); err != nil {
			return err
		}
		if err := encoder.writeRaw(`,"$value":`); err != nil {
			return err
		}
	}
	return encoder.writeRaw(delimiter)
}

func (encoder *jsonValueEncoder) writeRaw(value string) error {
	_, err := io.WriteString(encoder.output, value)
	return err
}

func writeJSONString(output io.Writer, value string) error {
	if !utf8.ValidString(value) {
		return errors.New("export JSON field name is not valid UTF-8")
	}
	if _, err := io.WriteString(output, `"`); err != nil {
		return err
	}
	start := 0
	for index, character := range value {
		replacement := ""
		switch character {
		case '\\':
			replacement = `\\`
		case '"':
			replacement = `\"`
		case '\b':
			replacement = `\b`
		case '\f':
			replacement = `\f`
		case '\n':
			replacement = `\n`
		case '\r':
			replacement = `\r`
		case '\t':
			replacement = `\t`
		case '<':
			replacement = `\u003c`
		case '>':
			replacement = `\u003e`
		case '&':
			replacement = `\u0026`
		case '\u2028':
			replacement = `\u2028`
		case '\u2029':
			replacement = `\u2029`
		default:
			if character < 0x20 {
				replacement = `\u00` + string("0123456789abcdef"[byte(character)>>4]) + string("0123456789abcdef"[byte(character)&0xf])
			}
		}
		if replacement == "" {
			continue
		}
		if _, err := io.WriteString(output, value[start:index]); err != nil {
			return err
		}
		if _, err := io.WriteString(output, replacement); err != nil {
			return err
		}
		start = index + utf8.RuneLen(character)
	}
	if _, err := io.WriteString(output, value[start:]); err != nil {
		return err
	}
	_, err := io.WriteString(output, `"`)
	return err
}

func encodeSignedJSON(value int64, policy JSONIntegerEncoding) string {
	if policy == JSONIntegerString || value > int64(maximumSafeJSONInteger) || value < -int64(maximumSafeJSONInteger) {
		return strconv.Quote(strconv.FormatInt(value, 10))
	}
	return strconv.FormatInt(value, 10)
}

func encodeUnsignedJSON(value uint64, policy JSONIntegerEncoding) string {
	if policy == JSONIntegerString || value > maximumSafeJSONInteger {
		return strconv.Quote(strconv.FormatUint(value, 10))
	}
	return strconv.FormatUint(value, 10)
}

func encodeDoubleJSON(value float64) string {
	switch {
	case math.IsNaN(value):
		return `"NaN"`
	case math.IsInf(value, 1):
		return `"+Inf"`
	case math.IsInf(value, -1):
		return `"-Inf"`
	default:
		return strconv.FormatFloat(value, 'g', -1, 64)
	}
}
