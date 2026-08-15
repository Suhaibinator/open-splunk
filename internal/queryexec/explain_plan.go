package queryexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maximumExplainPlanNodes       = 4_096
	maximumExplainPlanReads       = 256
	maximumExplainPlanHeaders     = 4_096
	maximumExplainPlanIndexes     = 4_096
	maximumExplainPlanChildren    = 1_024
	maximumExplainPlanIndexKeys   = 64
	maximumExplainPlanMetadataLen = 16 << 10
)

// ExplainPlan is the safe, detached structural projection of one fixed
// ClickHouse PLAN. Conditions, descriptions, node IDs, column types, query
// arguments, rendered expressions, and other literal-bearing fields are
// deliberately omitted.
type ExplainPlan struct {
	NodeTypes []string
	Reads     []ExplainRead
}

// ExplainRead describes one physical MergeTree read after ClickHouse plan
// optimization. Columns contains only canonical event-storage or fixed
// MergeTree virtual columns that ClickHouse emitted as direct header names.
// Rendered header expressions are never published.
type ExplainRead struct {
	Columns []string
	Indexes []ExplainIndex
}

// ExplainIndex contains only bounded index-selection counters and safe schema
// names. Keys is the allowlisted subset of storage columns and fixed schema
// expressions; query-derived rendered expressions are never published.
// Selected counts must not exceed their corresponding initial counts.
type ExplainIndex struct {
	Type             string
	Name             string
	Keys             []string
	InitialParts     uint64
	SelectedParts    uint64
	InitialGranules  uint64
	SelectedGranules uint64
}

type rawExplainEnvelope struct {
	Plan *rawExplainNode `json:"Plan"`
}

type rawExplainNode struct {
	NodeType string           `json:"Node Type"`
	Plans    []rawExplainNode `json:"Plans"`
	Header   json.RawMessage  `json:"Header"`
	Indexes  json.RawMessage  `json:"Indexes"`
	Actions  json.RawMessage  `json:"Actions"`
}

type rawExplainHeader struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type rawExplainIndex struct {
	Type             string   `json:"Type"`
	Name             string   `json:"Name"`
	Keys             []string `json:"Keys"`
	InitialParts     *uint64  `json:"Initial Parts"`
	SelectedParts    *uint64  `json:"Selected Parts"`
	InitialGranules  *uint64  `json:"Initial Granules"`
	SelectedGranules *uint64  `json:"Selected Granules"`
}

// ParseExplainPlan validates the complete fixed structured-PLAN contract and
// returns a detached, literal-free physical projection. It accepts only the
// one-envelope JSON shape emitted by the repository-pinned ClickHouse release.
// Every present MergeTree read must carry a physical header; index evidence is
// retained when ClickHouse emits it. ReadNothing plans and reads for which the
// optimizer omits Indexes are valid. Action output is always rejected.
func ParseExplainPlan(result ExplainResult) (ExplainPlan, error) {
	if err := ValidateExplainResult(result); err != nil {
		return ExplainPlan{}, err
	}
	return parseExplainPlanText(context.Background(), result.Text)
}

func parseExplainPlanText(
	ctx context.Context,
	text string,
) (ExplainPlan, error) {
	if ctx == nil {
		return ExplainPlan{}, malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return ExplainPlan{}, err
	}
	if err := preflightExplainPlan(ctx, text); err != nil {
		return ExplainPlan{}, err
	}
	if err := ctx.Err(); err != nil {
		return ExplainPlan{}, err
	}
	var envelopes []rawExplainEnvelope
	decoder := json.NewDecoder(strings.NewReader(text))
	if err := decoder.Decode(&envelopes); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return ExplainPlan{}, contextErr
		}
		return ExplainPlan{}, malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return ExplainPlan{}, err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if contextErr := ctx.Err(); contextErr != nil {
			return ExplainPlan{}, contextErr
		}
		return ExplainPlan{}, malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return ExplainPlan{}, err
	}
	if len(envelopes) != 1 ||
		envelopes[0].Plan == nil {
		return ExplainPlan{}, malformedExplainPlan()
	}

	projected := ExplainPlan{
		NodeTypes: make([]string, 0, 32),
		Reads:     make([]ExplainRead, 0, 1),
	}
	stack := []*rawExplainNode{envelopes[0].Plan}
	var nodeCount, headerCount, indexCount uint64
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return ExplainPlan{}, err
		}
		if nodeCount >= maximumExplainPlanNodes {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the node limit",
			)
		}
		nodeCount++
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !validExplainMetadata(node.NodeType) || len(node.Actions) != 0 {
			return ExplainPlan{}, malformedExplainPlan()
		}
		if nodeType, safe := projectExplainNodeType(node.NodeType); safe {
			projected.NodeTypes = append(
				projected.NodeTypes,
				nodeType,
			)
		}

		if len(node.Plans) > maximumExplainPlanChildren {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the child limit",
			)
		}
		for _, v := range slices.Backward(node.Plans) {
			stack = append(stack, &v)
		}

		if node.NodeType != "ReadFromMergeTree" {
			continue
		}
		if len(projected.Reads) >= maximumExplainPlanReads {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the read limit",
			)
		}
		read, addedHeaders, addedIndexes, err :=
			decodeExplainRead(ctx, node.Header, node.Indexes)
		if err != nil {
			return ExplainPlan{}, err
		}
		if addedHeaders > maximumExplainPlanHeaders-headerCount {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the header limit",
			)
		}
		if addedIndexes > maximumExplainPlanIndexes-indexCount {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the index limit",
			)
		}
		headerCount += addedHeaders
		indexCount += addedIndexes
		projected.Reads = append(projected.Reads, read)
	}
	if err := ctx.Err(); err != nil {
		return ExplainPlan{}, err
	}
	return projected, nil
}

func decodeExplainRead(
	ctx context.Context,
	rawHeaders json.RawMessage,
	rawIndexes json.RawMessage,
) (ExplainRead, uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return ExplainRead{}, 0, 0, err
	}
	if len(rawHeaders) == 0 ||
		bytes.Equal(bytes.TrimSpace(rawHeaders), []byte("null")) {
		return ExplainRead{}, 0, 0, malformedExplainPlan()
	}

	var headers []rawExplainHeader
	if err := json.Unmarshal(rawHeaders, &headers); err != nil ||
		len(headers) == 0 {
		if contextErr := ctx.Err(); contextErr != nil {
			return ExplainRead{}, 0, 0, contextErr
		}
		return ExplainRead{}, 0, 0, malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return ExplainRead{}, 0, 0, err
	}
	if len(headers) > maximumExplainPlanHeaders {
		return ExplainRead{}, 0, 0, explainLimit(
			"structured plan exceeded the header limit",
		)
	}
	columns := make([]string, 0, len(headers))
	for _, header := range headers {
		if err := ctx.Err(); err != nil {
			return ExplainRead{}, 0, 0, err
		}
		if !validExplainMetadata(header.Name) || !validExplainMetadata(header.Type) {
			return ExplainRead{}, 0, 0, malformedExplainPlan()
		}
		if safeExplainPhysicalColumn(header.Name) {
			columns = append(columns, strings.Clone(header.Name))
		}
	}
	var rawIndexValues []rawExplainIndex
	if len(rawIndexes) != 0 {
		if bytes.Equal(bytes.TrimSpace(rawIndexes), []byte("null")) {
			return ExplainRead{}, 0, 0, malformedExplainPlan()
		}
		if err := json.Unmarshal(rawIndexes, &rawIndexValues); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return ExplainRead{}, 0, 0, contextErr
			}
			return ExplainRead{}, 0, 0, malformedExplainPlan()
		}
		if err := ctx.Err(); err != nil {
			return ExplainRead{}, 0, 0, err
		}
	}
	if len(rawIndexValues) > maximumExplainPlanIndexes {
		return ExplainRead{}, 0, 0, explainLimit(
			"structured plan exceeded the index limit",
		)
	}
	indexes := make([]ExplainIndex, 0, len(rawIndexValues))
	for _, rawIndex := range rawIndexValues {
		if err := ctx.Err(); err != nil {
			return ExplainRead{}, 0, 0, err
		}
		projected, safe, err := projectExplainIndex(rawIndex)
		if err != nil {
			return ExplainRead{}, 0, 0, err
		}
		if safe {
			indexes = append(indexes, projected)
		}
	}
	return ExplainRead{
			Columns: columns,
			Indexes: indexes,
		},
		uint64(len(headers)),
		uint64(len(rawIndexValues)),
		nil
}

func projectExplainIndex(
	raw rawExplainIndex,
) (ExplainIndex, bool, error) {
	if !validExplainMetadata(raw.Type) ||
		(raw.Name != "" &&
			!validExplainMetadata(raw.Name)) ||
		len(raw.Keys) > maximumExplainPlanIndexKeys ||
		raw.InitialParts == nil ||
		raw.SelectedParts == nil ||
		raw.InitialGranules == nil ||
		raw.SelectedGranules == nil ||
		*raw.SelectedParts > *raw.InitialParts ||
		*raw.SelectedGranules > *raw.InitialGranules {
		return ExplainIndex{}, false, malformedExplainPlan()
	}
	indexType, safe := projectExplainIndexType(raw.Type)
	if !safe {
		for _, key := range raw.Keys {
			if !validExplainMetadata(key) {
				return ExplainIndex{}, false, malformedExplainPlan()
			}
		}
		return ExplainIndex{}, false, nil
	}
	keys := make([]string, 0, len(raw.Keys))
	for _, key := range raw.Keys {
		if !validExplainMetadata(key) {
			return ExplainIndex{}, false, malformedExplainPlan()
		}
		if safeExplainIndexKey(indexType, raw.Name, key) {
			keys = append(keys, strings.Clone(key))
		}
	}
	return ExplainIndex{
		Type:             indexType,
		Name:             projectExplainIndexName(indexType, raw.Name),
		Keys:             keys,
		InitialParts:     *raw.InitialParts,
		SelectedParts:    *raw.SelectedParts,
		InitialGranules:  *raw.InitialGranules,
		SelectedGranules: *raw.SelectedGranules,
	}, true, nil
}

func projectExplainNodeType(value string) (string, bool) {
	switch value {
	case "Aggregating",
		"CreatingSet",
		"CreatingSets",
		"Expression",
		"Filter",
		"Join",
		"JoinLazyColumnsStep",
		"LazilyReadFromMergeTree",
		"Limit",
		"MaterializingCTE",
		"MaterializingCTEs",
		"ReadFromMemoryStorage",
		"ReadFromMergeTree",
		"ReadFromSystemNumbers",
		"ReadNothing",
		"Sorting",
		"Union",
		"Window":
		return strings.Clone(value), true
	default:
		return "", false
	}
}

func projectExplainIndexType(value string) (string, bool) {
	switch value {
	case "Min-Max":
		// ClickHouse 26.7 renamed the part-level MinMax evidence label from
		// "MinMax" to "Min-Max"; both spell the same index, so the published
		// projection keeps the canonical "MinMax" type.
		return "MinMax", true
	case "MinMax", "Partition", "PrimaryKey", "Skip":
		return strings.Clone(value), true
	default:
		return "", false
	}
}

func projectExplainIndexName(indexType, value string) string {
	if indexType != "Skip" {
		return ""
	}
	switch value {
	case "idx_event_id",
		"idx_trace_id",
		"idx_span_id",
		"idx_field_names",
		"idx_raw_text",
		"idx_visibility_seq":
		return value
	default:
		return ""
	}
}

func safeExplainIndexKey(indexType, indexName, key string) bool {
	switch indexType {
	case "MinMax":
		return key == "event_time"
	case "Partition":
		return key == "toYYYYMM(event_time)"
	case "PrimaryKey":
		switch key {
		case "tenant_id",
			"index_name",
			"toStartOfHour(event_time)",
			"event_time":
			return true
		default:
			return false
		}
	case "Skip":
		switch indexName {
		case "idx_event_id":
			return key == "event_id"
		case "idx_trace_id":
			return key == "ifNull(trace_id, '')"
		case "idx_span_id":
			return key == "ifNull(span_id, '')"
		case "idx_field_names":
			return key == "field_names"
		case "idx_raw_text":
			return key == "lowerUTF8(raw)"
		case "idx_visibility_seq":
			return key == "visibility_seq"
		default:
			return false
		}
	default:
		return false
	}
}

func safeExplainPhysicalColumn(value string) bool {
	switch value {
	case "event_id",
		"tenant_id",
		"index_name",
		"event_time",
		"index_time",
		"collected_at",
		"event_time_source",
		"host",
		"source",
		"sourcetype",
		"service",
		"severity",
		"level",
		"body",
		"raw",
		"raw_encoding",
		"trace_id",
		"span_id",
		"fields",
		"field_names",
		"field_types",
		"field_metadata_version",
		"collector_id",
		"batch_id",
		"batch_sequence",
		"visibility_seq",
		"expires_at",
		"_part_starting_offset",
		"_part_offset":
		return true
	default:
		return false
	}
}

func validExplainMetadata(value string) bool {
	if value == "" ||
		len(value) > maximumExplainPlanMetadataLen ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func malformedExplainPlan() error {
	return invalidExplainResult("returned a malformed structured plan")
}
