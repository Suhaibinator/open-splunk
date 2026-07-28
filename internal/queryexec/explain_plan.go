package queryexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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
// arguments, and other literal-bearing fields are deliberately omitted.
type ExplainPlan struct {
	NodeTypes []string
	Reads     []ExplainRead
}

// ExplainRead describes one physical MergeTree read after ClickHouse plan
// optimization. Columns are the physical read header, not the public result
// schema.
type ExplainRead struct {
	Columns []string
	Indexes []ExplainIndex
}

// ExplainIndex contains only bounded index-selection counters and safe schema
// names. Selected counts must not exceed their corresponding initial counts.
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
	var headerCount, indexCount uint64
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return ExplainPlan{}, err
		}
		if len(projected.NodeTypes) >= maximumExplainPlanNodes {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the node limit",
			)
		}
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !validExplainMetadata(
			node.NodeType,
			maximumExplainPlanMetadataLen,
		) || len(node.Actions) != 0 {
			return ExplainPlan{}, malformedExplainPlan()
		}
		projected.NodeTypes = append(
			projected.NodeTypes,
			strings.Clone(node.NodeType),
		)

		if len(node.Plans) > maximumExplainPlanChildren {
			return ExplainPlan{}, explainLimit(
				"structured plan exceeded the child limit",
			)
		}
		for index := len(node.Plans) - 1; index >= 0; index-- {
			stack = append(stack, &node.Plans[index])
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
	columns := make([]string, len(headers))
	for index, header := range headers {
		if err := ctx.Err(); err != nil {
			return ExplainRead{}, 0, 0, err
		}
		if !validExplainMetadata(
			header.Name,
			maximumExplainPlanMetadataLen,
		) || !validExplainMetadata(
			header.Type,
			maximumExplainPlanMetadataLen,
		) {
			return ExplainRead{}, 0, 0, malformedExplainPlan()
		}
		columns[index] = strings.Clone(header.Name)
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
	indexes := make([]ExplainIndex, len(rawIndexValues))
	for index, rawIndex := range rawIndexValues {
		if err := ctx.Err(); err != nil {
			return ExplainRead{}, 0, 0, err
		}
		projected, err := projectExplainIndex(rawIndex)
		if err != nil {
			return ExplainRead{}, 0, 0, err
		}
		indexes[index] = projected
	}
	return ExplainRead{
			Columns: columns,
			Indexes: indexes,
		},
		uint64(len(columns)),
		uint64(len(indexes)),
		nil
}

func projectExplainIndex(raw rawExplainIndex) (ExplainIndex, error) {
	if !validExplainMetadata(raw.Type, maximumExplainPlanMetadataLen) ||
		(raw.Name != "" &&
			!validExplainMetadata(raw.Name, maximumExplainPlanMetadataLen)) ||
		len(raw.Keys) > maximumExplainPlanIndexKeys ||
		raw.InitialParts == nil ||
		raw.SelectedParts == nil ||
		raw.InitialGranules == nil ||
		raw.SelectedGranules == nil ||
		*raw.SelectedParts > *raw.InitialParts ||
		*raw.SelectedGranules > *raw.InitialGranules {
		return ExplainIndex{}, malformedExplainPlan()
	}
	keys := make([]string, len(raw.Keys))
	for index, key := range raw.Keys {
		if !validExplainMetadata(key, maximumExplainPlanMetadataLen) {
			return ExplainIndex{}, malformedExplainPlan()
		}
		keys[index] = strings.Clone(key)
	}
	return ExplainIndex{
		Type:             strings.Clone(raw.Type),
		Name:             strings.Clone(raw.Name),
		Keys:             keys,
		InitialParts:     *raw.InitialParts,
		SelectedParts:    *raw.SelectedParts,
		InitialGranules:  *raw.InitialGranules,
		SelectedGranules: *raw.SelectedGranules,
	}, nil
}

func validExplainMetadata(value string, maximumBytes int) bool {
	if value == "" ||
		len(value) > maximumBytes ||
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
