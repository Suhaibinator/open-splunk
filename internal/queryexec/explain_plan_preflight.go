package queryexec

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// preflightExplainPlan enforces collection bounds before encoding/json can
// materialize the recursive Plans slices. The accepted document is decoded a
// second time into the small typed tree used for projection; both passes are
// linear in the already bounded result text.
func preflightExplainPlan(ctx context.Context, text string) error {
	if ctx == nil {
		return malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := expectExplainDelimiter(ctx, decoder, '['); err != nil {
		return err
	}
	if !decoder.More() {
		return malformedExplainPlan()
	}
	state := explainPlanPreflight{}
	if err := state.scanEnvelope(ctx, decoder); err != nil {
		return err
	}
	if decoder.More() {
		return malformedExplainPlan()
	}
	if err := expectExplainDelimiter(ctx, decoder, ']'); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return malformedExplainPlan()
	}
	return ctx.Err()
}

type explainPlanPreflight struct {
	nodes   uint64
	headers uint64
	indexes uint64
}

func (state *explainPlanPreflight) scanEnvelope(
	ctx context.Context,
	decoder *json.Decoder,
) error {
	if err := expectExplainDelimiter(ctx, decoder, '{'); err != nil {
		return err
	}
	seenPlan := false
	for decoder.More() {
		key, err := nextExplainObjectKey(ctx, decoder)
		if err != nil {
			return err
		}
		if !strings.EqualFold(key, "Plan") {
			if err := discardExplainValue(ctx, decoder, 1); err != nil {
				return err
			}
			continue
		}
		if seenPlan {
			return malformedExplainPlan()
		}
		seenPlan = true
		if err := state.scanNode(ctx, decoder); err != nil {
			return err
		}
	}
	if err := expectExplainDelimiter(ctx, decoder, '}'); err != nil {
		return err
	}
	if !seenPlan {
		return malformedExplainPlan()
	}
	return nil
}

func (state *explainPlanPreflight) scanNode(
	ctx context.Context,
	decoder *json.Decoder,
) error {
	if err := expectExplainDelimiter(ctx, decoder, '{'); err != nil {
		return err
	}
	if state.nodes >= maximumExplainPlanNodes {
		return explainLimit("structured plan exceeded the node limit")
	}
	state.nodes++

	var (
		nodeType                 string
		headerCount, indexCount  uint64
		seenNodeType, seenPlans  bool
		seenHeaders, seenIndexes bool
	)
	for decoder.More() {
		key, err := nextExplainObjectKey(ctx, decoder)
		if err != nil {
			return err
		}
		switch {
		case strings.EqualFold(key, "Node Type"):
			if seenNodeType {
				return malformedExplainPlan()
			}
			seenNodeType = true
			if err := decodeExplainScalar(
				ctx,
				decoder,
				&nodeType,
			); err != nil {
				return err
			}
		case strings.EqualFold(key, "Plans"):
			if seenPlans {
				return malformedExplainPlan()
			}
			seenPlans = true
			if err := state.scanPlans(ctx, decoder); err != nil {
				return err
			}
		case strings.EqualFold(key, "Header"):
			if seenHeaders {
				return malformedExplainPlan()
			}
			seenHeaders = true
			headerCount, err = scanExplainArray(
				ctx,
				decoder,
				maximumExplainPlanHeaders,
				func() error {
					return scanExplainHeader(ctx, decoder)
				},
				"structured plan exceeded the header limit",
			)
			if err != nil {
				return err
			}
		case strings.EqualFold(key, "Indexes"):
			if seenIndexes {
				return malformedExplainPlan()
			}
			seenIndexes = true
			indexCount, err = scanExplainArray(
				ctx,
				decoder,
				maximumExplainPlanIndexes,
				func() error {
					return scanExplainIndex(ctx, decoder)
				},
				"structured plan exceeded the index limit",
			)
			if err != nil {
				return err
			}
		case strings.EqualFold(key, "Actions"):
			return malformedExplainPlan()
		default:
			if err := discardExplainValue(ctx, decoder, 1); err != nil {
				return err
			}
		}
	}
	if err := expectExplainDelimiter(ctx, decoder, '}'); err != nil {
		return err
	}
	if !seenNodeType ||
		!validExplainMetadata(nodeType) {
		return malformedExplainPlan()
	}
	if nodeType != "ReadFromMergeTree" {
		return nil
	}
	if !seenHeaders || headerCount == 0 {
		return malformedExplainPlan()
	}
	if headerCount > maximumExplainPlanHeaders-state.headers {
		return explainLimit("structured plan exceeded the header limit")
	}
	if indexCount > maximumExplainPlanIndexes-state.indexes {
		return explainLimit("structured plan exceeded the index limit")
	}
	state.headers += headerCount
	state.indexes += indexCount
	return nil
}

func (state *explainPlanPreflight) scanPlans(
	ctx context.Context,
	decoder *json.Decoder,
) error {
	_, err := scanExplainArray(
		ctx,
		decoder,
		maximumExplainPlanChildren,
		func() error {
			return state.scanNode(ctx, decoder)
		},
		"structured plan exceeded the child limit",
	)
	return err
}

func scanExplainIndex(
	ctx context.Context,
	decoder *json.Decoder,
) error {
	if err := expectExplainDelimiter(ctx, decoder, '{'); err != nil {
		return err
	}
	const (
		indexFieldType uint16 = 1 << iota
		indexFieldName
		indexFieldKeys
		indexFieldInitialParts
		indexFieldSelectedParts
		indexFieldInitialGranules
		indexFieldSelectedGranules
	)
	var seen uint16
	for decoder.More() {
		key, err := nextExplainObjectKey(ctx, decoder)
		if err != nil {
			return err
		}
		var field uint16
		switch {
		case strings.EqualFold(key, "Type"):
			field = indexFieldType
		case strings.EqualFold(key, "Name"):
			field = indexFieldName
		case strings.EqualFold(key, "Keys"):
			field = indexFieldKeys
		case strings.EqualFold(key, "Initial Parts"):
			field = indexFieldInitialParts
		case strings.EqualFold(key, "Selected Parts"):
			field = indexFieldSelectedParts
		case strings.EqualFold(key, "Initial Granules"):
			field = indexFieldInitialGranules
		case strings.EqualFold(key, "Selected Granules"):
			field = indexFieldSelectedGranules
		}
		if field == 0 {
			if err := discardExplainValue(ctx, decoder, 1); err != nil {
				return err
			}
			continue
		}
		if seen&field != 0 {
			return malformedExplainPlan()
		}
		seen |= field
		if field == indexFieldKeys {
			if _, err := scanExplainArray(
				ctx,
				decoder,
				maximumExplainPlanIndexKeys,
				func() error {
					return discardExplainValue(ctx, decoder, 1)
				},
				"structured plan exceeded the index-key limit",
			); err != nil {
				return err
			}
			continue
		}
		if err := discardExplainValue(ctx, decoder, 1); err != nil {
			return err
		}
	}
	return expectExplainDelimiter(ctx, decoder, '}')
}

func scanExplainHeader(
	ctx context.Context,
	decoder *json.Decoder,
) error {
	if err := expectExplainDelimiter(ctx, decoder, '{'); err != nil {
		return err
	}
	var seenName, seenType bool
	for decoder.More() {
		key, err := nextExplainObjectKey(ctx, decoder)
		if err != nil {
			return err
		}
		switch {
		case strings.EqualFold(key, "Name"):
			if seenName {
				return malformedExplainPlan()
			}
			seenName = true
		case strings.EqualFold(key, "Type"):
			if seenType {
				return malformedExplainPlan()
			}
			seenType = true
		}
		if err := discardExplainValue(ctx, decoder, 1); err != nil {
			return err
		}
	}
	return expectExplainDelimiter(ctx, decoder, '}')
}

func scanExplainArray(
	ctx context.Context,
	decoder *json.Decoder,
	maximum uint64,
	scanValue func() error,
	limitMessage string,
) (uint64, error) {
	if err := expectExplainDelimiter(ctx, decoder, '['); err != nil {
		return 0, err
	}
	var count uint64
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if count >= maximum {
			return 0, explainLimit(limitMessage)
		}
		if err := scanValue(); err != nil {
			return 0, err
		}
		count++
	}
	if err := expectExplainDelimiter(ctx, decoder, ']'); err != nil {
		return 0, err
	}
	return count, nil
}

func discardExplainValue(
	ctx context.Context,
	decoder *json.Decoder,
	depth uint64,
) error {
	if depth > maximumExplainPlanNodes {
		return explainLimit("structured plan exceeded the JSON depth limit")
	}
	token, err := nextExplainToken(ctx, decoder)
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		for decoder.More() {
			if _, err := nextExplainObjectKey(ctx, decoder); err != nil {
				return err
			}
			if err := discardExplainValue(
				ctx,
				decoder,
				depth+1,
			); err != nil {
				return err
			}
		}
		return expectExplainDelimiter(ctx, decoder, '}')
	case '[':
		for decoder.More() {
			if err := discardExplainValue(
				ctx,
				decoder,
				depth+1,
			); err != nil {
				return err
			}
		}
		return expectExplainDelimiter(ctx, decoder, ']')
	default:
		return malformedExplainPlan()
	}
}

func decodeExplainScalar(
	ctx context.Context,
	decoder *json.Decoder,
	target any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := decoder.Decode(target); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func nextExplainObjectKey(
	ctx context.Context,
	decoder *json.Decoder,
) (string, error) {
	token, err := nextExplainToken(ctx, decoder)
	if err != nil {
		return "", err
	}
	key, ok := token.(string)
	if !ok {
		return "", malformedExplainPlan()
	}
	return key, nil
}

func expectExplainDelimiter(
	ctx context.Context,
	decoder *json.Decoder,
	want json.Delim,
) error {
	token, err := nextExplainToken(ctx, decoder)
	if err != nil {
		return err
	}
	got, ok := token.(json.Delim)
	if !ok || got != want {
		return malformedExplainPlan()
	}
	return nil
}

func nextExplainToken(
	ctx context.Context,
	decoder *json.Decoder,
) (json.Token, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := decoder.Token()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, malformedExplainPlan()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return token, nil
}
