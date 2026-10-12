package decisions

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// NoulAnswer is a probabilistic boolean answer. Noul is a probability in [0, 1]:
// zero means false and one means true.
type NoulAnswer struct {
	Noul float64
}

// ChoiceAnswer is an answer selected from a question's typed choices. Confidence
// and Probabilities are optional provider metadata: Confidence is nil and
// Probabilities is nil when the corresponding field is omitted. All reported
// probabilities are in [0, 1].
type ChoiceAnswer[C ~string] struct {
	Choice        C
	Confidence    *float64
	Probabilities map[C]float64
}

// ScoreAnswer is a position on the zero-based scale supplied by a score question.
// Score may be fractional, representing a position between criteria, and lies
// between zero and the last criterion index. Confidence, Probabilities, and
// Legend are optional provider metadata and remain nil when omitted. Probability
// and legend map keys are zero-based criterion indexes.
type ScoreAnswer struct {
	Score         float64
	Confidence    *float64
	Probabilities map[int]float64
	Legend        map[int]Content
}

func decodeNoul(raw jsontext.Value) (any, error) {
	var wire struct {
		Type string   `json:"type"`
		Noul *float64 `json:"noul"`
	}
	if err := decodeAnswer(raw, &wire); err != nil {
		return nil, err
	}
	if wire.Type != "noul" {
		return nil, fmt.Errorf("decisions: expected noul answer, got %q", wire.Type)
	}
	if wire.Noul == nil {
		return nil, errors.New("decisions: noul answer is missing noul")
	}
	if err := validateProbability(*wire.Noul, "noul"); err != nil {
		return nil, err
	}
	return NoulAnswer{Noul: *wire.Noul}, nil
}

func decodeChoice[C ~string](members map[string]struct{}) func(jsontext.Value) (any, error) {
	return func(raw jsontext.Value) (any, error) {
		var wire struct {
			Type          string              `json:"type"`
			Choice        *string             `json:"choice"`
			Confidence    *float64            `json:"confidence"`
			Probabilities map[string]*float64 `json:"probabilities"`
		}
		if err := decodeAnswer(raw, &wire); err != nil {
			return nil, err
		}
		if wire.Type != "choice" {
			return nil, fmt.Errorf("decisions: expected choice answer, got %q", wire.Type)
		}
		if wire.Choice == nil {
			return nil, errors.New("decisions: choice answer is missing choice")
		}
		if _, ok := members[*wire.Choice]; !ok {
			return nil, fmt.Errorf("decisions: answer contains unknown choice %q", *wire.Choice)
		}
		if wire.Confidence != nil {
			if err := validateProbability(*wire.Confidence, "confidence"); err != nil {
				return nil, err
			}
		}
		probabilities := make(map[C]float64, len(wire.Probabilities))
		for key, value := range wire.Probabilities {
			if _, ok := members[key]; !ok {
				return nil, fmt.Errorf("decisions: probabilities contain unknown choice %q", key)
			}
			if value == nil {
				return nil, fmt.Errorf("decisions: probability for choice %q is null", key)
			}
			if err := validateProbability(*value, "probability"); err != nil {
				return nil, err
			}
			probabilities[C(key)] = *value
		}
		if wire.Probabilities == nil {
			probabilities = nil
		}
		return ChoiceAnswer[C]{Choice: C(*wire.Choice), Confidence: wire.Confidence, Probabilities: probabilities}, nil
	}
}

func decodeScore(criteriaCount int) func(jsontext.Value) (any, error) {
	return func(raw jsontext.Value) (any, error) {
		var wire struct {
			Type          string                    `json:"type"`
			Score         *float64                  `json:"score"`
			Confidence    *float64                  `json:"confidence"`
			Probabilities map[string]*float64       `json:"probabilities"`
			Legend        map[string]jsontext.Value `json:"legend"`
		}
		if err := decodeAnswer(raw, &wire); err != nil {
			return nil, err
		}
		if wire.Type != "score" {
			return nil, fmt.Errorf("decisions: expected score answer, got %q", wire.Type)
		}
		if wire.Score == nil {
			return nil, errors.New("decisions: score answer is missing score")
		}
		if math.IsNaN(*wire.Score) || math.IsInf(*wire.Score, 0) || *wire.Score < 0 || *wire.Score > float64(criteriaCount-1) {
			return nil, fmt.Errorf("decisions: score %v is outside [0, %d]", *wire.Score, criteriaCount-1)
		}
		if wire.Confidence != nil {
			if err := validateProbability(*wire.Confidence, "confidence"); err != nil {
				return nil, err
			}
		}
		probabilities := make(map[int]float64, len(wire.Probabilities))
		for key, value := range wire.Probabilities {
			index, err := parseScaleIndex(key, criteriaCount)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, fmt.Errorf("decisions: probability for scale index %q is null", key)
			}
			if err := validateProbability(*value, "probability"); err != nil {
				return nil, err
			}
			probabilities[index] = *value
		}
		if wire.Probabilities == nil {
			probabilities = nil
		}
		legend := make(map[int]Content, len(wire.Legend))
		for key, value := range wire.Legend {
			index, err := parseScaleIndex(key, criteriaCount)
			if err != nil {
				return nil, err
			}
			content, err := parseContent(value, false)
			if err != nil {
				return nil, fmt.Errorf("decisions: invalid legend entry %q: %w", key, err)
			}
			legend[index] = content
		}
		if wire.Legend == nil {
			legend = nil
		}
		return ScoreAnswer{Score: *wire.Score, Confidence: wire.Confidence, Probabilities: probabilities, Legend: legend}, nil
	}
}

func decodeAnswer(raw jsontext.Value, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decisions: decode answer: %w", err)
	}
	return nil
}

func validateProbability(value float64, field string) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return fmt.Errorf("decisions: %s %v is outside [0, 1]", field, value)
	}
	return nil
}

func parseScaleIndex(value string, count int) (int, error) {
	index, err := strconv.Atoi(value)
	if err != nil || strconv.Itoa(index) != value || index < 0 || index >= count {
		return 0, fmt.Errorf("decisions: scale index %q is outside [0, %d]", value, count-1)
	}
	return index, nil
}
