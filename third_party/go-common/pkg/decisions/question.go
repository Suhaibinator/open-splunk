package decisions

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
)

// AnyQuestion is the common, sealed form of every typed Question. It permits
// differently typed questions to be collected in Request.Questions.
type AnyQuestion interface {
	definition() *questionDefinition
}

// Question is a typed handle used both to submit a question and retrieve its
// answer. A handle may be copied, but its identity must come from the constructor
// that created it: an answer can only be read with a handle submitted in that
// request. The zero value is invalid.
type Question[T any] struct {
	def      *questionDefinition
	_phantom [0]func() T
}

type questionDefinition struct {
	id     string
	wire   jsontext.Value
	decode func(jsontext.Value) (any, error)
}

func (q Question[T]) definition() *questionDefinition { return q.def }

// Answer retrieves this question's typed answer from response. It returns an
// error for a zero Question, a nil Response, or a Response produced for a request
// that did not contain this question. Question and Response may be read
// concurrently; callers must synchronize mutations to maps in returned answers.
func (q Question[T]) Answer(response *Response) (T, error) {
	var zero T
	if q.def == nil {
		return zero, errors.New("decisions: question is not initialized")
	}
	if response == nil {
		return zero, errors.New("decisions: response is nil")
	}
	answer, ok := response.answers[q.def]
	if !ok {
		return zero, fmt.Errorf("decisions: response has no answer for question %q", q.def.id)
	}
	typed, ok := answer.(T)
	if !ok {
		return zero, fmt.Errorf("decisions: response contains an invalid answer for question %q", q.def.id)
	}
	return typed, nil
}

// NoulCriteria optionally describes the true and false ends of a noul question.
// Both fields must contain initialized, non-null Content.
type NoulCriteria struct {
	True  Content
	False Content
}

// NewNoul constructs a probabilistic boolean question. id must be nonempty and
// instructions must be initialized, non-null Content. A nil criteria value omits
// the true and false descriptions.
func NewNoul(id string, instructions Content, criteria *NoulCriteria) (Question[NoulAnswer], error) {
	if err := validateQuestionBase(id, instructions); err != nil {
		return Question[NoulAnswer]{}, err
	}
	var wireCriteria any
	if criteria != nil {
		if err := criteria.True.validate(false); err != nil {
			return Question[NoulAnswer]{}, fmt.Errorf("decisions: invalid true criterion: %w", err)
		}
		if err := criteria.False.validate(false); err != nil {
			return Question[NoulAnswer]{}, fmt.Errorf("decisions: invalid false criterion: %w", err)
		}
		wireCriteria = map[string]Content{"true": criteria.True, "false": criteria.False}
	}
	wire, err := json.Marshal(struct {
		Type         string  `json:"type"`
		Instructions Content `json:"instructions"`
		Criteria     any     `json:"criteria,omitempty"`
	}{Type: "noul", Instructions: instructions, Criteria: wireCriteria})
	if err != nil {
		return Question[NoulAnswer]{}, err
	}
	return Question[NoulAnswer]{def: &questionDefinition{id: id, wire: wire, decode: decodeNoul}}, nil
}

// NewChoice constructs a question whose result retains the caller's named string
// type. id and every choice name must be nonempty, and criteria must contain at
// least one entry. A Null description leaves that choice undescribed.
func NewChoice[C ~string](id string, instructions Content, criteria map[C]Content) (Question[ChoiceAnswer[C]], error) {
	if err := validateQuestionBase(id, instructions); err != nil {
		return Question[ChoiceAnswer[C]]{}, err
	}
	if len(criteria) == 0 {
		return Question[ChoiceAnswer[C]]{}, errors.New("decisions: choice criteria must not be empty")
	}
	wireCriteria := make(map[string]Content, len(criteria))
	members := make(map[string]struct{}, len(criteria))
	for choice, description := range criteria {
		key := string(choice)
		if key == "" {
			return Question[ChoiceAnswer[C]]{}, errors.New("decisions: choice name must not be empty")
		}
		if err := description.validate(true); err != nil {
			return Question[ChoiceAnswer[C]]{}, fmt.Errorf("decisions: invalid description for choice %q: %w", key, err)
		}
		wireCriteria[key] = description
		members[key] = struct{}{}
	}
	wire, err := json.Marshal(struct {
		Type         string             `json:"type"`
		Instructions Content            `json:"instructions"`
		Criteria     map[string]Content `json:"criteria"`
	}{Type: "choice", Instructions: instructions, Criteria: wireCriteria})
	if err != nil {
		return Question[ChoiceAnswer[C]]{}, err
	}
	return Question[ChoiceAnswer[C]]{def: &questionDefinition{id: id, wire: wire, decode: decodeChoice[C](members)}}, nil
}

// NewScore constructs a question answered on a zero-based ordered scale. Each
// criterion describes its position by slice index, and at least one initialized,
// non-null criterion is required. The criteria slice is copied before return.
func NewScore(id string, instructions Content, criteria []Content) (Question[ScoreAnswer], error) {
	if err := validateQuestionBase(id, instructions); err != nil {
		return Question[ScoreAnswer]{}, err
	}
	if len(criteria) == 0 {
		return Question[ScoreAnswer]{}, errors.New("decisions: score criteria must not be empty")
	}
	criteriaCopy := append([]Content(nil), criteria...)
	for index, criterion := range criteriaCopy {
		if err := criterion.validate(false); err != nil {
			return Question[ScoreAnswer]{}, fmt.Errorf("decisions: invalid score criterion %d: %w", index, err)
		}
	}
	wire, err := json.Marshal(struct {
		Type         string    `json:"type"`
		Instructions Content   `json:"instructions"`
		Criteria     []Content `json:"criteria"`
	}{Type: "score", Instructions: instructions, Criteria: criteriaCopy})
	if err != nil {
		return Question[ScoreAnswer]{}, err
	}
	return Question[ScoreAnswer]{def: &questionDefinition{id: id, wire: wire, decode: decodeScore(len(criteriaCopy))}}, nil
}

func validateQuestionBase(id string, instructions Content) error {
	if id == "" {
		return errors.New("decisions: question ID must not be empty")
	}
	if err := instructions.validate(false); err != nil {
		return fmt.Errorf("decisions: invalid instructions: %w", err)
	}
	return nil
}
