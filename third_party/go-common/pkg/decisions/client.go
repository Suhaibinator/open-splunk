package decisions

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	defaultBaseURL      = "https://openrouter.ai/api/alpha"
	defaultEndpointPath = "decisions"
	defaultModel        = "~typesafe/jev-latest"
	defaultTimeout      = 60 * time.Second
	maxResponseBytes    = 16 << 20
)

// Config configures a Decisions API client. Empty fields use the defaults
// documented on each field.
type Config struct {
	// APIKey is the bearer token sent to the Decisions endpoint. It is required;
	// NewClient does not read credentials from the environment.
	APIKey string
	// BaseURL is the absolute HTTP or HTTPS base URL. It defaults to
	// https://openrouter.ai/api/alpha and may include a path prefix.
	BaseURL string
	// EndpointPath is appended to BaseURL's existing path. It defaults to
	// "decisions" and must be a URL path without a query or fragment. A leading
	// slash is trimmed; it does not replace BaseURL's path prefix.
	EndpointPath string
	// Model is the default model for requests. It defaults to
	// "~typesafe/jev-latest" and can be overridden by Request.Model.
	Model string
	// HTTPClient supplies the transport settings used by the client. NewClient
	// makes a shallow copy, preserves its Timeout (including a zero timeout), and
	// disables redirect following on the copy. If nil, a client with a 60-second
	// timeout is used.
	HTTPClient *http.Client
}

// Client calls a Decisions-compatible HTTP endpoint. It is safe for concurrent use.
type Client struct {
	apiKey     string
	endpoint   string
	model      string
	httpClient *http.Client
}

// Request is a batch of typed questions about a state. State and Questions are
// required. An empty Model uses the default configured on Client.
type Request struct {
	State     Content
	Model     string
	Questions []AnyQuestion
}

// Usage is the token and optional cost metadata returned by the provider. Cost
// is nil when the provider omits it; its currency or unit is defined by the
// configured provider.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	Cost         *float64
}

// Response contains provider metadata and validated typed answers. Answers are
// retrieved with the same Question values submitted in Request.Questions;
// callers do not need to parse or unmarshal the provider response.
type Response struct {
	Model    string
	ID       string
	Provider string
	Usage    Usage
	answers  map[*questionDefinition]any
}

// NewClient validates config and constructs a reusable client. APIKey must be
// supplied explicitly. Empty BaseURL, EndpointPath, and Model fields select
// package defaults; see Config for details.
func NewClient(config Config) (*Client, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, fmt.Errorf("%w: API key must not be empty", ErrInvalidConfig)
	}
	base := config.BaseURL
	if base == "" {
		base = defaultBaseURL
	}
	endpointPath := config.EndpointPath
	if endpointPath == "" {
		endpointPath = defaultEndpointPath
	}
	endpoint, err := resolveEndpoint(base, endpointPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	model := config.Model
	if model == "" {
		model = defaultModel
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("%w: model must not be empty", ErrInvalidConfig)
	}

	var httpClient http.Client
	if config.HTTPClient == nil {
		httpClient.Timeout = defaultTimeout
	} else {
		httpClient = *config.HTTPClient
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{apiKey: config.APIKey, endpoint: endpoint, model: model, httpClient: &httpClient}, nil
}

func resolveEndpoint(base, endpointPath string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}
	if (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return "", errors.New("base URL must be an absolute HTTP or HTTPS URL")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return "", errors.New("base URL must not contain user information, a query, or a fragment")
	}
	if strings.TrimSpace(endpointPath) == "" {
		return "", errors.New("endpoint path must not be empty")
	}
	pathURL, err := url.Parse(endpointPath)
	if err != nil || pathURL.IsAbs() || pathURL.Host != "" || pathURL.RawQuery != "" || pathURL.Fragment != "" {
		return "", errors.New("endpoint path must be a URL path")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/") + "/" + strings.TrimLeft(pathURL.Path, "/")
	baseURL.RawPath = ""
	return baseURL.String(), nil
}

// Decide submits all questions and validates every requested answer before
// returning. It rejects an uninitialized state, missing or duplicate questions,
// and nil context. A nonempty Request.Model overrides the client's default.
//
// Decide handles request serialization and response decoding. On success, use
// Question.Answer to obtain each typed result. Non-2xx responses return an
// *HTTPError; malformed successful responses wrap ErrInvalidResponse. Response
// bodies larger than 16 MiB also wrap ErrInvalidResponse, regardless of status.
func (c *Client) Decide(ctx context.Context, request Request) (*Response, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidConfig)
	}
	if c.httpClient == nil || c.endpoint == "" || c.apiKey == "" || c.model == "" {
		return nil, fmt.Errorf("%w: client is not initialized", ErrInvalidConfig)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidRequest)
	}
	if err := request.State.validate(false); err != nil {
		return nil, fmt.Errorf("%w: invalid state: %v", ErrInvalidRequest, err)
	}
	if len(request.Questions) == 0 {
		return nil, fmt.Errorf("%w: questions must not be empty", ErrInvalidRequest)
	}

	definitions := make([]*questionDefinition, 0, len(request.Questions))
	wireQuestions := make(map[string]jsontext.Value, len(request.Questions))
	for index, question := range request.Questions {
		if question == nil || isNilQuestion(question) {
			return nil, fmt.Errorf("%w: question %d is nil", ErrInvalidRequest, index)
		}
		definition := question.definition()
		if definition == nil || definition.id == "" || len(definition.wire) == 0 || definition.decode == nil {
			return nil, fmt.Errorf("%w: question %d is not initialized", ErrInvalidRequest, index)
		}
		if _, duplicate := wireQuestions[definition.id]; duplicate {
			return nil, fmt.Errorf("%w: duplicate question ID %q", ErrInvalidRequest, definition.id)
		}
		definitions = append(definitions, definition)
		wireQuestions[definition.id] = definition.wire
	}
	model := request.Model
	if model == "" {
		model = c.model
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("%w: model must not be empty", ErrInvalidRequest)
	}
	body, err := json.Marshal(struct {
		Model     string                    `json:"model"`
		State     Content                   `json:"state"`
		Questions map[string]jsontext.Value `json:"questions"`
	}{Model: model, State: request.State, Questions: wireQuestions})
	if err != nil {
		return nil, fmt.Errorf("%w: encode request: %v", ErrInvalidRequest, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("decisions: create request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	httpResponse, err := c.httpClient.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("decisions: send request: %w", err)
	}
	defer httpResponse.Body.Close()
	responseBody, err := readBounded(httpResponse.Body)
	if err != nil {
		return nil, err
	}
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return nil, newHTTPError(httpResponse, responseBody)
	}
	return decodeResponse(responseBody, definitions)
}

func isNilQuestion(question AnyQuestion) bool {
	value := reflect.ValueOf(question)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func readBounded(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("decisions: read response: %w", err)
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("%w: response exceeds %d bytes", ErrInvalidResponse, maxResponseBytes)
	}
	return data, nil
}

func decodeResponse(body []byte, definitions []*questionDefinition) (*Response, error) {
	var wire struct {
		Model    *string                   `json:"model"`
		ID       string                    `json:"id"`
		Provider string                    `json:"provider"`
		Answers  map[string]jsontext.Value `json:"answers"`
		Usage    *struct {
			InputTokens  *int64   `json:"input_tokens"`
			OutputTokens *int64   `json:"output_tokens"`
			Cost         *float64 `json:"cost"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("%w: decode envelope: %v", ErrInvalidResponse, err)
	}
	if wire.Model == nil || *wire.Model == "" {
		return nil, fmt.Errorf("%w: response is missing model", ErrInvalidResponse)
	}
	if wire.Answers == nil {
		return nil, fmt.Errorf("%w: response is missing answers", ErrInvalidResponse)
	}
	if wire.Usage == nil || wire.Usage.InputTokens == nil || wire.Usage.OutputTokens == nil {
		return nil, fmt.Errorf("%w: response is missing usage token counts", ErrInvalidResponse)
	}
	if *wire.Usage.InputTokens < 0 || *wire.Usage.OutputTokens < 0 {
		return nil, fmt.Errorf("%w: usage token counts must not be negative", ErrInvalidResponse)
	}
	if wire.Usage.Cost != nil && *wire.Usage.Cost < 0 {
		return nil, fmt.Errorf("%w: usage cost must not be negative", ErrInvalidResponse)
	}
	answers := make(map[*questionDefinition]any, len(definitions))
	for _, definition := range definitions {
		raw, ok := wire.Answers[definition.id]
		if !ok {
			return nil, fmt.Errorf("%w: response is missing answer for question %q", ErrInvalidResponse, definition.id)
		}
		answer, err := definition.decode(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid answer for question %q: %v", ErrInvalidResponse, definition.id, err)
		}
		answers[definition] = answer
	}
	return &Response{
		Model: *wire.Model, ID: wire.ID, Provider: wire.Provider,
		Usage:   Usage{InputTokens: *wire.Usage.InputTokens, OutputTokens: *wire.Usage.OutputTokens, Cost: wire.Usage.Cost},
		answers: answers,
	}, nil
}

func newHTTPError(response *http.Response, body []byte) *HTTPError {
	details := append(jsontext.Value(nil), body...)
	result := &HTTPError{StatusCode: response.StatusCode, RetryAfter: parseRetryAfter(response.Header.Get("Retry-After")), Details: details}
	var envelope struct {
		Error *ProviderError `json:"error"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		result.ProviderError = envelope.Error
	}
	return result
}

func parseRetryAfter(value string) time.Duration {
	const maxSeconds = int64(^uint64(0)>>1) / int64(time.Second)
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds >= 0 && seconds <= maxSeconds {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		duration := time.Until(when)
		if duration > 0 {
			return duration
		}
	}
	return 0
}
