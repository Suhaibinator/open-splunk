package agentkit

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// ErrorCode is a stable, model- and renderer-visible tool-error discriminant. It is
// a typed enum rather than a raw string: agentkit's own generic codes are the
// GenericErrorCode enum below, and applications define their domain codes as their
// own typed int32 enums that implement this interface (String() returns the stable
// wire code), following the repo's proto-compatible enum convention. The JSON wire
// contract (ToolErrorPayload.Code) serializes code.String(), so the open set of app
// codes still round-trips as a name while the Go API stays typed and exhaustively
// checkable. There is deliberately no string-adapter type: a value that is already a
// string at a boundary should be parsed back into one of these typed enums, not
// wrapped, so the code set never degrades to stringly-typed.
type ErrorCode interface {
	// String returns the stable wire code (e.g. "invalid_arguments").
	String() string
}

// GenericErrorCode enumerates the tool-error codes agentkit itself emits. Apps do
// not extend this enum; they declare their own ErrorCode enum for domain codes.
// The underlying type is int32 to match the repo's proto-compatible enum convention.
type GenericErrorCode int32

const (
	// GenericErrorCodeUnspecified is the zero value; it should not be emitted.
	GenericErrorCodeUnspecified GenericErrorCode = iota
	// GenericErrorCodeInvalidArguments marks a tool call whose arguments failed
	// strict decoding or app validation. It is model-visible so the model can
	// correct and retry.
	GenericErrorCodeInvalidArguments
	// GenericErrorCodeDomainError is the fallback code for an app domain error that
	// is model-visible (per the DomainVisible predicate) but not a *ToolError
	// carrying its own code.
	GenericErrorCodeDomainError
)

// String returns the stable wire code for a GenericErrorCode.
func (c GenericErrorCode) String() string {
	switch c {
	case GenericErrorCodeInvalidArguments:
		return "invalid_arguments"
	case GenericErrorCodeDomainError:
		return "domain_error"
	default:
		return "unspecified"
	}
}

// codeString renders an ErrorCode's wire value, treating a nil code as unspecified.
func codeString(code ErrorCode) string {
	if code == nil {
		return GenericErrorCodeUnspecified.String()
	}
	return code.String()
}

// ToolError is a model-visible tool error carrying a stable typed code. Executors
// return it (via NewToolError) when a tool cannot proceed for a reason the model
// should see and adapt to, rather than an operational failure the runtime must
// surface. ResultForError converts it to an in-band is_error tool result.
type ToolError struct {
	code    ErrorCode
	message string
}

// NewToolError builds a *ToolError with a typed code and a formatted message.
func NewToolError(code ErrorCode, format string, args ...any) *ToolError {
	return &ToolError{code: code, message: fmt.Sprintf(format, args...)}
}

// Error implements error.
func (e *ToolError) Error() string { return e.message }

// Code returns the typed code carried by the error.
func (e *ToolError) Code() ErrorCode { return e.code }

// ToolErrorPayload is the in-band, model- and renderer-visible shape for a tool
// domain error. The frontend transcript renderers read these fields directly, so
// the JSON tags are a UI contract: evolve additively only. Code is the wire form
// (the ErrorCode's String()), because the set of app codes is open.
type ToolErrorPayload struct {
	ToolName    string `json:"tool_name"`
	RendererKey string `json:"renderer_key"`
	Code        string `json:"code"`
	Message     string `json:"message"`
}

// SuccessResult marshals payload into a structured ContentJSON tool result. A
// marshal failure is operational (a bug in the payload type) and is returned as an
// error so the runtime surfaces it rather than reporting a corrupt result.
func SuccessResult(payload any) (*ports.AgentToolResult, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &ports.AgentToolResult{ContentJSON: raw}, nil
}

// ErrorResult builds an in-band is_error tool result carrying a ToolErrorPayload.
// It never fails: if the payload cannot be marshaled it falls back to a
// hand-built JSON object so the model still receives a coherent error.
func ErrorResult(toolName string, rendererKey string, code ErrorCode, message string) *ports.AgentToolResult {
	payload := ToolErrorPayload{
		ToolName:    toolName,
		RendererKey: rendererKey,
		Code:        codeString(code),
		Message:     message,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		fallback := ToolErrorPayload{
			ToolName:    toolName,
			RendererKey: rendererKey,
			Code:        "internal_error",
			Message:     "failed to marshal tool error",
		}
		if raw, err = json.Marshal(fallback); err != nil {
			raw = jsontext.Value(`{"code":"internal_error","message":"failed to marshal tool error"}`)
		}
	}
	return &ports.AgentToolResult{ContentJSON: raw, IsError: true}
}

// ResultForError maps a failure returned by a tool read to either an in-band tool
// error result (nil error) or an operational error the runtime must surface
// (non-nil error). The classification is:
//
//   - a *ToolError is always in-band, using its own typed code;
//   - any error for which domainVisible reports true is in-band under
//     GenericErrorCodeDomainError;
//   - everything else is operational and propagates unchanged.
//
// domainVisible is the app-provided predicate (typically an errors.As over the
// app's domain error types); a nil predicate treats every non-ToolError as
// operational. This is the dependency-inversion seam: agentkit never imports an
// app error package.
func ResultForError(toolName string, rendererKey string, err error, domainVisible func(error) bool) (*ports.AgentToolResult, error) {
	if toolErr, ok := errors.AsType[*ToolError](err); ok {
		return ErrorResult(toolName, rendererKey, toolErr.code, toolErr.message), nil
	}
	if domainVisible != nil && domainVisible(err) {
		return ErrorResult(toolName, rendererKey, GenericErrorCodeDomainError, err.Error()), nil
	}
	return nil, err
}
