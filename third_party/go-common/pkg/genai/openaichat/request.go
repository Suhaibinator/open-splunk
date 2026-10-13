package openaichat

import (
	json "encoding/json/v2"
	"fmt"
	"mime"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/genai/strictschema"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/shared"
)

// metadataReasoningContent is the assistant-message extra field OpenAI-compatible
// reasoning models (DeepSeek, Kimi, OpenRouter's normalization) expect their own
// reasoning replayed under alongside the tool calls it produced.
const metadataReasoningContent = "reasoning_content"

type requestProjection struct {
	params         openai.ChatCompletionNewParams
	responseIsJSON bool
}

func buildRequest(modelID string, req *ai.ModelRequest) (*requestProjection, error) {
	if req == nil {
		return nil, fmt.Errorf("openaichat: model request is nil")
	}
	if len(req.Docs) != 0 {
		return nil, fmt.Errorf("openaichat: request.docs is not supported; add document parts to request.messages")
	}

	params, err := ConfigFrom(req.Config)
	if err != nil {
		return nil, err
	}
	if err := rejectManagedConfig(&params); err != nil {
		return nil, err
	}
	// A config Model pins the exact version the request is served by; the
	// action's model fills in otherwise.
	if strings.TrimSpace(params.Model) == "" {
		params.Model = modelID
	}

	messages, err := messagesToParams(req.Messages)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("openaichat: no messages provided")
	}
	params.Messages = messages

	tools, err := toolsToParams(req.Tools)
	if err != nil {
		return nil, err
	}
	// Some compatible APIs reject an empty tools array, so it stays unset.
	if len(tools) != 0 {
		params.Tools = tools
	}
	if req.ToolChoice != "" {
		switch req.ToolChoice {
		case ai.ToolChoiceAuto, ai.ToolChoiceNone, ai.ToolChoiceRequired:
			params.ToolChoice.OfAuto = param.NewOpt(string(req.ToolChoice))
		default:
			return nil, fmt.Errorf("openaichat: unsupported tool choice %q", req.ToolChoice)
		}
	}

	responseIsJSON := false
	if req.Output != nil {
		responseIsJSON = req.Output.Format == "json" || req.Output.ContentType == "application/json" || req.Output.Schema != nil
		if responseIsJSON {
			if req.Output.Schema != nil {
				schema, err := prepareJSONSchema(req.Output.Schema, req.Output.Constrained)
				if err != nil {
					return nil, fmt.Errorf("openaichat: output schema: %w", err)
				}
				params.ResponseFormat.OfJSONSchema = &shared.ResponseFormatJSONSchemaParam{
					JSONSchema: shared.ResponseFormatJSONSchemaJSONSchemaParam{
						Name:   "output",
						Schema: schema,
						Strict: param.NewOpt(req.Output.Constrained),
					},
				}
			} else {
				params.ResponseFormat.OfJSONObject = &shared.ResponseFormatJSONObjectParam{}
			}
		}
		// The text format sends nothing: an explicit response_format of type text
		// only restates the default, and some compatible endpoints reject it.
	}

	return &requestProjection{params: params, responseIsJSON: responseIsJSON}, nil
}

func messagesToParams(messages []*ai.Message) ([]openai.ChatCompletionMessageParamUnion, error) {
	params := make([]openai.ChatCompletionMessageParamUnion, 0, len(messages))
	for i, message := range messages {
		if message == nil {
			return nil, fmt.Errorf("openaichat: message %d is nil", i)
		}
		converted, err := messageToParams(message)
		if err != nil {
			return nil, fmt.Errorf("openaichat: convert message %d (%s): %w", i, message.Role, err)
		}
		params = append(params, converted...)
	}
	return params, nil
}

func messageToParams(message *ai.Message) ([]openai.ChatCompletionMessageParamUnion, error) {
	switch message.Role {
	case ai.RoleSystem:
		var text strings.Builder
		for i, part := range message.Content {
			if part == nil || !part.IsText() {
				return nil, fmt.Errorf("system part %d must be text", i)
			}
			text.WriteString(part.Text)
		}
		return []openai.ChatCompletionMessageParamUnion{openai.SystemMessage(text.String())}, nil

	case ai.RoleUser:
		if len(message.Content) == 0 {
			return nil, fmt.Errorf("user message has no content")
		}
		parts := make([]openai.ChatCompletionContentPartUnionParam, 0, len(message.Content))
		for i, part := range message.Content {
			if part == nil {
				return nil, fmt.Errorf("part %d is nil", i)
			}
			switch part.Kind {
			case ai.PartText:
				parts = append(parts, openai.TextContentPart(part.Text))
			case ai.PartMedia:
				converted, err := mediaContentPart(part)
				if err != nil {
					return nil, fmt.Errorf("media part %d: %w", i, err)
				}
				parts = append(parts, converted)
			case ai.PartData:
				converted, err := dataContentPart(part)
				if err != nil {
					return nil, fmt.Errorf("data part %d: %w", i, err)
				}
				parts = append(parts, converted)
			default:
				return nil, fmt.Errorf("unsupported user part kind %d at index %d", part.Kind, i)
			}
		}
		return []openai.ChatCompletionMessageParamUnion{openai.UserMessage(parts)}, nil

	case ai.RoleModel:
		return assistantMessageParams(message)

	case ai.RoleTool:
		params := make([]openai.ChatCompletionMessageParamUnion, 0, len(message.Content))
		for i, part := range message.Content {
			if part == nil || part.Kind != ai.PartToolResponse || part.ToolResponse == nil {
				return nil, fmt.Errorf("tool message part %d must be a tool response", i)
			}
			converted, err := toolMessageParam(part.ToolResponse)
			if err != nil {
				return nil, fmt.Errorf("tool response part %d: %w", i, err)
			}
			params = append(params, converted)
		}
		return params, nil

	default:
		return nil, fmt.Errorf("unsupported role %q", message.Role)
	}
}

// assistantMessageParams replays a model turn: the visible text, the reasoning
// the provider returned under its non-standard field, and the tool calls. A
// rejected call is replayed with the exact argument text the model produced so
// the corrective tool result the runtime persisted still refers to it.
func assistantMessageParams(message *ai.Message) ([]openai.ChatCompletionMessageParamUnion, error) {
	var text, reasoning strings.Builder
	var toolCalls []openai.ChatCompletionMessageToolCallUnionParam
	for i, part := range message.Content {
		if part == nil {
			return nil, fmt.Errorf("part %d is nil", i)
		}
		if rejected := toolrejection.FromPart(part); rejected != nil {
			callID := rejected.GetCallId()
			if callID == "" {
				return nil, fmt.Errorf("rejected tool call part %d requires a call id", i)
			}
			toolCalls = append(toolCalls, functionToolCall(callID, rejected.GetProviderToolName(), rejected.GetRawArguments()))
			continue
		}
		switch part.Kind {
		case ai.PartText:
			text.WriteString(part.Text)
		case ai.PartReasoning:
			reasoning.WriteString(part.Text)
		case ai.PartToolRequest:
			if part.ToolRequest == nil {
				return nil, fmt.Errorf("tool request part %d is empty", i)
			}
			callID := part.ToolRequest.Ref
			if callID == "" {
				callID = part.ToolRequest.Name
			}
			arguments := ""
			if part.ToolRequest.Input != nil {
				encoded, err := json.Marshal(part.ToolRequest.Input)
				if err != nil {
					return nil, fmt.Errorf("marshal tool request %q input: %w", part.ToolRequest.Name, err)
				}
				arguments = string(encoded)
			}
			toolCalls = append(toolCalls, functionToolCall(callID, part.ToolRequest.Name, arguments))
		default:
			return nil, fmt.Errorf("unsupported model part kind %d at index %d", part.Kind, i)
		}
	}
	assistant := openai.ChatCompletionAssistantMessageParam{}
	assistant.Content.OfString = param.NewOpt(text.String())
	if reasoning.Len() != 0 {
		// SetExtraFields requires this map; it carries the provider's own field.
		assistant.SetExtraFields(map[string]any{metadataReasoningContent: reasoning.String()})
	}
	if len(toolCalls) != 0 {
		assistant.ToolCalls = toolCalls
	}
	return []openai.ChatCompletionMessageParamUnion{{OfAssistant: &assistant}}, nil
}

func functionToolCall(id, name, arguments string) openai.ChatCompletionMessageToolCallUnionParam {
	return openai.ChatCompletionMessageToolCallUnionParam{OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
		ID:       id,
		Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{Name: name, Arguments: arguments},
	}}
}

// toolMessageParam serializes a tool result. Chat Completions tool messages are
// text-only, so multipart media or data content fails closed instead of being
// dropped; the durable transcript folds text-only parts into Output already.
func toolMessageParam(tool *ai.ToolResponse) (openai.ChatCompletionMessageParamUnion, error) {
	callID := tool.Ref
	if callID == "" {
		callID = tool.Name
	}
	var text strings.Builder
	for i, part := range tool.Content {
		if part == nil || !part.IsText() {
			return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("multipart content %d is not text; Chat Completions tool messages carry text only", i)
		}
		text.WriteString(part.Text)
	}
	if tool.Output == nil && len(tool.Content) != 0 {
		return openai.ToolMessage(text.String(), callID), nil
	}
	output, err := outputText(tool.Output)
	if err != nil {
		return openai.ChatCompletionMessageParamUnion{}, err
	}
	return openai.ToolMessage(output, callID), nil
}

// outputText sends a string output verbatim and JSON-encodes structured values,
// including nil, which becomes the literal null the runtime's fallback replaces.
func outputText(output any) (string, error) { // any: Genkit's ai.ToolResponse.Output.
	if text, ok := output.(string); ok {
		return text, nil
	}
	b, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("marshal tool output: %w", err)
	}
	return string(b), nil
}

// mediaContentPart projects a media part. Images travel as image_url (data URI
// or absolute HTTP(S) URL); other inline media becomes a file part, and audio a
// native input_audio part. A non-image HTTP reference has no Chat Completions
// form and fails closed rather than being mislabeled as an image.
func mediaContentPart(part *ai.Part) (openai.ChatCompletionContentPartUnionParam, error) {
	source := strings.TrimSpace(part.Text)
	if source == "" {
		return openai.ChatCompletionContentPartUnionParam{}, fmt.Errorf("media source is empty")
	}
	contentType := canonicalContentType(part.ContentType)
	isDataURI := common_util.HasDataURIScheme(source)
	if contentType == "" && isDataURI {
		var err error
		if contentType, _, err = splitDataURI(source); err != nil {
			return openai.ChatCompletionContentPartUnionParam{}, err
		}
	}
	if strings.HasPrefix(contentType, "image/") || contentType == "" {
		if !isDataURI && !hasHTTPURLScheme(source) {
			return openai.ChatCompletionContentPartUnionParam{}, fmt.Errorf("image source must be a data URI or absolute HTTP(S) URL")
		}
		detail, err := imageDetail(part.Metadata)
		if err != nil {
			return openai.ChatCompletionContentPartUnionParam{}, err
		}
		return openai.ImageContentPart(openai.ChatCompletionContentPartImageImageURLParam{URL: source, Detail: detail}), nil
	}
	if !isDataURI {
		return openai.ChatCompletionContentPartUnionParam{}, fmt.Errorf("non-image media %q must be inline data; Chat Completions has no URL form for files", contentType)
	}
	if strings.HasPrefix(contentType, "audio/") {
		format, ok := audioFormats[contentType]
		if !ok {
			return openai.ChatCompletionContentPartUnionParam{}, fmt.Errorf("unsupported input_audio content type %q", contentType)
		}
		_, payload, err := splitDataURI(source)
		if err != nil {
			return openai.ChatCompletionContentPartUnionParam{}, err
		}
		return openai.InputAudioContentPart(openai.ChatCompletionContentPartInputAudioInputAudioParam{Data: payload, Format: format}), nil
	}
	return fileContentPart(source, contentType, part.Metadata), nil
}

// audioFormats maps the content types Chat Completions accepts as input_audio to
// the wire format names it expects.
var audioFormats = map[string]string{"audio/wav": "wav", "audio/x-wav": "wav", "audio/wave": "wav", "audio/mpeg": "mp3", "audio/mp3": "mp3"}

// dataContentPart projects Genkit's opaque data part as an inline file. The one
// encoded source form Genkit defines for data parts is a data: URI; any other
// payload is literal bytes and is encoded here, so an HTTP-looking string is
// never reinterpreted as a reference.
func dataContentPart(part *ai.Part) (openai.ChatCompletionContentPartUnionParam, error) {
	payload, err := partdata.String(part)
	if err != nil {
		return openai.ChatCompletionContentPartUnionParam{}, err
	}
	contentType := canonicalContentType(partdata.ContentType(part))
	source := strings.TrimSpace(payload)
	if !common_util.HasDataURIScheme(source) {
		if contentType == "" {
			contentType = "text/plain"
		}
		source = common_util.EncodeDataURI(contentType, []byte(payload))
	} else if contentType == "" {
		contentType, _, err = splitDataURI(source)
		if err != nil {
			return openai.ChatCompletionContentPartUnionParam{}, err
		}
	}
	return fileContentPart(source, contentType, part.Metadata), nil
}

func fileContentPart(dataURI, contentType string, metadata map[string]any) openai.ChatCompletionContentPartUnionParam { // any: Genkit's ai.Part.Metadata.
	filename := metadataString(metadata, "filename")
	if filename == "" {
		filename = defaultFilename(contentType)
	}
	return openai.FileContentPart(openai.ChatCompletionContentPartFileFileParam{
		FileData: param.NewOpt(dataURI),
		Filename: param.NewOpt(filename),
	})
}

// splitDataURI returns the media type and base64 payload of a data: URI.
func splitDataURI(value string) (mediaType, payload string, err error) {
	comma := strings.IndexByte(value, ',')
	if comma < 0 {
		return "", "", fmt.Errorf("data URI must contain a payload")
	}
	descriptor := value[len("data:"):comma]
	segments := strings.Split(descriptor, ";")
	base64Encoded := false
	for _, segment := range segments[1:] {
		if strings.EqualFold(segment, "base64") {
			base64Encoded = true
		}
	}
	if !base64Encoded {
		return "", "", fmt.Errorf("data URI must carry a base64 payload")
	}
	return canonicalContentType(segments[0]), value[comma+1:], nil
}

func hasHTTPURLScheme(value string) bool {
	return len(value) >= len("http://") && strings.EqualFold(value[:len("http://")], "http://") ||
		len(value) >= len("https://") && strings.EqualFold(value[:len("https://")], "https://")
}

func canonicalContentType(contentType string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		return strings.ToLower(strings.TrimSpace(contentType))
	}
	return strings.ToLower(parsed)
}

func defaultFilename(contentType string) string {
	if extension := map[string]string{
		"application/json": ".json",
		"application/pdf":  ".pdf",
		"text/plain":       ".txt",
	}[contentType]; extension != "" {
		return "input" + extension
	}
	extensions, _ := mime.ExtensionsByType(contentType)
	if len(extensions) != 0 {
		return "input" + extensions[0]
	}
	return "input.bin"
}

// imageDetail reads the image_url detail level from part metadata. The SDK
// models it as a plain string, so the accepted values are checked here.
func imageDetail(metadata map[string]any) (string, error) { // any: Genkit's ai.Part.Metadata.
	detail := metadataString(metadata, "detail")
	switch detail {
	case "", "auto", "low", "high":
		return detail, nil
	default:
		return "", fmt.Errorf("unsupported image detail %q", detail)
	}
}

func metadataString(metadata map[string]any, key string) string { // any: Genkit's ai.Part.Metadata.
	value, _ := metadata[key].(string)
	return value
}

func toolsToParams(defs []*ai.ToolDefinition) ([]openai.ChatCompletionToolUnionParam, error) {
	tools := make([]openai.ChatCompletionToolUnionParam, 0, len(defs))
	seen := make(map[string]struct{}, len(defs))
	for i, def := range defs {
		if def == nil || strings.TrimSpace(def.Name) == "" {
			return nil, fmt.Errorf("openaichat: tool definition %d requires a name", i)
		}
		if _, exists := seen[def.Name]; exists {
			return nil, fmt.Errorf("openaichat: duplicate tool definition %q", def.Name)
		}
		seen[def.Name] = struct{}{}
		// Strict mode is opt-in through Genkit's WithStrictSchema; the caller owns
		// OpenAI's other strict requirements (every property listed in required).
		strict, _ := def.Metadata["strict"].(bool)
		parameters, err := prepareJSONSchema(def.InputSchema, false)
		if err != nil {
			return nil, fmt.Errorf("openaichat: tool %q input schema: %w", def.Name, err)
		}
		if strict {
			enforceStrict(parameters)
		}
		function := shared.FunctionDefinitionParam{
			Name:       def.Name,
			Parameters: parameters,
			Strict:     param.NewOpt(strict),
		}
		if def.Description != "" {
			function.Description = param.NewOpt(def.Description)
		}
		tools = append(tools, openai.ChatCompletionFunctionTool(function))
	}
	return tools, nil
}

// prepareJSONSchema makes an independent JSON-safe copy of a schema and, in
// strict mode, applies the object constraints structured outputs require:
// closed objects whose every property is required.
func prepareJSONSchema(input map[string]any, strict bool) (map[string]any, error) { // any: Genkit's schema maps.
	if input == nil {
		input = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	b, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("schema is not JSON serializable: %w", err)
	}
	var output map[string]any
	if err := json.Unmarshal(b, &output); err != nil {
		return nil, fmt.Errorf("decode cloned schema: %w", err)
	}
	if output == nil {
		return nil, fmt.Errorf("schema must be a JSON object")
	}
	if strict {
		if len(output) == 0 {
			output["type"] = "object"
			output["properties"] = map[string]any{}
		}
		normalizeStrictSchema(output)
	}
	return output, nil
}

// enforceStrict closes every object subschema of a tool schema in place. It
// leaves required untouched: the tool author must list every property, and
// silently rewriting required would change which arguments the model may omit.
func enforceStrict(schema map[string]any) { // any: Genkit's schema maps.
	strictschema.Close(schema, false)
}

func normalizeStrictSchema(schema map[string]any) { // any: Genkit's schema maps.
	strictschema.Close(schema, true)
}
