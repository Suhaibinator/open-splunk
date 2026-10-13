package openairesponses

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/firebase/genkit/go/ai"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
)

func responseToGenkit(resp *responses.Response, req *ai.ModelRequest, projection *requestProjection) (*ai.ModelResponse, error) {
	if resp == nil {
		return nil, fmt.Errorf("openairesponses: OpenAI returned a nil response")
	}
	switch resp.Status {
	case responses.ResponseStatusFailed:
		return nil, responseFailureError(
			"openairesponses: response failed",
			string(resp.Error.Code),
			resp.Error.Message,
		)
	case responses.ResponseStatusCancelled:
		return nil, fmt.Errorf("openairesponses: response was cancelled")
	case responses.ResponseStatusQueued, responses.ResponseStatusInProgress:
		return nil, fmt.Errorf("openairesponses: response returned non-terminal status %q", resp.Status)
	case responses.ResponseStatusCompleted, responses.ResponseStatusIncomplete:
		// These are the only terminal statuses whose non-executable output can be
		// projected. Function calls are restricted to COMPLETED below.
	default:
		return nil, fmt.Errorf("openairesponses: response returned unsupported terminal status %q", resp.Status)
	}
	message := &ai.Message{Role: ai.RoleModel}
	rejected := false
	var refusalMessages []string
	seenOutputItemIDs := make(map[string]int, len(resp.Output))
	seenFunctionCallIDs := make(map[string]int)

	for i, item := range resp.Output {
		if item.ID != "" {
			if previous, exists := seenOutputItemIDs[item.ID]; exists {
				return nil, fmt.Errorf("openairesponses: duplicate output item id %q at output[%d] and output[%d]", item.ID, previous, i)
			}
			seenOutputItemIDs[item.ID] = i
		}
		switch item.Type {
		case "message":
			for j, content := range item.Content {
				switch content.Type {
				case "output_text":
					var part *ai.Part
					if projection != nil && projection.responseIsJSON {
						part = ai.NewJSONPart(content.Text)
					} else {
						part = ai.NewTextPart(content.Text)
					}
					part.Metadata = map[string]any{metadataItemID: item.ID}
					message.Content = append(message.Content, part)
				case "refusal":
					part := ai.NewTextPart(content.Refusal)
					part.Metadata = map[string]any{
						metadataItemID: item.ID,
						"refusal":      true,
					}
					message.Content = append(message.Content, part)
					refusalMessages = append(refusalMessages, content.Refusal)
				default:
					return nil, fmt.Errorf("openairesponses: unsupported output message content type %q at output[%d].content[%d]", content.Type, i, j)
				}
			}

		case "function_call":
			if resp.Status != responses.ResponseStatusCompleted {
				// Preserve the terminal finish reason and usage, but never expose
				// a tool request from an incomplete response for execution.
				continue
			}
			part, err := functionCallPart(item, projection)
			if err != nil {
				return nil, fmt.Errorf("openairesponses: function call output[%d]: %w", i, err)
			}
			if previous, exists := seenFunctionCallIDs[item.CallID]; exists {
				return nil, fmt.Errorf("openairesponses: duplicate function call_id %q at output[%d] and output[%d]", item.CallID, previous, i)
			}
			seenFunctionCallIDs[item.CallID] = i
			if rejection := toolrejection.FromPart(part); rejection != nil {
				rejection.OutputIndex = uint32(i)
				rejected = true
			}
			message.Content = append(message.Content, part)

		case "reasoning":
			added := false
			for _, summary := range item.Summary {
				part := newReasoningPart(summary.Text, []byte(item.EncryptedContent), item.ID)
				message.Content = append(message.Content, part)
				added = true
			}
			for _, content := range item.Content {
				if content.Type != "reasoning_text" {
					return nil, fmt.Errorf("openairesponses: unsupported reasoning content type %q at output[%d]", content.Type, i)
				}
				part := newReasoningPart(content.Text, []byte(item.EncryptedContent), item.ID)
				message.Content = append(message.Content, part)
				added = true
			}
			if !added {
				part := newReasoningPart("", []byte(item.EncryptedContent), item.ID)
				message.Content = append(message.Content, part)
			}

		case "image_generation_call":
			if projection == nil || projection.hostedImageContentType == "" {
				return nil, fmt.Errorf("openairesponses: unexpected image generation output[%d] without an enabled image_generation tool", i)
			}
			if item.Status != "completed" {
				return nil, fmt.Errorf("openairesponses: image generation output[%d] has incomplete status %q", i, item.Status)
			}
			if err := validateHostedImageResult(item.Result); err != nil {
				return nil, fmt.Errorf("openairesponses: image generation output[%d]: %w", i, err)
			}
			contentType := projection.hostedImageContentType
			contents := "data:" + contentType + ";base64," + item.Result
			part := ai.NewMediaPart(contentType, contents)
			part.Metadata = map[string]any{metadataItemID: item.ID}
			message.Content = append(message.Content, part)

		case "mcp_approval_request":
			return nil, fmt.Errorf("openairesponses: OpenAI returned MCP approval request at output[%d], but MCP approval round-trips are not implemented", i)

		default:
			if !isHostedDiagnosticOutput(item.Type) {
				return nil, fmt.Errorf("openairesponses: unsupported Responses output item type %q at output[%d]", item.Type, i)
			}
			// Provider-hosted tool diagnostics remain available in the exact output
			// replay metadata but have no portable Genkit content projection.
		}
	}

	rawOutput := resp.JSON.Output.Raw()
	if rawOutput == "" {
		b, err := json.Marshal(resp.Output)
		if err != nil {
			return nil, fmt.Errorf("openairesponses: encode response output for replay: %w", err)
		}
		rawOutput = string(b)
	}
	// Some Responses-compatible providers return null for a genuinely empty turn.
	// Persist an empty array so the continuation runner can perform its bounded retry.
	// Preserve non-empty arrays verbatim for exact provider replay.
	if strings.TrimSpace(rawOutput) == "null" && len(resp.Output) == 0 {
		rawOutput = "[]"
	}
	var outputItems []jsontext.Value
	if err := json.Unmarshal([]byte(rawOutput), &outputItems); err != nil || outputItems == nil {
		return nil, fmt.Errorf("openairesponses: response output must be a JSON array")
	}
	projectionHash, err := contentProjectionHash(message.Content)
	if err != nil {
		return nil, fmt.Errorf("openairesponses: hash response projection: %w", err)
	}
	message.Metadata = map[string]any{
		MetadataOutputItemsJSON: rawOutput,
		MetadataProjectionHash:  projectionHash,
		MetadataResponseID:      resp.ID,
		MetadataReplayVerified:  common_service_models.NewOpenAIResponsesReplayAttestation(),
	}

	if resp.Status != responses.ResponseStatusCompleted {
		// Partial provider output must not be replayed as a completed tool call.
		message.Metadata = map[string]any{MetadataResponseID: resp.ID}
	}

	finishReason, finishMessage := responseFinish(resp)
	if len(refusalMessages) != 0 {
		finishReason = ai.FinishReasonBlocked
		finishMessage = strings.Join(refusalMessages, "; ")
	}
	result := &ai.ModelResponse{
		Request:       req,
		Message:       message,
		FinishReason:  finishReason,
		FinishMessage: finishMessage,
		Usage: &ai.GenerationUsage{
			InputTokens:         int(resp.Usage.InputTokens),
			OutputTokens:        int(resp.Usage.OutputTokens),
			TotalTokens:         int(resp.Usage.TotalTokens),
			CachedContentTokens: int(resp.Usage.InputTokensDetails.CachedTokens),
			ThoughtsTokens:      int(resp.Usage.OutputTokensDetails.ReasoningTokens),
		},
		Raw: resp.RawJSON(),
	}
	if rejected {
		return nil, &toolrejection.Error{Response: result}
	}
	return result, nil
}

func newReasoningPart(text string, signature []byte, itemID string) *ai.Part {
	part := ai.NewReasoningPart(text, signature)
	if part.Metadata == nil {
		part.Metadata = make(map[string]any, 1)
	}
	part.Metadata[metadataItemID] = itemID
	return part
}

func functionCallPart(item responses.ResponseOutputItemUnion, projection *requestProjection) (*ai.Part, error) {
	if item.Status != "completed" {
		return nil, fmt.Errorf("function call %q has non-executable status %q", item.ID, item.Status)
	}
	if strings.TrimSpace(item.ID) == "" {
		return nil, fmt.Errorf("function call is missing output item id")
	}
	if strings.TrimSpace(item.CallID) == "" {
		return nil, fmt.Errorf("function call %q is missing call_id", item.ID)
	}
	genkitName, err := advertisedGenkitToolName(projection, item.Name)
	if err != nil {
		return nil, err
	}
	arguments := item.Arguments.OfString

	var input any
	if err := common_json.UnmarshalExact([]byte(arguments), &input); err != nil {
		diagnostic := err.Error()
		if strings.TrimSpace(arguments) == "" {
			diagnostic = "missing JSON-encoded arguments"
		}
		return toolrejection.Part(&proto_models.RejectedToolArguments{ToolName: genkitName, ProviderToolName: item.Name, CallId: item.CallID, ItemId: item.ID, RawArguments: arguments, Diagnostic: diagnostic}), nil
	}
	part := ai.NewToolRequestPart(&ai.ToolRequest{
		Name: genkitName, Input: input, Ref: item.CallID,
	})
	part.Metadata = map[string]any{metadataItemID: item.ID, metadataToolName: item.Name}
	return part, nil
}

func validateHostedImageResult(result string) error {
	if result == "" {
		return fmt.Errorf("result is empty")
	}
	if strings.ContainsAny(result, " \t\r\n") {
		return fmt.Errorf("result must be canonical bare base64 without whitespace")
	}
	decodedBytes, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(result)))
	if err != nil {
		return fmt.Errorf("result must be valid bare base64: %w", err)
	}
	if decodedBytes == 0 {
		return fmt.Errorf("result decodes to an empty image")
	}
	return nil
}

func advertisedGenkitToolName(projection *requestProjection, providerName string) (string, error) {
	if projection == nil {
		return "", fmt.Errorf("provider function %q was not advertised in this request", providerName)
	}
	genkitName := projection.providerToGenkit[providerName]
	if genkitName == "" {
		return "", fmt.Errorf("provider function %q was not advertised in this request", providerName)
	}
	return genkitName, nil
}

func isHostedDiagnosticOutput(itemType string) bool {
	switch itemType {
	case "file_search_call", "web_search_call", "code_interpreter_call",
		"mcp_call", "mcp_list_tools", "mcp_approval_response",
		"program", "program_output", "tool_search_call", "tool_search_output",
		"additional_tools", "compaction":
		return true
	default:
		return false
	}
}

func responseFinish(resp *responses.Response) (ai.FinishReason, string) {
	switch resp.Status {
	case responses.ResponseStatusCompleted:
		return ai.FinishReasonStop, ""
	case responses.ResponseStatusIncomplete:
		switch resp.IncompleteDetails.Reason {
		case "max_output_tokens":
			return ai.FinishReasonLength, resp.IncompleteDetails.Reason
		case "content_filter":
			return ai.FinishReasonBlocked, resp.IncompleteDetails.Reason
		default:
			return ai.FinishReasonOther, resp.IncompleteDetails.Reason
		}
	case responses.ResponseStatusFailed:
		return ai.FinishReasonOther, resp.Error.Message
	case responses.ResponseStatusCancelled:
		return ai.FinishReasonInterrupted, "response cancelled"
	case responses.ResponseStatusQueued, responses.ResponseStatusInProgress:
		return ai.FinishReasonUnknown, string(resp.Status)
	default:
		return ai.FinishReasonUnknown, string(resp.Status)
	}
}

func contentProjectionHash(content []*ai.Part) (string, error) {
	b, err := common_json.MarshalCanonical(content)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func replayOutput(message *ai.Message) (responses.ResponseInputParam, bool, error) {
	if message == nil || message.Metadata == nil {
		return nil, false, nil
	}
	raw := metadataString(message.Metadata, MetadataOutputItemsJSON)
	wantHash := metadataString(message.Metadata, MetadataProjectionHash)
	trusted := common_service_models.IsOpenAIResponsesReplayAttested(message.Metadata[MetadataReplayVerified])
	// Exact provider output may contain opaque reasoning and hosted-tool items that have no
	// complete, portable Genkit projection. A caller-computable content hash therefore cannot
	// attest that the raw array contains only the visible parts. Accept exact replay only when the
	// message carries the process-local capability minted by response conversion or by the durable
	// provider-state verifier. JSON/action callers cannot manufacture that pointer identity.
	if raw == "" || !trusted {
		return nil, false, nil
	}
	// Fresh in-memory responses also carry a local projection hash. Reject their replay if an
	// application edits the visible message while retaining its metadata. Durable reconstruction
	// intentionally omits this provider-local hash because it verifies its own canonical visible
	// projection before minting the capability.
	if wantHash != "" {
		gotHash, err := contentProjectionHash(message.Content)
		if err != nil {
			return nil, false, err
		}
		if gotHash != wantHash {
			return nil, false, nil
		}
	}

	var rawItems []jsontext.Value
	if err := json.Unmarshal([]byte(raw), &rawItems); err != nil {
		return nil, false, fmt.Errorf("decode %s: %w", MetadataOutputItemsJSON, err)
	}
	items := make(responses.ResponseInputParam, 0, len(rawItems))
	for i, rawItem := range rawItems {
		var header struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(rawItem, &header); err != nil {
			return nil, false, fmt.Errorf("decode replay output item %d: %w", i, err)
		}
		if header.Type == "" {
			return nil, false, fmt.Errorf("replay output item %d has no type", i)
		}
	}

	for _, rawItem := range rawItems {
		items = append(items, param.Override[responses.ResponseInputItemUnionParam](rawItem))
	}
	return items, true, nil
}
