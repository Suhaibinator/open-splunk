package openairesponses

import (
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"io"
	"mime"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	"github.com/Suhaibinator/go-common/pkg/genai/openaiinput"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/genai/strictschema"
	"github.com/firebase/genkit/go/ai"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
)

var responseNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

const maxResponsesFileInputBytes uint64 = openaiinput.MaxFileInputBytes
const maxResponsesImageInputBytes uint64 = openaiinput.MaxImageInputPayloadBytes
const maxResponsesImageInputs uint64 = openaiinput.MaxImageInputsPerRequest

// Data URI descriptors only carry a media type plus a small set of parameters.
// Bounding them prevents a malformed inline source from making strings.Split
// allocate in proportion to untrusted input before the payload budget is checked.
const maxDataURIMediaDescriptorBytes = 1024

type fileInputBudget struct {
	totalBytes uint64
	imageBytes uint64
	imageCount uint64
}

func (b *fileInputBudget) add(size uint64) error {
	if size >= maxResponsesFileInputBytes {
		return fmt.Errorf("file input is %d bytes; each file must be under 50 MB", size)
	}
	if b == nil || size == 0 {
		return nil
	}
	if b.totalBytes > maxResponsesFileInputBytes || size > maxResponsesFileInputBytes-b.totalBytes {
		return fmt.Errorf("combined file inputs exceed the 50 MB Responses API limit")
	}
	b.totalBytes += size
	return nil
}

func (b *fileInputBudget) addImage(size uint64) error {
	if b == nil {
		return nil
	}
	if b.imageCount >= maxResponsesImageInputs {
		return fmt.Errorf("image inputs exceed the Responses API limit of 1500 images per request")
	}
	if b.imageBytes > maxResponsesImageInputBytes || size > maxResponsesImageInputBytes-b.imageBytes {
		return fmt.Errorf("combined image inputs exceed the 512 MB Responses API payload limit")
	}
	b.imageCount++
	b.imageBytes += size
	return nil
}

func (b *fileInputBudget) remainingInlineFileBytes() uint64 {
	// OpenAI requires every individual file to be strictly under 50 MB.
	remaining := maxResponsesFileInputBytes - 1
	if b == nil {
		return remaining
	}
	if b.totalBytes >= maxResponsesFileInputBytes {
		return 0
	}
	if aggregateRemaining := maxResponsesFileInputBytes - b.totalBytes; aggregateRemaining < remaining {
		remaining = aggregateRemaining
	}
	return remaining
}

func (b *fileInputBudget) remainingInlineImageBytes() uint64 {
	if b == nil {
		return maxResponsesImageInputBytes
	}
	if b.imageCount >= maxResponsesImageInputs || b.imageBytes >= maxResponsesImageInputBytes {
		return 0
	}
	return maxResponsesImageInputBytes - b.imageBytes
}

// metadataToolResponseOutputSynthesized is stamped by durable transcript reconstruction when
// Output merely mirrors multipart text for adapters that cannot consume Content. Responses is
// multipart-aware and must skip that compatibility copy to avoid sending text twice.
const metadataToolResponseOutputSynthesized = "go-common.tool_response_output_synthesized"

type requestProjection struct {
	params                 responses.ResponseNewParams
	requestOptions         []option.RequestOption
	providerToGenkit       map[string]string
	responseIsJSON         bool
	hostedImageContentType string
}

type requestPolicy struct {
	allowHostedImageGeneration bool
	allowAtLeastOnceHostedMCP  bool
}

func buildRequest(modelID string, req *ai.ModelRequest) (*requestProjection, error) {
	return buildRequestWithPolicy(modelID, req, requestPolicy{})
}

func buildRequestWithPolicy(modelID string, req *ai.ModelRequest, policy requestPolicy) (*requestProjection, error) {
	if req == nil {
		return nil, fmt.Errorf("openairesponses: model request is nil")
	}
	if len(req.Docs) != 0 {
		return nil, fmt.Errorf("openairesponses: request.docs is not supported; add document parts to request.messages")
	}

	cfg, err := ConfigFrom(req.Config)
	if err != nil {
		return nil, err
	}
	params, err := modelconfig.NormalizeResponsesParams(cfg.Params)
	if err != nil {
		return nil, fmt.Errorf("openairesponses: %w", err)
	}
	params.Include = append([]responses.ResponseIncludable(nil), params.Include...)
	nativeTools := append([]responses.ToolUnionParam(nil), params.Tools...)
	// Genkit owns request semantics that must stay aligned with the transcript and
	// tool loop. Native config may tune provider-specific behavior, but it cannot
	// smuggle in a different tool-choice or structured-output contract.
	params.ToolChoice = responses.ResponseNewParamsToolChoiceUnion{}
	params.Text.Format = responses.ResponseFormatTextConfigUnionParam{}
	if err := validateNativeHostedTools(nativeTools, policy); err != nil {
		return nil, err
	}
	hostedImageContentType, err := nativeImageContentType(nativeTools)
	if err != nil {
		return nil, err
	}

	if params.Background.Valid() && params.Background.Value {
		return nil, fmt.Errorf("openairesponses: background responses are not supported")
	}
	if params.PreviousResponseID.Valid() {
		return nil, fmt.Errorf("openairesponses: previous_response_id is stateful and is not supported")
	}
	if !param.IsOmitted(params.Conversation) {
		return nil, fmt.Errorf("openairesponses: conversation is stateful and is not supported")
	}
	if params.Store.Valid() && params.Store.Value {
		return nil, fmt.Errorf("openairesponses: store=true is not supported; the plugin is stateless")
	}
	params.Store = param.NewOpt(false)
	if cfg.RequestReasoningTraces != nil {
		if *cfg.RequestReasoningTraces {
			if params.Reasoning.Summary == "" {
				params.Reasoning.Summary = shared.ReasoningSummaryAuto
			}
		} else {
			// The plugin-level switch owns summary projection when explicitly set.
			// Clear a native value instead of allowing Params to override false.
			params.Reasoning.Summary = ""
		}
	}
	if !containsInclude(params.Include, responses.ResponseIncludableReasoningEncryptedContent) {
		params.Include = append(params.Include, responses.ResponseIncludableReasoningEncryptedContent)
	}

	actualModel := modelID
	var requestOpts []option.RequestOption
	if common := cfg.Common; common != nil {
		if len(common.StopSequences) != 0 {
			return nil, fmt.Errorf("openairesponses: common.stopSequences is not supported by the Responses API")
		}
		if common.TopK != 0 {
			return nil, fmt.Errorf("openairesponses: common.topK is not supported by the Responses API")
		}
		if common.MaxOutputTokens > 0 {
			params.MaxOutputTokens = param.NewOpt(int64(common.MaxOutputTokens))
		}
		if common.Temperature != 0 {
			params.Temperature = param.NewOpt(common.Temperature)
		}
		if common.TopP != 0 {
			params.TopP = param.NewOpt(common.TopP)
		}
		if strings.TrimSpace(common.Version) != "" {
			actualModel = strings.TrimSpace(common.Version)
		}
		if strings.TrimSpace(common.APIKey) != "" {
			requestOpts = append(requestOpts, option.WithAPIKey(common.APIKey))
		}
	}
	params.Model = shared.ResponsesModel(actualModel)

	input, err := messagesToInput(req.Messages)
	if err != nil {
		return nil, err
	}
	params.Input = responses.ResponseNewParamsInputUnion{OfInputItemList: input}

	tools, providerToGenkit, err := toolsToParams(req.Tools, cfg.ClientTools)
	if err != nil {
		return nil, err
	}
	params.Tools = append(nativeTools, tools...)
	if req.ToolChoice != "" {
		mode, ok := map[ai.ToolChoice]responses.ToolChoiceOptions{
			ai.ToolChoiceAuto:     responses.ToolChoiceOptionsAuto,
			ai.ToolChoiceRequired: responses.ToolChoiceOptionsRequired,
			ai.ToolChoiceNone:     responses.ToolChoiceOptionsNone,
		}[req.ToolChoice]
		if !ok {
			return nil, fmt.Errorf("openairesponses: unsupported tool choice %q", req.ToolChoice)
		}
		params.ToolChoice = responses.ResponseNewParamsToolChoiceUnion{OfToolChoiceMode: param.NewOpt(mode)}
	}

	responseIsJSON := false
	if req.Output != nil {
		responseIsJSON = req.Output.Format == "json" || req.Output.ContentType == "application/json" || req.Output.Schema != nil
		if responseIsJSON {
			if req.Output.Schema != nil {
				schema, err := prepareJSONSchema(req.Output.Schema, req.Output.Constrained)
				if err != nil {
					return nil, fmt.Errorf("openairesponses: output schema: %w", err)
				}
				format := responses.ResponseFormatTextConfigParamOfJSONSchema("genkit_output", schema)
				format.OfJSONSchema.Strict = param.NewOpt(req.Output.Constrained)
				params.Text.Format = format
			} else {
				params.Text.Format = responses.ResponseFormatTextConfigUnionParam{
					OfJSONObject: &shared.ResponseFormatJSONObjectParam{},
				}
			}
		}
	}

	return &requestProjection{
		params:                 params,
		requestOptions:         requestOpts,
		providerToGenkit:       providerToGenkit,
		responseIsJSON:         responseIsJSON,
		hostedImageContentType: hostedImageContentType,
	}, nil
}

func validateNativeHostedTools(tools []responses.ToolUnionParam, policy requestPolicy) error {
	for i, tool := range tools {
		switch {
		case tool.OfFileSearch != nil,
			tool.OfWebSearch != nil,
			tool.OfCodeInterpreter != nil,
			tool.OfWebSearchPreview != nil:
			// These tools execute on the provider side and need no Genkit callback.
		case tool.OfProgrammaticToolCalling != nil:
			return fmt.Errorf("openairesponses: params.tools[%d] uses programmatic tool calling; nested caller round-trips are not implemented", i)
		case tool.OfToolSearch != nil:
			if tool.OfToolSearch.Execution == responses.ToolSearchToolExecutionClient {
				return fmt.Errorf("openairesponses: params.tools[%d] is a client-executed tool_search tool; client tool-search round-trips are not implemented", i)
			}
		case tool.OfNamespace != nil:
			return fmt.Errorf("openairesponses: params.tools[%d] is a namespace of client-side function/custom tools; expose executable tools through request.tools and Config.ClientTools", i)
		case tool.OfMcp != nil:
			if !policy.allowAtLeastOnceHostedMCP {
				return fmt.Errorf("openairesponses: params.tools[%d] is an MCP tool; provider-side MCP execution is not durably fenced and may repeat after retries; enable PluginOptions.AllowAtLeastOnceHostedMCPExecution only when every MCP operation tolerates at-least-once execution", i)
			}
			approval := tool.OfMcp.RequireApproval
			if approval.OfMcpToolApprovalFilter != nil ||
				!approval.OfMcpToolApprovalSetting.Valid() ||
				approval.OfMcpToolApprovalSetting.Value != string(responses.ToolMcpRequireApprovalMcpToolApprovalSettingNever) {
				return fmt.Errorf("openairesponses: params.tools[%d] is an MCP tool that may require approval; only require_approval=\"never\" is supported because MCP approval round-trips are not implemented", i)
			}
		case tool.OfImageGeneration != nil:
			if !policy.allowHostedImageGeneration {
				return fmt.Errorf("openairesponses: params.tools[%d] is an image_generation tool; enable PluginOptions.AllowHostedImageGeneration only when the caller can persist hosted image output", i)
			}
		case tool.OfFunction != nil:
			return fmt.Errorf("openairesponses: params.tools[%d] is a function tool; expose it through request.tools and Config.ClientTools so Genkit can execute it", i)
		default:
			return fmt.Errorf("openairesponses: params.tools[%d] is a client-side native tool variant that this plugin cannot execute", i)
		}
	}
	return nil
}

func nativeImageContentType(tools []responses.ToolUnionParam) (string, error) {
	contentType := ""
	for i, tool := range tools {
		if tool.OfImageGeneration == nil {
			continue
		}
		candidate := "image/png"
		switch tool.OfImageGeneration.OutputFormat {
		case "", "png":
		case "jpeg":
			candidate = "image/jpeg"
		case "webp":
			candidate = "image/webp"
		default:
			return "", fmt.Errorf("openairesponses: params.tools[%d] has unsupported image output_format %q", i, tool.OfImageGeneration.OutputFormat)
		}
		if contentType != "" && contentType != candidate {
			return "", fmt.Errorf("openairesponses: image_generation tools must use one output_format so response media can be typed unambiguously")
		}
		contentType = candidate
	}
	return contentType, nil
}

func containsInclude(includes []responses.ResponseIncludable, target responses.ResponseIncludable) bool {
	return slices.Contains(includes, target)
}

func messagesToInput(messages []*ai.Message) (responses.ResponseInputParam, error) {
	return messagesToInputWithFileBudget(messages, &fileInputBudget{})
}

func messagesToInputWithFileBudget(messages []*ai.Message, budget *fileInputBudget) (responses.ResponseInputParam, error) {
	var input responses.ResponseInputParam
	for i, message := range messages {
		if message == nil {
			return nil, fmt.Errorf("openairesponses: message %d is nil", i)
		}
		if message.Role == ai.RoleModel {
			if replay, ok, err := replayOutput(message); err != nil {
				return nil, fmt.Errorf("openairesponses: replay model message %d: %w", i, err)
			} else if ok {
				input = append(input, replay...)
				continue
			}
		}

		for _, part := range message.Content {
			if toolrejection.FromPart(part) != nil {
				return nil, fmt.Errorf("openairesponses: rejected call requires verified exact replay")
			}
		}
		items, err := messageToInputWithFileBudget(message, budget)
		if err != nil {
			return nil, fmt.Errorf("openairesponses: convert message %d (%s): %w", i, message.Role, err)
		}
		input = append(input, items...)
	}
	return input, nil
}

func messageToInput(message *ai.Message) ([]responses.ResponseInputItemUnionParam, error) {
	return messageToInputWithFileBudget(message, &fileInputBudget{})
}

func messageToInputWithFileBudget(message *ai.Message, budget *fileInputBudget) ([]responses.ResponseInputItemUnionParam, error) {
	switch message.Role {
	case ai.RoleSystem, ai.RoleUser:
		content, err := inputContentWithFileBudget(message.Content, budget)
		if err != nil {
			return nil, err
		}
		role := responses.EasyInputMessageRoleUser
		if message.Role == ai.RoleSystem {
			role = responses.EasyInputMessageRoleSystem
		}
		return []responses.ResponseInputItemUnionParam{responses.ResponseInputItemParamOfMessage(content, role)}, nil

	case ai.RoleModel:
		var items []responses.ResponseInputItemUnionParam
		var textContent responses.ResponseInputMessageContentListParam
		flushText := func() {
			if len(textContent) != 0 {
				items = append(items, responses.ResponseInputItemParamOfMessage(textContent, responses.EasyInputMessageRoleAssistant))
				textContent = nil
			}
		}
		for i, part := range message.Content {
			if part == nil {
				return nil, fmt.Errorf("part %d is nil", i)
			}
			switch part.Kind {
			case ai.PartText:
				textContent = append(textContent, responses.ResponseInputContentParamOfInputText(part.Text))
			case ai.PartToolRequest:
				flushText()
				if part.ToolRequest == nil || strings.TrimSpace(part.ToolRequest.Ref) == "" {
					return nil, fmt.Errorf("tool request part %d requires a call Ref", i)
				}
				arguments, err := json.Marshal(part.ToolRequest.Input)
				if err != nil {
					return nil, fmt.Errorf("marshal tool request %q input: %w", part.ToolRequest.Name, err)
				}
				providerName := metadataString(part.Metadata, metadataToolName)
				if providerName == "" {
					providerName = part.ToolRequest.Name
				}
				items = append(items, responses.ResponseInputItemParamOfFunctionCall(string(arguments), part.ToolRequest.Ref, providerName))
			case ai.PartReasoning:
				return nil, fmt.Errorf("reasoning part %d requires intact %q replay metadata", i, MetadataOutputJSON)
			default:
				return nil, fmt.Errorf("unsupported model part kind %d at index %d", part.Kind, i)
			}
		}
		flushText()
		return items, nil

	case ai.RoleTool:
		items := make([]responses.ResponseInputItemUnionParam, 0, len(message.Content))
		for i, part := range message.Content {
			if part == nil || part.Kind != ai.PartToolResponse || part.ToolResponse == nil {
				return nil, fmt.Errorf("tool message part %d must be a tool response", i)
			}
			if strings.TrimSpace(part.ToolResponse.Ref) == "" {
				return nil, fmt.Errorf("tool response part %d requires a call Ref", i)
			}
			item, err := toolResponseToInputWithFileBudget(part.ToolResponse, budget, metadataBool(part.Metadata, metadataToolResponseOutputSynthesized))
			if err != nil {
				return nil, fmt.Errorf("tool response part %d: %w", i, err)
			}
			items = append(items, item)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported role %q", message.Role)
	}
}

func inputContent(parts []*ai.Part) (responses.ResponseInputMessageContentListParam, error) {
	return inputContentWithFileBudget(parts, &fileInputBudget{})
}

func inputContentWithFileBudget(parts []*ai.Part, budget *fileInputBudget) (responses.ResponseInputMessageContentListParam, error) {
	content := make(responses.ResponseInputMessageContentListParam, 0, len(parts))
	for i, part := range parts {
		if part == nil {
			return nil, fmt.Errorf("part %d is nil", i)
		}
		switch part.Kind {
		case ai.PartText:
			content = append(content, responses.ResponseInputContentParamOfInputText(part.Text))
		case ai.PartMedia:
			media, err := mediaInputPartWithFileBudget(part, budget)
			if err != nil {
				return nil, fmt.Errorf("media part %d: %w", i, err)
			}
			content = append(content, media)
		case ai.PartData:
			data, err := dataInputPartWithFileBudget(part, budget)
			if err != nil {
				return nil, fmt.Errorf("data part %d: %w", i, err)
			}
			content = append(content, data)
		default:
			return nil, fmt.Errorf("unsupported input part kind %d at index %d", part.Kind, i)
		}
	}
	return content, nil
}

func mediaInputPartWithFileBudget(part *ai.Part, budget *fileInputBudget) (responses.ResponseInputContentUnionParam, error) {
	contentType := strings.ToLower(strings.TrimSpace(part.ContentType))
	if strings.HasPrefix(contentType, "image/") {
		if err := validateInputImageType(contentType); err != nil {
			return responses.ResponseInputContentUnionParam{}, err
		}
		detail, err := imageDetail(part.Metadata)
		if err != nil {
			return responses.ResponseInputContentUnionParam{}, err
		}
		image := &responses.ResponseInputImageParam{Detail: detail}
		size, err := setImageSource(part.Text, part.Metadata, contentType, budget.remainingInlineImageBytes(), &image.FileID, &image.ImageURL)
		if err != nil {
			return responses.ResponseInputContentUnionParam{}, err
		}
		if err := budget.addImage(size); err != nil {
			return responses.ResponseInputContentUnionParam{}, err
		}
		return responses.ResponseInputContentUnionParam{OfInputImage: image}, nil
	}
	file := &responses.ResponseInputFileParam{}
	size, err := setFileSource(part.Text, part.Metadata, contentType, budget.remainingInlineFileBytes(), &file.FileID, &file.FileURL, &file.FileData, &file.Filename)
	if err != nil {
		return responses.ResponseInputContentUnionParam{}, err
	}
	if err := budget.add(size); err != nil {
		return responses.ResponseInputContentUnionParam{}, err
	}
	if !file.FileID.Valid() && !file.FileURL.Valid() && !file.FileData.Valid() {
		return responses.ResponseInputContentUnionParam{}, fmt.Errorf("file URL, data, or ID is empty")
	}
	detail, err := inputFileDetail(part.Metadata, contentType, file.Filename.Value)
	if err != nil {
		return responses.ResponseInputContentUnionParam{}, err
	}
	file.Detail = detail
	return responses.ResponseInputContentUnionParam{OfInputFile: file}, nil
}

func toolResponseToInput(tool *ai.ToolResponse, outputSynthesized ...bool) (responses.ResponseInputItemUnionParam, error) {
	return toolResponseToInputWithFileBudget(tool, &fileInputBudget{}, outputSynthesized...)
}

func toolResponseToInputWithFileBudget(tool *ai.ToolResponse, budget *fileInputBudget, outputSynthesized ...bool) (responses.ResponseInputItemUnionParam, error) {
	skipOutput := len(outputSynthesized) > 0 && outputSynthesized[0]
	if len(tool.Content) == 0 {
		text, err := outputText(tool.Output)
		if err != nil {
			return responses.ResponseInputItemUnionParam{}, err
		}
		item := responses.ResponseInputItemParamOfFunctionCallOutput(text)
		item.OfFunctionCallOutput.CallID = param.NewOpt(tool.Ref)
		return item, nil
	}

	var output responses.ResponseFunctionCallOutputItemListParam
	if tool.Output != nil && !skipOutput {
		text, err := outputText(tool.Output)
		if err != nil {
			return responses.ResponseInputItemUnionParam{}, err
		}
		output = append(output, responses.ResponseFunctionCallOutputItemParamOfInputText(text))
	}
	for i, part := range tool.Content {
		if part == nil {
			return responses.ResponseInputItemUnionParam{}, fmt.Errorf("multipart content %d is nil", i)
		}
		switch part.Kind {
		case ai.PartText:
			output = append(output, responses.ResponseFunctionCallOutputItemParamOfInputText(part.Text))
		case ai.PartMedia:
			item, err := mediaToolOutputPartWithFileBudget(part, budget)
			if err != nil {
				return responses.ResponseInputItemUnionParam{}, fmt.Errorf("multipart media %d: %w", i, err)
			}
			output = append(output, item)
		case ai.PartData:
			item, err := dataToolOutputPartWithFileBudget(part, budget)
			if err != nil {
				return responses.ResponseInputItemUnionParam{}, fmt.Errorf("multipart data %d: %w", i, err)
			}
			output = append(output, item)
		default:
			return responses.ResponseInputItemUnionParam{}, fmt.Errorf("unsupported multipart part kind %d at index %d", part.Kind, i)
		}
	}
	if len(output) == 0 {
		return responses.ResponseInputItemUnionParam{}, fmt.Errorf("multipart function_call_output cannot be empty")
	}
	item := responses.ResponseInputItemParamOfFunctionCallOutput(output)
	item.OfFunctionCallOutput.CallID = param.NewOpt(tool.Ref)
	return item, nil
}

func mediaToolOutputPart(part *ai.Part) (responses.ResponseFunctionCallOutputItemUnionParam, error) {
	return mediaToolOutputPartWithFileBudget(part, &fileInputBudget{})
}

func mediaToolOutputPartWithFileBudget(part *ai.Part, budget *fileInputBudget) (responses.ResponseFunctionCallOutputItemUnionParam, error) {
	contentType := strings.ToLower(strings.TrimSpace(part.ContentType))
	if strings.HasPrefix(contentType, "image/") {
		if err := validateInputImageType(contentType); err != nil {
			return responses.ResponseFunctionCallOutputItemUnionParam{}, err
		}
		detail, err := imageContentDetail(part.Metadata)
		if err != nil {
			return responses.ResponseFunctionCallOutputItemUnionParam{}, err
		}
		image := &responses.ResponseInputImageContentParam{Detail: detail}
		size, err := setImageSource(part.Text, part.Metadata, contentType, budget.remainingInlineImageBytes(), &image.FileID, &image.ImageURL)
		if err != nil {
			return responses.ResponseFunctionCallOutputItemUnionParam{}, err
		}
		if err := budget.addImage(size); err != nil {
			return responses.ResponseFunctionCallOutputItemUnionParam{}, err
		}
		return responses.ResponseFunctionCallOutputItemUnionParam{OfInputImage: image}, nil
	}
	file := &responses.ResponseInputFileContentParam{}
	size, err := setFileSource(part.Text, part.Metadata, contentType, budget.remainingInlineFileBytes(), &file.FileID, &file.FileURL, &file.FileData, &file.Filename)
	if err != nil {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, err
	}
	if err := budget.add(size); err != nil {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, err
	}
	if !file.FileID.Valid() && !file.FileURL.Valid() && !file.FileData.Valid() {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, fmt.Errorf("file URL, data, or ID is empty")
	}
	detail, err := inputFileContentDetail(part.Metadata, contentType, file.Filename.Value)
	if err != nil {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, err
	}
	file.Detail = detail
	return responses.ResponseFunctionCallOutputItemUnionParam{OfInputFile: file}, nil
}

type dataFileProjection struct {
	contentType string
	filename    string
	fileData    string
}

func dataInputPartWithFileBudget(part *ai.Part, budget *fileInputBudget) (responses.ResponseInputContentUnionParam, error) {
	projected, err := projectDataFileWithBudget(part, budget)
	if err != nil {
		return responses.ResponseInputContentUnionParam{}, err
	}
	detail, err := inputFileDetail(part.Metadata, projected.contentType, projected.filename)
	if err != nil {
		return responses.ResponseInputContentUnionParam{}, err
	}
	return responses.ResponseInputContentUnionParam{OfInputFile: &responses.ResponseInputFileParam{
		FileData: param.NewOpt(projected.fileData),
		Filename: param.NewOpt(projected.filename),
		Detail:   detail,
	}}, nil
}

func dataToolOutputPartWithFileBudget(part *ai.Part, budget *fileInputBudget) (responses.ResponseFunctionCallOutputItemUnionParam, error) {
	projected, err := projectDataFileWithBudget(part, budget)
	if err != nil {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, err
	}
	detail, err := inputFileContentDetail(part.Metadata, projected.contentType, projected.filename)
	if err != nil {
		return responses.ResponseFunctionCallOutputItemUnionParam{}, err
	}
	return responses.ResponseFunctionCallOutputItemUnionParam{OfInputFile: &responses.ResponseInputFileContentParam{
		FileData: param.NewOpt(projected.fileData),
		Filename: param.NewOpt(projected.filename),
		Detail:   detail,
	}}, nil
}

// projectDataFileWithBudget converts Genkit's PartData representation into a
// Responses input_file. A data part is an opaque payload, not a media reference:
// HTTP-looking strings therefore remain literal bytes. The one encoded source
// form Genkit defines for data parts is a data: URI; decode and canonicalize it
// so it is neither double-encoded nor charged by its encoded length.
func projectDataFileWithBudget(part *ai.Part, budget *fileInputBudget) (dataFileProjection, error) {
	if part == nil || !part.IsData() {
		return dataFileProjection{}, fmt.Errorf("expected a Genkit data part")
	}
	payload, err := partdata.String(part)
	if err != nil {
		return dataFileProjection{}, err
	}

	contentType := partdata.ContentType(part)
	trimmedPayload := strings.TrimSpace(payload)

	encoded, mediaType, size, isDataURI, err := parseFileDataURI(trimmedPayload, budget.remainingInlineFileBytes())
	if err != nil {
		return dataFileProjection{}, err
	}
	if isDataURI {
		if contentType != "" {
			declaredType := canonicalContentType(contentType)
			if declaredType == "" {
				return dataFileProjection{}, fmt.Errorf("malformed part content type %q", contentType)
			}
			if !strings.EqualFold(declaredType, mediaType) {
				return dataFileProjection{}, fmt.Errorf("data URI media type %q does not match part content type %q", mediaType, declaredType)
			}
		}
		contentType = mediaType
	} else {
		if payload == "" {
			return dataFileProjection{}, fmt.Errorf("data payload is empty")
		}
		size = uint64(len(payload))
	}

	filename, prefix, err := finalizeInlineFile(contentType, metadataString(part.Metadata, "filename"), part.Metadata, size)
	if err != nil {
		return dataFileProjection{}, err
	}
	if err := budget.add(size); err != nil {
		return dataFileProjection{}, err
	}
	if !isDataURI {
		// Enforce both individual and aggregate limits before allocating the
		// larger base64 representation.
		encoded = base64.StdEncoding.EncodeToString([]byte(payload))
	}
	return dataFileProjection{
		contentType: canonicalContentType(contentType),
		filename:    filename,
		fileData:    prefix + encoded,
	}, nil
}

// finalizeInlineFile completes an inline file_data source shared by media and data parts: the
// default filename for the content type, input_file type validation, the size_bytes metadata
// cross-check, and the canonical data: prefix the caller appends its base64 payload to.
func finalizeInlineFile(contentType, filename string, metadata map[string]any, decodedSize uint64) (name, prefix string, err error) {
	if filename == "" {
		filename = defaultFilename(canonicalContentType(contentType))
	}
	if err := validateInputFileType(contentType, filename); err != nil {
		return "", "", err
	}
	if metadataSize, present, err := metadataSizeBytes(metadata); err != nil {
		return "", "", err
	} else if present && metadataSize != decodedSize {
		return "", "", fmt.Errorf("file size_bytes metadata %d does not match decoded payload size %d", metadataSize, decodedSize)
	}
	prefix, err = inlineFileDataURI(contentType, filename, nil)
	if err != nil {
		return "", "", err
	}
	return filename, prefix, nil
}

func setFileSource(text string, metadata map[string]any, contentType string, maxInlineBytes uint64, fileID, fileURL, fileData, filename *param.Opt[string]) (uint64, error) {
	text = strings.TrimSpace(text)
	effectiveContentType := strings.TrimSpace(contentType)
	var decodedSize uint64
	var encodedData string
	if effectiveContentType != "" {
		parsedType, _, err := mime.ParseMediaType(effectiveContentType)
		if err != nil {
			return 0, fmt.Errorf("malformed part content type %q: %w", effectiveContentType, err)
		}
		if parsedType == "" {
			return 0, fmt.Errorf("malformed part content type %q", effectiveContentType)
		}
		effectiveContentType = strings.ToLower(parsedType)
	}
	if validProviderFileID(text) {
		*fileID = param.NewOpt(text)
	} else if strings.HasPrefix(text, "file-") {
		return 0, fmt.Errorf("file ID is malformed")
	} else if encoded, mediaType, size, isDataURI, err := parseFileDataURI(text, maxInlineBytes); isDataURI {
		if err != nil {
			return 0, err
		}
		if effectiveContentType != "" && !strings.EqualFold(effectiveContentType, mediaType) {
			return 0, fmt.Errorf("data URI media type %q does not match part content type %q", mediaType, effectiveContentType)
		}
		effectiveContentType = mediaType
		decodedSize = size
		encodedData = encoded
	} else if hasHTTPURLScheme(text) {
		parsed, err := url.Parse(text)
		if err != nil || parsed.Host == "" {
			return 0, fmt.Errorf("file URL must be absolute and include a host")
		}
		*fileURL = param.NewOpt(text)
		if name := path.Base(parsed.Path); name != "." && name != "/" && name != "" {
			*filename = param.NewOpt(name)
		}
	} else if text != "" {
		if err := preflightBase64Payload(text, maxInlineBytes); err != nil {
			return 0, fmt.Errorf("inline file payload: %w", err)
		}
		normalized, size, err := normalizeBase64Payload(text)
		if err != nil {
			return 0, fmt.Errorf("file data must be base64 encoded: %w", err)
		}
		decodedSize = size
		encodedData = normalized
	}
	if encodedData != "" {
		name, prefix, err := finalizeInlineFile(effectiveContentType, metadataString(metadata, "filename"), metadata, decodedSize)
		if err != nil {
			return 0, err
		}
		*filename = param.NewOpt(name)
		*fileData = param.NewOpt(prefix + encodedData)
		return decodedSize, nil
	}
	if name := metadataString(metadata, "filename"); name != "" {
		*filename = param.NewOpt(name)
	}
	if err := validateInputFileType(effectiveContentType, filename.Value); err != nil {
		return 0, err
	}
	// Remote and provider-hosted sources carry no decodable payload; size_bytes metadata is
	// the only size signal, so it is adopted rather than cross-checked.
	if metadataSize, present, err := metadataSizeBytes(metadata); err != nil {
		return 0, err
	} else if present {
		decodedSize = metadataSize
	}
	return decodedSize, nil
}

// inlineFileDataURI returns the Responses file_data wire form. OpenAI accepts
// inline files as base64 data URLs (rather than a bare base64 payload). When
// encoded is nil the returned value is the canonical prefix, allowing callers
// that already validated and normalized base64 to append it without decoding a
// second time.
func inlineFileDataURI(contentType, filename string, data []byte) (string, error) {
	mediaType := canonicalContentType(contentType)
	if mediaType == "" {
		mediaType = canonicalContentType(mime.TypeByExtension(strings.ToLower(path.Ext(strings.TrimSpace(filename)))))
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	prefix := "data:" + mediaType + ";base64,"
	if data == nil {
		return prefix, nil
	}
	return prefix + base64.StdEncoding.EncodeToString(data), nil
}

// parseFileDataURI validates and normalizes a data URI into a canonical base64
// payload. Callers add the canonical data:<mime>;base64, prefix required by the
// corresponding Responses input_file or input_image wire field. The returned
// boolean distinguishes a non-data source from a malformed data URI so callers
// cannot silently forward a malformed URI as file bytes.
func parseFileDataURI(value string, maxDecodedBytes uint64) (encoded, mediaType string, decodedSize uint64, isDataURI bool, err error) {
	if !common_util.HasDataURIScheme(value) {
		return "", "", 0, false, nil
	}
	isDataURI = true
	body := value[len("data:"):]
	before, after, ok := strings.Cut(body, ",")
	if !ok {
		return "", "", 0, true, fmt.Errorf("malformed data URI: missing comma separator")
	}

	descriptor, payload := before, after
	if len(descriptor) > maxDataURIMediaDescriptorBytes {
		return "", "", 0, true, fmt.Errorf("malformed data URI: media descriptor exceeds %d bytes", maxDataURIMediaDescriptorBytes)
	}
	segments := strings.Split(descriptor, ";")
	declaredType := segments[0]
	if declaredType == "" {
		declaredType = "text/plain"
	}
	parameters := make([]string, 0, len(segments)-1)
	isBase64 := false
	for i, segment := range segments[1:] {
		if strings.EqualFold(segment, "base64") {
			if isBase64 || i != len(segments[1:])-1 {
				return "", "", 0, true, fmt.Errorf("malformed data URI: base64 marker must appear once at the end of the media descriptor")
			}
			isBase64 = true
			continue
		}
		if segment == "" {
			return "", "", 0, true, fmt.Errorf("malformed data URI: empty media parameter")
		}
		parameters = append(parameters, segment)
	}
	mediaDescriptor := declaredType
	if len(parameters) != 0 {
		mediaDescriptor += ";" + strings.Join(parameters, ";")
	}
	parsedType, _, parseErr := mime.ParseMediaType(mediaDescriptor)
	if parseErr != nil {
		return "", "", 0, true, fmt.Errorf("malformed data URI media type %q: %w", mediaDescriptor, parseErr)
	}
	if parsedType == "" {
		return "", "", 0, true, fmt.Errorf("malformed data URI media type %q", mediaDescriptor)
	}

	maxUnescapedBytes := maxDecodedBytes
	if isBase64 {
		maxUnescapedBytes = maxBase64EncodedBytes(maxDecodedBytes)
	}
	if err := preflightPercentDecodedLength(payload, maxUnescapedBytes); err != nil {
		return "", "", 0, true, fmt.Errorf("malformed data URI payload: %w", err)
	}
	unescaped, unescapeErr := url.PathUnescape(payload)
	if unescapeErr != nil {
		return "", "", 0, true, fmt.Errorf("malformed data URI payload: %w", unescapeErr)
	}
	if isBase64 {
		if err := preflightBase64Payload(unescaped, maxDecodedBytes); err != nil {
			return "", "", 0, true, fmt.Errorf("malformed base64 data URI payload: %w", err)
		}
		encoded, decodedSize, err = normalizeBase64Payload(unescaped)
		if err != nil {
			return "", "", 0, true, fmt.Errorf("malformed base64 data URI payload: %w", err)
		}
	} else {
		data := []byte(unescaped)
		decodedSize = uint64(len(data))
		encoded = base64.StdEncoding.EncodeToString(data)
	}
	if decodedSize == 0 {
		return "", "", 0, true, fmt.Errorf("malformed data URI: payload is empty")
	}
	return encoded, strings.ToLower(parsedType), decodedSize, true, nil
}

// normalizeBase64Payload validates encoded and returns its canonical standard-base64 form and
// decoded size. Canonical input is validated through a streaming decoder and reused verbatim so
// normalization does not allocate buffers proportional to the payload size. Accepted but
// noncanonical input retains the legacy decode-then-encode behavior.
func normalizeBase64Payload(encoded string) (normalized string, decodedSize uint64, err error) {
	if size, ok := canonicalBase64DecodedSize(encoded); ok {
		return encoded, size, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", 0, err
	}
	return base64.StdEncoding.EncodeToString(decoded), uint64(len(decoded)), nil
}

// canonicalBase64DecodedSize reports the exact decoded size when encoded is already in the
// canonical form emitted by base64.StdEncoding.EncodeToString.
func canonicalBase64DecodedSize(encoded string) (uint64, bool) {
	if encoded == "" || len(encoded)%4 != 0 || strings.ContainsAny(encoded, "\r\n") {
		return 0, false
	}

	padding := 0
	for index := len(encoded) - 1; index >= 0 && encoded[index] == '='; index-- {
		padding++
	}
	if padding > 2 || strings.IndexByte(encoded[:len(encoded)-padding], '=') >= 0 {
		return 0, false
	}

	// NewDecoder processes input in 1024-byte chunks. The global padding check above is required
	// so padding at a chunk boundary cannot terminate one chunk before another encoded segment.
	decodedBytes, err := io.Copy(
		io.Discard,
		base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(encoded)),
	)
	if err != nil {
		return 0, false
	}
	return uint64(decodedBytes), true
}

func maxBase64EncodedBytes(decodedBytes uint64) uint64 {
	encodedBytes := (decodedBytes / 3) * 4
	if decodedBytes%3 != 0 {
		encodedBytes += 4
	}
	return encodedBytes
}

// preflightBase64Payload bounds DecodeString's allocation before decoding. The
// standard decoder ignores CR/LF, so the source length itself is also bounded;
// otherwise an attacker could pad a small logical payload with arbitrary
// whitespace and still make DecodeString allocate from the larger source length.
func preflightBase64Payload(encoded string, maxDecodedBytes uint64) error {
	maxEncodedBytes := maxBase64EncodedBytes(maxDecodedBytes)
	if uint64(len(encoded)) > maxEncodedBytes {
		return fmt.Errorf("encoded payload exceeds the remaining %d-byte inline media budget before decoding", maxDecodedBytes)
	}

	var logicalLen uint64
	var last, penultimate byte
	for i := 0; i < len(encoded); i++ {
		if encoded[i] == '\r' || encoded[i] == '\n' {
			continue
		}
		logicalLen++
		penultimate, last = last, encoded[i]
	}
	if logicalLen == 0 || logicalLen%4 != 0 {
		return nil // DecodeString will return the more precise syntax error.
	}

	decodedBytes := (logicalLen / 4) * 3
	if last == '=' {
		decodedBytes--
		if penultimate == '=' {
			decodedBytes--
		}
	}
	if decodedBytes > maxDecodedBytes {
		return fmt.Errorf("decoded payload exceeds the remaining %d-byte inline media budget before decoding", maxDecodedBytes)
	}
	return nil
}

// preflightPercentDecodedLength computes PathUnescape's output length without
// allocating it. Percent-escaped input may be up to three times the decoded
// representation, so comparing the source length directly would either reject
// valid input or allow an unbounded unescape allocation.
func preflightPercentDecodedLength(value string, maxDecodedBytes uint64) error {
	var decodedBytes uint64
	for i := 0; i < len(value); {
		if decodedBytes >= maxDecodedBytes {
			return fmt.Errorf("percent-decoded payload exceeds the remaining %d-byte inline media budget before unescaping", maxDecodedBytes)
		}
		if value[i] != '%' {
			i++
		} else {
			if i+2 >= len(value) || !isHexDigit(value[i+1]) || !isHexDigit(value[i+2]) {
				return fmt.Errorf("invalid URL escape in inline media payload")
			}
			i += 3
		}
		decodedBytes++
	}
	return nil
}

func isHexDigit(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func validProviderFileID(value string) bool {
	return strings.HasPrefix(value, "file-") && len(value) > len("file-") && !strings.ContainsAny(value, " \t\r\n")
}

func validateInputImageType(contentType string) error {
	return openaiinput.ValidateImageType(contentType)
}

func setImageSource(text string, metadata map[string]any, contentType string, maxInlineBytes uint64, fileID, imageURL *param.Opt[string]) (uint64, error) {
	ref := strings.TrimSpace(text)
	var decodedSize uint64
	if validProviderFileID(ref) {
		*fileID = param.NewOpt(ref)
	} else if strings.HasPrefix(ref, "file-") {
		return 0, fmt.Errorf("image file ID is malformed")
	} else if len(ref) >= len("data:") && strings.EqualFold(ref[:len("data:")], "data:") {
		comma := strings.IndexByte(ref, ',')
		if comma < 0 {
			return 0, fmt.Errorf("image data URI must contain a base64 payload")
		}
		descriptor := ref[len("data:"):comma]
		const base64Marker = ";base64"
		if len(descriptor) < len(base64Marker) || !strings.EqualFold(descriptor[len(descriptor)-len(base64Marker):], base64Marker) {
			return 0, fmt.Errorf("image data URI must contain a base64 payload")
		}
		encoded, mediaType, size, _, err := parseFileDataURI(ref, maxInlineBytes)
		if err != nil {
			return 0, err
		}
		canonical, _, _ := mime.ParseMediaType(contentType)
		if !strings.EqualFold(mediaType, canonical) {
			return 0, fmt.Errorf("data URI media type %q does not match part content type %q", mediaType, canonical)
		}
		decodedSize = size
		*imageURL = param.NewOpt("data:" + mediaType + ";base64," + encoded)
	} else {
		if !hasHTTPURLScheme(ref) {
			return 0, fmt.Errorf("image source must be a data URI, file ID, or absolute HTTP(S) URL")
		}
		parsed, err := url.Parse(ref)
		if err != nil || parsed.Host == "" {
			return 0, fmt.Errorf("image source must be a data URI, file ID, or absolute HTTP(S) URL")
		}
		*imageURL = param.NewOpt(ref)
	}
	metadataSize, present, err := metadataSizeBytes(metadata)
	if err != nil {
		return 0, err
	}
	if present {
		if decodedSize != 0 && decodedSize != metadataSize {
			return 0, fmt.Errorf("image size_bytes metadata %d does not match decoded payload size %d", metadataSize, decodedSize)
		}
		decodedSize = metadataSize
	}
	return decodedSize, nil
}

func hasHTTPURLScheme(value string) bool {
	return len(value) >= len("http://") && strings.EqualFold(value[:len("http://")], "http://") ||
		len(value) >= len("https://") && strings.EqualFold(value[:len("https://")], "https://")
}

func defaultFilename(contentType string) string {
	if extension := openaiinput.DefaultExtension(contentType); extension != "" {
		return "input" + extension
	}
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

func validateInputFileType(contentType, filename string) error {
	return openaiinput.ValidateFileType(contentType, filename)
}

func canonicalContentType(contentType string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed)
}

func imageDetail(metadata map[string]any) (responses.ResponseInputImageDetail, error) {
	detail := metadataString(metadata, "detail")
	if detail == "" {
		return responses.ResponseInputImageDetailAuto, nil
	}
	switch responses.ResponseInputImageDetail(detail) {
	case responses.ResponseInputImageDetailAuto, responses.ResponseInputImageDetailLow,
		responses.ResponseInputImageDetailHigh, responses.ResponseInputImageDetailOriginal:
		return responses.ResponseInputImageDetail(detail), nil
	default:
		return "", fmt.Errorf("unsupported image detail %q", detail)
	}
}

func imageContentDetail(metadata map[string]any) (responses.ResponseInputImageContentDetail, error) {
	detail := metadataString(metadata, "detail")
	if detail == "" {
		return responses.ResponseInputImageContentDetailAuto, nil
	}
	switch responses.ResponseInputImageContentDetail(detail) {
	case responses.ResponseInputImageContentDetailAuto, responses.ResponseInputImageContentDetailLow,
		responses.ResponseInputImageContentDetailHigh, responses.ResponseInputImageContentDetailOriginal:
		return responses.ResponseInputImageContentDetail(detail), nil
	default:
		return "", fmt.Errorf("unsupported image detail %q", detail)
	}
}

func inputFileDetail(metadata map[string]any, contentType, filename string) (responses.ResponseInputFileDetail, error) {
	detail, err := validatedInputFileDetail(metadata, contentType, filename)
	if err != nil {
		return "", err
	}
	return responses.ResponseInputFileDetail(detail), nil
}

func inputFileContentDetail(metadata map[string]any, contentType, filename string) (responses.ResponseInputFileContentDetail, error) {
	detail, err := validatedInputFileDetail(metadata, contentType, filename)
	if err != nil {
		return "", err
	}
	return responses.ResponseInputFileContentDetail(detail), nil
}

func validatedInputFileDetail(metadata map[string]any, contentType, filename string) (string, error) {
	detail := metadataString(metadata, "detail")
	if detail == "" {
		return "", nil
	}
	switch detail {
	case string(responses.ResponseInputFileDetailAuto), string(responses.ResponseInputFileDetailLow), string(responses.ResponseInputFileDetailHigh):
	default:
		return "", fmt.Errorf("unsupported input_file detail %q", detail)
	}
	canonical := canonicalContentType(contentType)
	extension := strings.ToLower(path.Ext(strings.TrimSpace(filename)))
	if canonical != "application/pdf" && extension != ".pdf" && !(canonical == "" && filename == "") {
		return "", fmt.Errorf("input_file detail %q requires a PDF, got content type %q and filename %q", detail, canonical, filename)
	}
	return detail, nil
}

func outputText(output any) (string, error) {
	if text, ok := output.(string); ok {
		return text, nil
	}
	b, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("marshal tool output: %w", err)
	}
	return string(b), nil
}

func toolsToParams(defs []*ai.ToolDefinition, bindings []ClientToolBinding) ([]responses.ToolUnionParam, map[string]string, error) {
	byGenkit := make(map[string]ClientToolBinding, len(bindings))
	for i, binding := range bindings {
		name := strings.TrimSpace(binding.GenkitToolName)
		if name == "" {
			return nil, nil, fmt.Errorf("openairesponses: clientTools[%d].genkitToolName is required", i)
		}
		normalizedTool, err := normalizeToolUnion(binding.Tool)
		if err != nil {
			return nil, nil, fmt.Errorf("openairesponses: normalize client tool %q: %w", name, err)
		}
		binding.Tool = normalizedTool
		if binding.Tool.OfFunction == nil {
			return nil, nil, fmt.Errorf("openairesponses: client tool %q uses an unsupported native tool variant; only function tools are supported", name)
		}
		if _, exists := byGenkit[name]; exists {
			return nil, nil, fmt.Errorf("openairesponses: duplicate client tool binding for %q", name)
		}
		byGenkit[name] = binding
	}

	tools := make([]responses.ToolUnionParam, 0, len(defs))
	providerToGenkit := make(map[string]string, len(defs))
	seenGenkit := make(map[string]bool, len(defs))
	for i, def := range defs {
		if def == nil || strings.TrimSpace(def.Name) == "" {
			return nil, nil, fmt.Errorf("openairesponses: tool definition %d requires a name", i)
		}
		genkitName := strings.TrimSpace(def.Name)
		if seenGenkit[genkitName] {
			return nil, nil, fmt.Errorf("openairesponses: duplicate Genkit tool definition %q", genkitName)
		}
		seenGenkit[genkitName] = true
		if binding, ok := byGenkit[genkitName]; ok {
			boundTool, err := cloneClientFunctionTool(binding.Tool)
			if err != nil {
				return nil, nil, fmt.Errorf("openairesponses: client tool %q: %w", genkitName, err)
			}
			providerName := strings.TrimSpace(boundTool.OfFunction.Name)
			if err := validateResponseName(providerName, "function tool"); err != nil {
				return nil, nil, err
			}
			boundTool.OfFunction.Name = providerName
			if old, exists := providerToGenkit[providerName]; exists && old != genkitName {
				return nil, nil, fmt.Errorf("openairesponses: provider function %q is bound to both %q and %q", providerName, old, genkitName)
			}
			providerToGenkit[providerName] = genkitName
			tools = append(tools, boundTool)
			continue
		}

		if err := validateResponseName(genkitName, "Genkit tool"); err != nil {
			return nil, nil, err
		}
		strict, _ := def.Metadata["strict"].(bool)
		inputSchema, err := prepareToolSchema(def.InputSchema, strict)
		if err != nil {
			return nil, nil, fmt.Errorf("openairesponses: tool %q input schema: %w", genkitName, err)
		}
		tool := responses.ToolParamOfFunction(genkitName, inputSchema, strict)
		tool.OfFunction.Description = param.NewOpt(def.Description)
		if def.OutputSchema != nil {
			outputSchema, err := prepareToolSchema(def.OutputSchema, strict)
			if err != nil {
				return nil, nil, fmt.Errorf("openairesponses: tool %q output schema: %w", genkitName, err)
			}
			tool.OfFunction.OutputSchema = outputSchema
		}
		if old, exists := providerToGenkit[genkitName]; exists && old != genkitName {
			return nil, nil, fmt.Errorf("openairesponses: provider function %q is bound to both %q and %q", genkitName, old, genkitName)
		}
		providerToGenkit[genkitName] = genkitName
		tools = append(tools, tool)
	}
	for genkitName := range byGenkit {
		if !seenGenkit[genkitName] {
			return nil, nil, fmt.Errorf("openairesponses: client tool binding %q has no matching Genkit tool definition", genkitName)
		}
	}
	return tools, providerToGenkit, nil
}

// normalizeToolUnion materializes SDK escape-hatch overrides before the
// function-only ClientTools policy is checked. SetExtraFields can otherwise
// replace a function tool's discriminator after the typed variant check.
func normalizeToolUnion(tool responses.ToolUnionParam) (responses.ToolUnionParam, error) {
	encoded, err := json.Marshal(tool)
	if err != nil {
		return responses.ToolUnionParam{}, fmt.Errorf("encode tool: %w", err)
	}
	var normalized responses.ToolUnionParam
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return responses.ToolUnionParam{}, fmt.Errorf("decode tool: %w", err)
	}
	return normalized, nil
}

func validateResponseName(name, kind string) error {
	if !responseNamePattern.MatchString(name) {
		return fmt.Errorf("openairesponses: %s name %q must match %s", kind, name, responseNamePattern.String())
	}
	return nil
}

func cloneClientFunctionTool(input responses.ToolUnionParam) (responses.ToolUnionParam, error) {
	if input.OfFunction == nil {
		return responses.ToolUnionParam{}, fmt.Errorf("only function tools can be bound to Genkit tools")
	}
	output := input
	function := *input.OfFunction
	strict := function.Strict.Valid() && function.Strict.Value
	parameters, err := prepareToolSchema(function.Parameters, strict)
	if err != nil {
		return responses.ToolUnionParam{}, fmt.Errorf("input schema: %w", err)
	}
	function.Parameters = parameters
	if function.OutputSchema != nil {
		outputSchema, err := prepareToolSchema(function.OutputSchema, strict)
		if err != nil {
			return responses.ToolUnionParam{}, fmt.Errorf("output schema: %w", err)
		}
		function.OutputSchema = outputSchema
	}
	output.OfFunction = &function
	return output, nil
}

// prepareJSONSchema makes an independent JSON-safe schema and, in strict mode,
// applies the object constraints required by OpenAI. Optional fields in a strict
// schema must be represented as nullable because every property is required.
func prepareJSONSchema(input map[string]any, strict bool) (map[string]any, error) {
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

func normalizeStrictSchema(schema map[string]any) {
	strictschema.Close(schema, true)
}

func prepareToolSchema(input map[string]any, strict bool) (map[string]any, error) {
	output, err := prepareJSONSchema(input, false)
	if err == nil && strict {
		strictschema.Close(output, false)
	}
	return output, err
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case jsontext.Value:
		return string(value)
	default:
		return ""
	}
}

func metadataBool(metadata map[string]any, key string) bool {
	if metadata == nil {
		return false
	}
	value, _ := metadata[key].(bool)
	return value
}

func metadataSizeBytes(metadata map[string]any) (uint64, bool, error) {
	if metadata == nil {
		return 0, false, nil
	}
	value, ok := metadata["size_bytes"]
	if !ok {
		return 0, false, nil
	}
	var size uint64
	switch typed := value.(type) {
	case uint64:
		size = typed
	case uint32:
		size = uint64(typed)
	case uint:
		size = uint64(typed)
	case int:
		if typed < 0 {
			return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
		}
		size = uint64(typed)
	case int64:
		if typed < 0 {
			return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
		}
		size = uint64(typed)
	case float64:
		if typed < 0 || typed != float64(uint64(typed)) {
			return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
		}
		size = uint64(typed)
	case jsontext.Value:
		parsed, err := strconv.ParseUint(string(typed), 10, 64)
		if err != nil {
			return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
		}
		size = parsed
	default:
		return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
	}
	if size == 0 {
		return 0, true, fmt.Errorf("file size_bytes metadata must be a positive integer")
	}
	return size, true, nil
}
