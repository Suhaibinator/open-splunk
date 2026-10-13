package common_service

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/internal/hotpoll"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_converter "github.com/Suhaibinator/go-common/pkg/common_converter"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	anthropicsdk "github.com/anthropics/anthropic-sdk-go"
	anthropicparam "github.com/anthropics/anthropic-sdk-go/packages/param"
	genkitai "github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	azureaifoundry "github.com/xavidop/genkit-azure-foundry-go"
	"go.uber.org/zap"
	googlegenai "google.golang.org/genai"
)

var (
	errIncompleteAgentRunStep = errors.New("agent runtime: incomplete durable model step")
	errFailedAgentRunToolStep = errors.New("agent runtime: sealed tool step contains a terminal execution failure")
	// errAgentRuntimeStopping is the cancellation cause of the managed and recovery roots on Stop;
	// an attempt abandoned under it is a shutdown abandonment (see driveTurn).
	errAgentRuntimeStopping = errors.New("agent runtime: stopping")
	// errGraphDriveSuspended is the cancellation cause of a DriveGraph graph context: claim loss and
	// Completed cleanup suspend the graph's drives. Its abandonments are shutdown abandonments.
	errGraphDriveSuspended = errors.New("agent runtime: graph drive suspended")
	// errAgentRunDriveAbandoned reports a drive whose call ignored cancellation past the abandon grace.
	errAgentRunDriveAbandoned = errors.New("agent runtime: drive abandoned after its call ignored cancellation")
)

// transcriptReader adapts the persistence transcript reads to commonai.TranscriptReader: it reads
// hidden-inclusive (ServiceTranscript) and converts persistence ChatMessage rows to service-model
// rows so the ai context builder never imports persistence. It is the internal read seam behind
// GetContextWindow.
type transcriptReader struct {
	persistence common_persistence.CommonPersistenceInterface
	converter   common_converter.ConverterInterface
}

// GetLatestCompaction returns the latest compaction checkpoint for the scope, converted to the
// service model, or nil when none exists.
func (r transcriptReader) GetLatestCompaction(ctx context.Context, channelID uint64, parentMessageID uint64) (*common_service_models.ChatMessage, error) {
	pMsg, err := r.persistence.GetLatestCompaction(ctx, channelID, parentMessageID)
	if err != nil {
		return nil, err
	}
	if pMsg == nil {
		return nil, nil
	}
	return r.converter.PersistenceChatMessageToService(pMsg), nil
}

// StreamMessagesAfter streams transcript rows after the watermark with the ServiceTranscript
// (hidden-inclusive) read mode, converting each batch to service-model rows before handing them to
// fn. The unbounded id cursor loop lives in the persistence StreamChatMessagesAfter; this adapter
// only converts.
func (r transcriptReader) StreamMessagesAfter(
	ctx context.Context,
	channelID uint64,
	parentMessageID uint64,
	afterMessageID uint64,
	fn func(batch []*common_service_models.ChatMessage) (stop bool, err error),
) error {
	opts := common_persistence.ChatMessageReadOptions{
		VisibilityMode:         common_persistence.ChatMessageVisibilityServiceTranscript,
		ModelContextProjection: true,
	}
	return r.persistence.StreamChatMessagesAfter(ctx, channelID, parentMessageID, afterMessageID, opts, 0,
		func(pBatch []*common_persistence_models.ChatMessage) (bool, error) {
			batch := make([]*common_service_models.ChatMessage, 0, len(pBatch))
			for _, pMsg := range pBatch {
				if sMsg := r.converter.PersistenceChatMessageToService(pMsg); sMsg != nil {
					batch = append(batch, sMsg)
				}
			}
			return fn(batch)
		})
}

// newTranscriptReader builds the persistence-backed TranscriptReader for the agent context builder.
func (h *CommonServiceHandler[U]) newTranscriptReader() commonai.TranscriptReader {
	return transcriptReader{persistence: h.commonPersistenceHandler, converter: h.converter}
}

// newToolCallExecutor builds the pure tool executor wired to this service handler's agent tool
// registry and a persistence-backed captured-snapshot reader. The reader maps the normalized
// ChatAgentSessionToolConfig rows for a captured config version into the executor's minimal
// SessionToolConfig view, so the executor can verify a tool was enabled in the run's captured
// snapshot without importing persistence models. It returns a ports.ToolCallExecutor suitable for
// injection into the chat handler's approved-work scanner.
func (h *CommonServiceHandler[U]) newToolCallExecutor() ports.ToolCallExecutor {
	reader := transactionalSessionToolConfigReader{commonai.SessionToolConfigReaderFunc(func(ctx context.Context, sessionID, version uint64) ([]commonai.SessionToolConfig, error) {
		_, toolConfigs, err := h.commonPersistenceHandler.GetChatAgentSessionConfigSnapshot(ctx, sessionID, version)
		if err != nil {
			return nil, err
		}
		out := make([]commonai.SessionToolConfig, 0, len(toolConfigs))
		for _, tc := range toolConfigs {
			out = append(out, commonai.SessionToolConfig{
				ToolName:         tc.ToolName,
				Enabled:          tc.Enabled,
				RequiresApproval: tc.RequiresApproval,
			})
		}
		return out, nil
	})}
	actors := agentkit.NewActorContext(h.GetUserByID, 0, 0)
	return &workflowToolExecutor[U]{handler: h, inner: commonai.NewToolCallExecutor(h.agentToolRegistry, reader, actors.Attach)}
}

// AgentGeneratorFactoryInput is the immutable run/session context passed to an app-supplied agent
// generator factory. It includes the resolved model policy and captured tool set plus durable
// session/run identity so apps can route by agent type/run rather than provider/model only.
type AgentGeneratorFactoryInput struct {
	Purpose                   proto_models.InferenceCallPurpose
	OrganizationID            common_service_models.OrganizationIdType
	AgentType                 string
	ChannelID                 uint64
	ParentMessageID           uint64
	AgentRunID                uint64
	AgentSessionID            uint64
	AgentSessionConfigVersion uint64
	// ParentAgentSessionID is the delegating parent's session when the run belongs to a delegated
	// child, zero otherwise, so an application factory can attribute child work to the parent.
	ParentAgentSessionID uint64
	TriggeringUserID     common_service_models.UserIdType
	// InferenceActorUserID is the user the route was planned for: the session's accountable human
	// when it has one, otherwise TriggeringUserID. A custom factory must hand it to its retrier's
	// GenerationCallContext (zero falls back to TriggeringUserID).
	InferenceActorUserID common_service_models.UserIdType
	ContextID            uint64
	ContextEntityID      uint64
	Metadata             map[string]string
	Policy               ports.ModelPolicy
	Tools                []ports.AgentToolDefinition
	// Retrier wraps each attempt of the generator built for this run. It comes from the drive's
	// generation plan, already bound to the plan's resolved route, so retry classification, rate
	// buckets, and attempt-ledger attribution agree with the model the generator runs on. It is
	// required before the runtime builds any generator. The DEFAULT factory
	// (SetAgentModelRuntimeFactory) threads it into the generator it builds; custom factories receive
	// the same required route-bound retrier.
	Retrier commonai.GenerationRetrier
}

// AgentGeneratorFactory builds the ai.Generator (one transcript-native Genkit request/response) for
// one agent run, from the run's resolved context, model policy, and tool set. It is the seam the
// runtime uses to obtain a provider/model-bound generator without the runtime importing provider SDK
// config (API keys, project IDs). The default factory (SetAgentModelRuntimeFactory) is backed by
// genai.ModelRuntimeFactory; an app may inject its own (for example a test generator) via
// WithAgentGeneratorFactory.
type AgentGeneratorFactory func(ctx context.Context, input AgentGeneratorFactoryInput) (commonai.Generator, error)

// The agent-run lifecycle types are re-exported from ports for the service-handler API.
type AgentRunStatus = ports.AgentRunStatus
type AgentRunStatusReason = ports.AgentRunStatusReason
type AgentRunTransition = ports.AgentRunTransition
type AgentRunCancellation = ports.AgentRunCancellation
type AgentRunObserver = ports.AgentRunObserver

// ErrResolvedModelPolicyIncomplete is returned when a per-run model-policy resolver returns a
// non-nil policy missing a concrete provider or model ID. It mirrors the registration-time
// model-policy discipline (bot.ErrAgentModelPolicyRequired) for the live-resolution path: an
// incomplete resolved policy fails the run start rather than serving a blank/garbage route.
var ErrResolvedModelPolicyIncomplete = errors.New("resolved agent model policy is incomplete: provider and model id are required")

// ErrModelPolicyResolverFailed classifies an error returned BY an app-supplied model-policy resolver
// (a route lookup failure, or a route the app rejects such as an inactive inference model). The
// resolver's own error stays wrapped alongside it; the sentinel exists so the runtime can recognize
// the failure as terminal for the run without knowing anything about app-specific error types.
var ErrModelPolicyResolverFailed = errors.New("agent model policy resolver failed")

// ErrModelPolicyResolverRouteMismatch means the compatibility resolver tried to replace the
// provider/model identity selected by the operation's control-plane plan. A resolver may customize
// generation config for that same model, but it cannot redirect attempts around the planned route.
var ErrModelPolicyResolverRouteMismatch = errors.New("agent model policy resolver changed the operation-routed provider/model")

// ErrImmutableAgentRuntimeConfiguration classifies deterministic setup failures in go-common's
// default generator factory or a run's captured configuration snapshot. Re-driving a run resolves
// the same process-local runtime and the same immutable snapshot, so these failures cannot heal via
// recovery. App-supplied generator factories are deliberately not wrapped with this sentinel: their
// errors may represent transient routing or dependency failures and remain retryable by default.
var ErrImmutableAgentRuntimeConfiguration = errors.New("immutable agent runtime configuration is invalid")

// ErrProviderConfigBuilderFailed classifies a panic from an app-owned provider-config builder. The
// builder signature intentionally has no error return, so a panic is converted into a terminal run
// setup error instead of taking down the drive goroutine (or process).
var ErrProviderConfigBuilderFailed = errors.New("agent provider config builder failed")

// AgentModelPolicyResolverInput is the per-run context passed to an app-supplied model-policy
// resolver: the agent type, organization, triggering actor, and the drive's session and run.
type AgentModelPolicyResolverInput struct {
	AgentType      string
	OrganizationID common_service_models.OrganizationIdType
	ActorUserID    common_service_models.UserIdType
	AgentSessionID uint64
	AgentRunID     uint64
}

// AgentModelPolicyResolver is a compatibility override composed after the operation-routed plan.
// It is consulted once per drive; errors, nil policies, incomplete policies, and provider/model
// identity changes fail setup. It may customize config for the already-routed model.
type AgentModelPolicyResolver func(ctx context.Context, in AgentModelPolicyResolverInput) (*ports.ModelPolicy, error)

// setAgentGeneratorFactory replaces the generator factory; WithAgentGeneratorFactory and
// SetAgentModelRuntimeFactory install theirs through it.
func (h *CommonServiceHandler[U]) setAgentGeneratorFactory(factory AgentGeneratorFactory) {
	if h == nil {
		return
	}
	h.ensureAgentRuntime()
	h.agentRuntime.generatorFactory = factory
}

// SetAgentUsageObserver registers a per-attempt usage observer for the DEFAULT agent generator. It
// receives explicit start/finish events per attempt (including retries); a nil observer is a no-op.
// It has no effect on a custom generator installed via WithAgentGeneratorFactory. Call before serving
// traffic; the order relative to SetAgentModelRuntimeFactory does not matter (the factory closure
// reads this value at generator-construction time).
func (h *CommonServiceHandler[U]) SetAgentUsageObserver(o commonai.GenerationUsageObserver) {
	if h == nil {
		return
	}
	h.ensureAgentRuntime()
	h.agentRuntime.usageObserver = o
}

// setAgentRunObserver installs the WithAgentRunObserver hook on the runtime and the chat handler.
// A nil observer clears it.
func (h *CommonServiceHandler[U]) setAgentRunObserver(observer AgentRunObserver) {
	if h == nil {
		return
	}
	h.ensureAgentRuntime()
	h.agentRuntime.runObserver = observer
	if h.chatHandler != nil {
		h.chatHandler.SetAgentRunObserver(observer)
	}
}

// SetAgentModelRuntimeFactory wires the default Genkit generator factory for agent runs from a
// genai.ModelRuntimeFactory, so the runtime can resolve an agent's ModelPolicy (provider + model ID)
// into a live genkit app/model. This is the agent-runtime counterpart to
// the constructor-injected catalog and activates the production fresh-turn / resume
// loop; without a generator factory the runtime is inert (it logs and returns without generating).
func (h *CommonServiceHandler[U]) SetAgentModelRuntimeFactory(factory *genai.ModelRuntimeFactory) {
	if h == nil || factory == nil || (h.agentRuntime != nil && h.agentRuntime.customGeneratorFactory) {
		return
	}
	h.setAgentGeneratorFactory(func(_ context.Context, input AgentGeneratorFactoryInput) (commonai.Generator, error) {
		resolved, err := factory.ResolveModelForAPI(&common_service_models.InferenceModel{
			ProviderID:      input.Policy.ProviderID,
			ProviderModelID: input.Policy.ModelID,
		}, input.Policy.API)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve default model runtime: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		// The retrier is per-drive state carried on the input (the drive's plan bound it to the
		// resolved route). The observer and logger are process-wide seams read here, at
		// generator-construction time, so an app may register the observer before OR after this
		// factory is installed. h.agentRuntime is non-nil: setAgentGeneratorFactory (called above)
		// ran ensureAgentRuntime, and nothing clears it.
		rt := h.agentRuntime
		policy := input.Policy
		policy.API = resolved.API
		config, err := modelPolicyConfig(policy, rt.modelPolicyDefaults())
		if err != nil {
			return nil, err
		}
		generator, err := commonai.NewGenkitGenerator(
			resolved.Genkit,
			resolved.ModelName,
			input.Tools,
			config,
			commonai.WithActualModel(resolved.ProviderID, resolved.ProviderModelID),
			commonai.WithActualAPI(resolved.API, resolved.DestinationFingerprint),
			commonai.WithGenerationCallContext(newGenerationCallContext(input, resolved)),
			commonai.WithRetrier(input.Retrier),
			commonai.WithUsageObserver(rt.usageObserver),
			commonai.WithLogger(rt.logger),
		)
		if err != nil {
			return nil, fmt.Errorf("%w: construct default generator: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		return generator, nil
	})
}

// newGenerationCallContext builds the ai.GenerationCallContext that attributes a default agent
// generator's attempts to one run: the run/session identity comes from the factory input, the
// provider + model come from the resolved model (drive-scoped, so observers attribute usage to the
// model that actually generated), and Purpose is the run's REPRESENTATIVE purpose
// (assistant turn). One generator drives every step of a run, so a single fixed Purpose cannot
// distinguish a tool-loop model call from the final assistant turn — that per-step purpose is only
// known once the outcome is produced. An observer that needs the precise purpose derives it from the
// finished event's ai.GenerationOutcome.ToolRequests (see the field doc on GenerationCallContext.
// Purpose); the internal ChatInferenceCall ledger records the precise per-step purpose itself. It
// is a package-level helper so the input->context mapping is unit-testable without a live generator.
func newGenerationCallContext(input AgentGeneratorFactoryInput, resolved *genai.ResolvedModel) commonai.GenerationCallContext {
	call := commonai.GenerationCallContext{
		OrganizationID:            input.OrganizationID,
		ChannelID:                 input.ChannelID,
		ParentMessageID:           input.ParentMessageID,
		AgentRunID:                input.AgentRunID,
		AgentSessionID:            input.AgentSessionID,
		AgentSessionConfigVersion: input.AgentSessionConfigVersion,
		AgentType:                 input.AgentType,
		TriggeringUserID:          input.TriggeringUserID,
		InferenceActorUserID:      input.InferenceActorUserID,
		Purpose:                   input.Purpose,
	}
	if call.Purpose == 0 {
		call.Purpose = proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN
	}
	if resolved != nil {
		call.ProviderID = resolved.ProviderID
		call.ProviderModelID = resolved.ProviderModelID
	}
	return call
}

// modelPolicyDefaults is one sample of the hot keys that fill request fields a
// provider requires when neither the model policy nor the native config sets them.
type modelPolicyDefaults struct {
	anthropicMaxOutputTokens int64
}

// modelPolicyConfig copies the selected API's typed configuration and overlays
// route-owned settings. Caller-owned configs remain immutable. defaults is the
// per-operation sample of the hot fallback keys (see modelPolicyDefaults).
func modelPolicyConfig(policy ports.ModelPolicy, defaults modelPolicyDefaults) (*modelconfig.Config, error) {
	if err := policy.ProviderConfig.Validate(policy.API); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
	}
	cfg := modelconfig.Config{}
	if policy.ProviderConfig != nil {
		cfg = *policy.ProviderConfig
	}
	api := policy.API
	if api == apishape.Default && policy.ProviderConfig != nil {
		switch {
		case cfg.ChatCompletions != nil:
			api = apishape.ChatCompletions
		case cfg.Responses != nil:
			api = apishape.Responses
		case cfg.Anthropic != nil:
			api = apishape.AnthropicMessages
		case cfg.Google != nil:
			api = apishape.GoogleNative
		case cfg.Azure != nil:
			api = apishape.AzureNative
		}
	}
	hasOverrides := policy.Temperature != nil || policy.MaxOutputTokens != nil
	switch api {
	case apishape.ChatCompletions:
		if cfg.ChatCompletions == nil && !hasOverrides {
			return nil, nil
		}
		params := openai.ChatCompletionNewParams{}
		if cfg.ChatCompletions != nil {
			params = *cfg.ChatCompletions
		}
		if err := overlayOpenAICompatiblePolicy(&params, policy); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		cfg.ChatCompletions = &params
	case apishape.Responses:
		if cfg.Responses == nil && !hasOverrides && policy.RequestReasoningTraces == nil {
			return nil, nil
		}
		native := modelconfig.ResponsesConfig{}
		if cfg.Responses != nil {
			native = *cfg.Responses
		}
		if native.Common != nil {
			common := *native.Common
			native.Common = &common
		}
		members, overridden, err := modelconfig.OverrideMembers(native.Params)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		if overridden && hasOverrides {
			// A whole-object override ignores typed fields, and SetExtraFields
			// would replace it, so patch its JSON members instead.
			if policy.Temperature != nil {
				err = modelconfig.SetOverrideMember(members, "temperature", float64(*policy.Temperature))
			}
			if err == nil && policy.MaxOutputTokens != nil {
				err = modelconfig.SetOverrideMember(members, "max_output_tokens", int64(*policy.MaxOutputTokens))
			}
			var encoded []byte
			if err == nil {
				encoded, err = modelconfig.EncodeOverrideMembers(members)
			}
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
			}
			param.SetJSON(encoded, &native.Params)
		} else if hasOverrides {
			// SDK extras take precedence over typed fields. Route-owned knobs must
			// win even when an SDK caller supplied them through its extras API.
			extras := maps.Clone(native.Params.ExtraFields())
			if policy.Temperature != nil {
				delete(extras, "temperature")
				native.Params.Temperature = param.NewOpt(float64(*policy.Temperature))
			}
			if policy.MaxOutputTokens != nil {
				delete(extras, "max_output_tokens")
				native.Params.MaxOutputTokens = param.NewOpt(int64(*policy.MaxOutputTokens))
			}
			native.Params.SetExtraFields(extras)
		}
		if native.Common != nil && policy.Temperature != nil {
			native.Common.Temperature = 0
		}
		if native.Common != nil && policy.MaxOutputTokens != nil {
			native.Common.MaxOutputTokens = 0
		}
		if policy.RequestReasoningTraces != nil {
			requested := *policy.RequestReasoningTraces
			native.RequestReasoningTraces = &requested
		}
		cfg.Responses = &native
	case apishape.AzureNative:
		if cfg.Azure == nil && !hasOverrides {
			return nil, nil
		}
		native := azureaifoundry.GenerationConfig{}
		if cfg.Azure != nil {
			native = *cfg.Azure
		}
		if policy.Temperature != nil {
			native.Temperature = new(float64(*policy.Temperature))
		}
		if policy.MaxOutputTokens != nil {
			native.MaxOutputTokens = new(int64(*policy.MaxOutputTokens))
		}
		cfg.Azure = &native
	case apishape.GoogleNative:
		if cfg.Google == nil && !hasOverrides && !shouldRequestReasoningTraces(policy) {
			return nil, nil
		}
		native := googlegenai.GenerateContentConfig{}
		if cfg.Google != nil {
			native = *cfg.Google
		}
		if policy.Temperature != nil {
			native.Temperature = new(*policy.Temperature)
		}
		if policy.MaxOutputTokens != nil {
			native.MaxOutputTokens = int32(*policy.MaxOutputTokens)
		}
		if native.ThinkingConfig != nil {
			thinking := *native.ThinkingConfig
			native.ThinkingConfig = &thinking
		}
		if shouldRequestReasoningTraces(policy) {
			if native.ThinkingConfig == nil {
				native.ThinkingConfig = &googlegenai.ThinkingConfig{}
			}
			native.ThinkingConfig.IncludeThoughts = true
		} else if native.ThinkingConfig != nil {
			native.ThinkingConfig.IncludeThoughts = false
		}
		cfg.Google = &native
	case apishape.AnthropicMessages:
		native := anthropicsdk.MessageNewParams{}
		if cfg.Anthropic != nil {
			native = *cfg.Anthropic
		}
		members, overridden, err := modelconfig.OverrideMembers(native)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		if overridden {
			// A whole-object override ignores typed fields, so the route's
			// settings and the required max_tokens fallback go into its members.
			if err := overlayAnthropicOverride(&native, members, policy, defaults); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
			}
			cfg.Anthropic = &native
			break
		}
		if native.MaxTokens == 0 {
			native.MaxTokens = defaults.anthropicMaxOutputTokens
		}
		if policy.MaxOutputTokens != nil {
			native.MaxTokens = int64(*policy.MaxOutputTokens)
		}
		if policy.Temperature != nil {
			native.Temperature = anthropicsdk.Float(float64(*policy.Temperature))
		}
		cfg.Anthropic = &native
	case apishape.Default:
		if cfg.Common == nil && !hasOverrides {
			return nil, nil
		}
		native := genkitai.GenerationCommonConfig{}
		if cfg.Common != nil {
			native = *cfg.Common
		}
		if policy.Temperature != nil {
			native.Temperature = float64(*policy.Temperature)
		}
		if policy.MaxOutputTokens != nil {
			native.MaxOutputTokens = int(*policy.MaxOutputTokens)
		}
		cfg.Common = &native
	default:
		return nil, fmt.Errorf("%w: unsupported inference API %s", ErrImmutableAgentRuntimeConfiguration, api)
	}
	return &cfg, nil
}

func overlayOpenAICompatiblePolicy(config *openai.ChatCompletionNewParams, policy ports.ModelPolicy) error {
	if policy.Temperature == nil && policy.MaxOutputTokens == nil {
		return nil
	}
	members, overridden, err := modelconfig.OverrideMembers(*config)
	if err != nil {
		return err
	}
	if overridden {
		// A whole-object override ignores typed fields, and SetExtraFields
		// would replace it, so patch its JSON members instead.
		if policy.Temperature != nil {
			err = modelconfig.SetOverrideMember(members, "temperature", float64(*policy.Temperature))
		}
		if err == nil && policy.MaxOutputTokens != nil {
			err = modelconfig.SetOverrideMember(members, "max_completion_tokens", int64(*policy.MaxOutputTokens))
			delete(members, "max_tokens")
		}
		var encoded []byte
		if err == nil {
			encoded, err = modelconfig.EncodeOverrideMembers(members)
		}
		if err != nil {
			return err
		}
		param.SetJSON(encoded, config)
		return nil
	}
	extras := maps.Clone(config.ExtraFields())
	if policy.Temperature != nil {
		delete(extras, "temperature")
		config.Temperature = param.NewOpt(float64(*policy.Temperature))
	}
	if policy.MaxOutputTokens != nil {
		delete(extras, "max_completion_tokens")
		delete(extras, "max_tokens")
		config.MaxCompletionTokens = param.NewOpt(int64(*policy.MaxOutputTokens))
		config.MaxTokens = param.Opt[int64]{}
	}
	config.SetExtraFields(extras)
	return nil
}

// overlayAnthropicOverride writes route policy and the max_tokens fallback
// into an Anthropic whole-object override, keeping its other members.
func overlayAnthropicOverride(native *anthropicsdk.MessageNewParams, members map[string]jsontext.Value, policy ports.ModelPolicy, defaults modelPolicyDefaults) error {
	var err error
	switch maxTokens, set := members["max_tokens"]; {
	case policy.MaxOutputTokens != nil:
		err = modelconfig.SetOverrideMember(members, "max_tokens", int64(*policy.MaxOutputTokens))
	case !set || maxTokens.Kind() == 'n':
		err = modelconfig.SetOverrideMember(members, "max_tokens", defaults.anthropicMaxOutputTokens)
	}
	if err == nil && policy.Temperature != nil {
		err = modelconfig.SetOverrideMember(members, "temperature", float64(*policy.Temperature))
	}
	if err != nil {
		return err
	}
	encoded, err := modelconfig.EncodeOverrideMembers(members)
	if err != nil {
		return err
	}
	anthropicparam.SetJSON(encoded, native)
	return nil
}

func shouldRequestReasoningTraces(policy ports.ModelPolicy) bool {
	if policy.RequestReasoningTraces == nil {
		return true
	}
	return *policy.RequestReasoningTraces
}

func agentSessionMetadataFromJSON(raw []byte) map[string]string {
	metadata, _ := common_service_models.DecodeAgentSessionMetadata(raw)
	return metadata
}

func copyAgentToolDefinitions(input []ports.AgentToolDefinition) []ports.AgentToolDefinition {
	if len(input) == 0 {
		return nil
	}
	out := make([]ports.AgentToolDefinition, len(input))
	copy(out, input)
	for i := range out {
		out[i].InputSchemaJSON = append([]byte(nil), out[i].InputSchemaJSON...)
		out[i].OutputSchemaJSON = append([]byte(nil), out[i].OutputSchemaJSON...)
	}
	return out
}

// agentRuntime drives the transcript-native Genkit continuation loop for registered agent sessions.
// It is the production caller of ai.ContinuationRunner + ai.GenkitGenerator: it resolves a run's
// captured config snapshot, the registered agent + tools, and a provider-bound generator, then runs
// the loop and transitions the run to COMPLETED on a final assistant turn. It holds only the seams
// it needs from the service handler so its logic is independently testable.
type agentRuntime[U common_service_models.UserLike] struct {
	persistence       common_persistence.CommonPersistenceInterface
	eventPublisher    ports.ChatEventPublisher
	converter         common_converter.ConverterInterface
	reader            commonai.TranscriptReader
	executor          ports.ToolCallExecutor
	agentRegistry     ports.AgentRegistry
	agentToolRegistry ports.AgentToolRegistry
	generatorFactory  AgentGeneratorFactory
	// customGeneratorFactory marks a WithAgentGeneratorFactory factory, which
	// SetAgentModelRuntimeFactory then leaves in place.
	customGeneratorFactory bool
	writeCompaction        func(context.Context, chatpkg.CompactionSummaryInput) (*common_service_models.ChatMessage, error)
	// planner resolves an agent's operation into one drive's generation plan — the model the
	// generator is built from, the exact catalog row the ledger stamps, and a retrier bound to the
	// same resolved route — at the start of every drive (SetAgentGenerationPlanner). It is
	// required: without it a drive has no model to run on.
	planner            commonai.GenerationPlanner
	checkOverrideActor func(context.Context, common_service_models.UserIdType) error
	// modelPolicyResolver is a compatibility config override applied only after a route-bound plan
	// has been validated; it cannot change that plan's provider/model identity.
	modelPolicyResolver AgentModelPolicyResolver
	// usageObserver is the app-injected per-attempt observer seam (SetAgentUsageObserver).
	// SetAgentModelRuntimeFactory's closure reads it at invocation time and threads it into the
	// default GenkitGenerator it builds; a custom generator installed via WithAgentGeneratorFactory
	// ignores it. nil reproduces the ledger-only generation behavior exactly.
	usageObserver commonai.GenerationUsageObserver
	// runObserver receives CAS-winning post-commit lifecycle transitions. The same callback is also
	// wired into the chat approval/recovery lane, whose workers can settle a run on another replica.
	runObserver ports.AgentRunObserver
	// settleRun finishes a terminal transition after commit (the chat handler's
	// SettleAgentRunAfterCommit): it publishes a delegated child's notice and drives every run the
	// ending minted. nil leaves settlement to persistence alone, which is complete but leaves a
	// minted run to the recovery sweep.
	settleRun chatpkg.AgentRunSettleHook
	// delegationState loads a parent's live delegation state for its prompt (the chat handler's
	// DelegationStateForSession). nil disables the delegation state block.
	delegationState func(context.Context, uint64) (*chatpkg.DelegationState, error)
	// publishRunNotice publishes MESSAGE_SENT for the generic SYSTEM_EVENT a terminal write inserted
	// (the chat handler's PublishAgentRunNoticeAfterCommit). nil leaves the notice to the next read.
	publishRunNotice func(context.Context, uint64)
	// publishDeliveredInput announces a step's delivery of queued input (the chat handler's
	// PublishDeliveredAgentInputAfterCommit): the messages it created, then the session's
	// interaction change. nil leaves both to the next read.
	publishDeliveredInput func(ctx context.Context, sessionID, channelID, parentMessageID uint64, messageIDs []uint64)
	// reconcileSuspendedRuns cancels the delegated child runs a parent's Stop left running (the
	// facade's reconcileSuspendedRuns), one scan page per read under the sweep's policy sample. The
	// recovery sweep starts it beside each tick's dispatch, one pass at a time; nil skips it.
	reconcileSuspendedRuns func(ctx context.Context, policy agentRecoveryPolicy)
	// writerFactory builds the run-scoped transcript writer. Production binds it to the dispatcher's
	// NewRunTranscriptWriter; tests inject a fake writer. nil means the runtime cannot persist.
	writerFactory     func(ctx context.Context, channel *common_service_models.ChatChannel, botUserID, triggeringUserID common_service_models.UserIdType) ports.AgentTranscriptWriter
	typingStarter     func(ctx context.Context, channel *common_service_models.ChatChannel, botUserID common_service_models.UserIdType) func()
	logger            *zap.Logger
	runBudgetProvider func() ports.AgentRunBudget
	// recoveryPolicyProvider samples the agent runtime keys: per drive (stream recovery attempts,
	// result recovery floor, drive lease, heartbeat, timeouts, and abandonment), per sweep tick (scan page, batch,
	// interval, database timeout), and per detached settlement or duration
	// write (settle timeout). The restart-bound worker capacities are read only when the recovery
	// sweep and the managed drive root start. nil uses the KMS defaults.
	recoveryPolicyProvider func() agentRecoveryPolicy
	// modelPolicyDefaultsProvider samples chat_agent_anthropic_default_max_output_tokens once per
	// generator construction, context-maintenance setup, or manifest fingerprint. nil uses the
	// KMS defaults.
	modelPolicyDefaultsProvider func() modelPolicyDefaults
	modelStepCeilingProvider    func() uint32

	// activeRuns avoids redundant drive attempts per run within this process: every nudge path (fresh turn,
	// approval resume, approved-work backstop) lands in runTurn on a detached goroutine, and two
	// concurrent drives of the same run would both read the same next step index and both Generate.
	// A losing nudge is safe to skip outright — the holder re-reads the durable transcript each
	// step, and a nudge only fires after the state it announces is committed. An entry lives exactly
	// as long as its driveTurn pass, including revocation retries unless shutdown interrupts them.
	// The durable drive lease (claim, heartbeat, token-checked writes, expiry) is the cross-instance
	// authority; this map is only the local scheduling guard.
	//
	// TODO(multi-instance-v2): after a takeover, the abandoned or expired owner's in-flight external
	// tool side effects can still happen, so a tool call may run more than once across instances.
	activeRunsMu sync.Mutex
	activeRuns   map[uint64]activeRunAttempt

	// recoveryDriver is a narrow test seam for the bounded recovery worker. nil uses driveTurn
	// with the recovery root's independent shutdown signal.
	recoveryDriver func(context.Context, uint64) error

	recoveryMu     sync.Mutex
	recoveryCtx    context.Context
	recoveryCancel context.CancelCauseFunc
	recoveryWG     sync.WaitGroup
	recoverySem    chan struct{}
	// reconcileRunning single-flights the sweep's suspended-run pass: a tick that finds the previous
	// pass still running (an application observer blocked inside it) skips the pass.
	reconcileRunning atomic.Bool

	// managed drives are fresh/resume entrypoint calls detached from request cancellation but rooted
	// in service lifecycle cancellation. Stop cancels and waits them; recovery attempts keep their
	// separately bounded scheduler lifecycle above.
	managedMu       sync.Mutex
	managedCtx      context.Context
	managedCancel   context.CancelCauseFunc
	managedStopping bool
	managedWG       sync.WaitGroup
	// managedAttemptSem bounds concurrent managed drives. Each pass keeps its slot through
	// abandonment cleanup and capacity waits; shutdown interrupts retries and capacity waits.
	managedAttemptSem chan struct{}
	// abandonedDriveSem (guarded by managedMu) bounds abandoned attempts whose calls still run, on
	// every drive path; each holds a slot until its call returns. See driveTurn.
	abandonedDriveSem chan struct{}
}

type activeRunAttempt struct {
	cancel context.CancelFunc
	// cancelRequestedAt is when cancelActiveRun first asked this attempt to stop (zero until then).
	// Releasing the attempt records the time since as the run's cancel-propagation latency.
	cancelRequestedAt time.Time
	// renudge is set when a drive request found the run busy; the release reports it so the driver
	// makes one more pass, because the busy drive may already be past the state that request saw.
	renudge bool
}

// tryAcquireRun marks a run as actively driven by this process. It returns false when another
// goroutine currently drives the run (the caller must skip; the busy attempt is flagged to drive
// again), and a release func otherwise. The release reports whether a request arrived meanwhile.
func (r *agentRuntime[U]) tryAcquireRun(runID uint64) (func() bool, bool) {
	r.activeRunsMu.Lock()
	defer r.activeRunsMu.Unlock()
	if r.activeRuns == nil {
		r.activeRuns = make(map[uint64]activeRunAttempt)
	}
	if active, busy := r.activeRuns[runID]; busy {
		active.renudge = true
		r.activeRuns[runID] = active
		return nil, false
	}
	r.activeRuns[runID] = activeRunAttempt{}
	return func() bool {
		r.activeRunsMu.Lock()
		active := r.activeRuns[runID]
		delete(r.activeRuns, runID)
		r.activeRunsMu.Unlock()
		if !active.cancelRequestedAt.IsZero() && r.persistence != nil {
			r.persistence.GetMetricsHandler().RecordChatAgentRunCancelPropagation(time.Since(active.cancelRequestedAt))
		}
		return active.renudge
	}, true
}

// bindActiveRunCancel attaches the concrete provider/tool drive cancellation function to the entry
// tryAcquireRun installed. Cancellation can win its durable CAS in the small window between those
// operations; cancelRequestedAt carries that race across the bind boundary. A missing entry means
// this attempt no longer owns the local guard and must stop too.
func (r *agentRuntime[U]) bindActiveRunCancel(runID uint64, cancel context.CancelFunc) bool {
	if r == nil || runID == 0 || cancel == nil {
		if cancel != nil {
			cancel()
		}
		return false
	}
	r.activeRunsMu.Lock()
	active, owned := r.activeRuns[runID]
	cancelNow := !owned
	if owned {
		active.cancel = cancel
		cancelNow = !active.cancelRequestedAt.IsZero()
		r.activeRuns[runID] = active
	}
	r.activeRunsMu.Unlock()
	if cancelNow {
		cancel()
	}
	return owned
}

// cancelActiveRun requests cancellation of the currently registered local drive. It marks the
// request even before the concrete cancel function is bound, closing the acquisition/CAS race.
func (r *agentRuntime[U]) cancelActiveRun(runID uint64) bool {
	if r == nil || runID == 0 {
		return false
	}
	r.activeRunsMu.Lock()
	active, ok := r.activeRuns[runID]
	if ok && active.cancelRequestedAt.IsZero() {
		active.cancelRequestedAt = time.Now()
		r.activeRuns[runID] = active
	}
	cancel := active.cancel
	r.activeRunsMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return ok
}

// cancelActiveRunAfterCommit ensures an ambient transaction cannot cancel provider work for a
// status change that later rolls back. Ordinary calls have no transaction and cancel immediately.
func (r *agentRuntime[U]) cancelActiveRunAfterCommit(ctx context.Context, runID uint64) {
	if r == nil || runID == 0 {
		return
	}
	if common_persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error {
		r.cancelActiveRun(runID)
		return nil
	}) {
		return
	}
	r.cancelActiveRun(runID)
}

func (r *agentRuntime[U]) isRunActive(runID uint64) bool {
	r.activeRunsMu.Lock()
	defer r.activeRunsMu.Unlock()
	_, ok := r.activeRuns[runID]
	return ok
}

// ensureAgentRuntime lazily constructs the runtime from the handler's wired seams. It is safe to call
// before the chat handler/dispatcher are fully initialized (the fields are read at RunAgentTurn time,
// not captured here), and idempotent so the generator-factory setters can run in any order.
func (h *CommonServiceHandler[U]) ensureAgentRuntime() {
	if h.agentRuntime != nil {
		return
	}
	h.agentRuntime = &agentRuntime[U]{}
}

// initializeAgentRuntime binds the runtime's seams after the chat handler, dispatcher, and registries
// exist. Called from initializeBotDispatcher (which wires the dispatcher + registries). It preserves
// any generator factory already injected via WithAgentGeneratorFactory/SetAgentModelRuntimeFactory.
func (h *CommonServiceHandler[U]) initializeAgentRuntime(logger *zap.Logger) {
	h.ensureAgentRuntime()
	rt := h.agentRuntime
	rt.checkOverrideActor = h.checkInferenceModelOverrideActor
	if h.chatHandler != nil {
		h.chatHandler.SetInferenceModelSelection(rt.planner, h)
		rt.writeCompaction = h.chatHandler.WriteCompactionSummary
		rt.settleRun = h.chatHandler.SettleAgentRunAfterCommit
		rt.delegationState = h.chatHandler.DelegationStateForSession
		rt.publishRunNotice = h.chatHandler.PublishAgentRunNoticeAfterCommit
		rt.publishDeliveredInput = h.chatHandler.PublishDeliveredAgentInputAfterCommit
	}
	rt.persistence = h.commonPersistenceHandler
	rt.converter = h.converter
	rt.reader = h.newTranscriptReader()
	rt.executor = h.newToolCallExecutor()
	rt.agentRegistry = h.agentRegistry
	rt.agentToolRegistry = h.agentToolRegistry
	if h.botDispatcher != nil {
		rt.writerFactory = h.botDispatcher.NewRunTranscriptWriter
		rt.typingStarter = h.botDispatcher.StartRunTypingIndicator
	}
	rt.eventPublisher = h.chatEventPublisher
	rt.logger = logger
	if h.runtimeConfig != nil {
		rt.modelStepCeilingProvider = func() uint32 { return h.runtimeConfig().InferenceMaxModelSteps() }
		rt.runBudgetProvider = func() ports.AgentRunBudget {
			view := h.runtimeConfig()
			return ports.AgentRunBudget{MaxActiveDuration: view.ChatAgentRunMaxActiveDuration(), MaxGenerations: uint64(view.ChatAgentRunMaxGenerations()), MaxToolCalls: uint64(view.ChatAgentRunMaxToolCalls())}
		}
	}
	rt.recoveryPolicyProvider = h.agentRecoveryPolicyProvider()
	rt.modelPolicyDefaultsProvider = h.modelPolicyDefaultsProvider()
	rt.reconcileSuspendedRuns = h.reconcileSuspendedRuns
}

// modelPolicyDefaultsFromView is one sample of the hot provider-fallback keys.
func modelPolicyDefaultsFromView(view config.ChatView) modelPolicyDefaults {
	return modelPolicyDefaults{anthropicMaxOutputTokens: int64(view.ChatAgentAnthropicDefaultMaxOutputTokens())}
}

func (r *agentRuntime[U]) modelStepCeiling() uint32 {
	if r.modelStepCeilingProvider != nil {
		return r.modelStepCeilingProvider()
	}
	return config.DefaultInferenceMaxModelSteps
}

// modelPolicyDefaultsProvider samples the hot provider-fallback keys from the live provider; nil
// without one, which leaves the runtime on the KMS defaults.
func (h *CommonServiceHandler[U]) modelPolicyDefaultsProvider() func() modelPolicyDefaults {
	if h.runtimeConfig == nil {
		return nil
	}
	return func() modelPolicyDefaults { return modelPolicyDefaultsFromView(h.runtimeConfig()) }
}

// modelPolicyDefaults returns the provider's sample as-is; without a provider (a runtime built
// without a config) it is the sample of the pkg/config defaults.
func (r *agentRuntime[U]) modelPolicyDefaults() modelPolicyDefaults {
	if r != nil && r.modelPolicyDefaultsProvider != nil {
		return r.modelPolicyDefaultsProvider()
	}
	return defaultModelPolicyDefaults()
}

// defaultModelPolicyDefaults is the sample of the pkg/config defaults.
var defaultModelPolicyDefaults = sync.OnceValue(func() modelPolicyDefaults {
	return modelPolicyDefaultsFromView(config.NewRootView(config.GetDefaults()))
})

// agentRecoveryPolicyFromView is one sample of the agent runtime keys.
func agentRecoveryPolicyFromView(view config.ChatView) agentRecoveryPolicy {
	return agentRecoveryPolicy{streamMaxAttempts: view.ChatAgentStreamRecoveryMaxAttempts(), scanPageSize: view.ChatAgentRunRecoveryScanPageSize(), settleTimeout: view.ChatAgentRunSettleTimeout(),
		resultRecoveryFloor: view.ChatAgentWorkflowResultRecoveryFloor(),
		driveTimeout:        view.ChatAgentRunDriveTimeout(), leaseDuration: view.ChatAgentRunDriveLeaseDuration(), heartbeatInterval: view.ChatAgentRunDriveHeartbeatInterval(),
		retryBackoff: view.ChatAgentRunDriveRetryBackoff(), dbTimeout: view.ChatAgentRunDriveDBTimeout(), scanInterval: view.ChatAgentRunRecoveryScanInterval(),
		batchSize: view.ChatAgentRunRecoveryBatchSize(), recoveryConcurrency: view.ChatAgentRunRecoveryConcurrency(),
		managedAttemptCapacity: view.ChatAgentRunManagedAttemptCapacity(), abandonedDriveCapacity: view.ChatAgentRunAbandonedDriveCapacity(),
		abandonGrace: view.ChatAgentRunDriveAbandonGrace(), maxAbandons: view.ChatAgentRunMaxDriveAbandonments(),
		workflowPollInterval: view.ChatAgentWorkflowWaitPollInterval(), graphClaimPollInterval: view.ChatAgentGraphDriveClaimPollInterval()}
}

// agentRecoveryPolicyProvider samples the hot agent recovery keys from the live provider; nil
// without one, which leaves the runtime on the KMS defaults.
func (h *CommonServiceHandler[U]) agentRecoveryPolicyProvider() func() agentRecoveryPolicy {
	if h.runtimeConfig == nil {
		return nil
	}
	return func() agentRecoveryPolicy { return agentRecoveryPolicyFromView(h.runtimeConfig()) }
}

// agentRecoveryPolicy is one sample of the agent runtime keys, passed explicitly.
type agentRecoveryPolicy struct {
	streamMaxAttempts int
	scanPageSize      int
	settleTimeout     time.Duration
	// resultRecoveryFloor is chat_agent_workflow_result_recovery_floor; see runClaimedTurn.
	resultRecoveryFloor time.Duration
	// driveTimeout is chat_agent_run_drive_timeout, the wall-clock budget of one managed or recovery drive.
	driveTimeout time.Duration
	// leaseDuration, heartbeatInterval and dbTimeout come from one sample per drive, so a drive's
	// claim and renewals keep the lease > heartbeat + timeout invariant validated together.
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	dbTimeout         time.Duration
	retryBackoff      time.Duration
	scanInterval      time.Duration
	batchSize         int
	// recoveryConcurrency, managedAttemptCapacity and abandonedDriveCapacity are restart-bound: they
	// size the semaphores once, when the recovery sweep and the managed drive root start.
	recoveryConcurrency    int
	managedAttemptCapacity int
	abandonedDriveCapacity int
	// abandonGrace and maxAbandons are chat_agent_run_drive_abandon_grace and
	// chat_agent_run_max_drive_abandonments; see driveTurn.
	abandonGrace time.Duration
	maxAbandons  int
	// workflowPollInterval is chat_agent_workflow_wait_poll_interval: DriveGraph's backstop before it
	// drives again a run whose last drive changed nothing.
	workflowPollInterval time.Duration
	// graphClaimPollInterval is chat_agent_graph_drive_claim_poll_interval: how often an idle
	// DriveGraph worker looks again for drivable runs while other drives of its graph are in flight.
	graphClaimPollInterval time.Duration
}

// defaultAgentRecoveryPolicy is the sample of the pkg/config defaults, for runtimes built without
// a config provider.
var defaultAgentRecoveryPolicy = sync.OnceValue(func() agentRecoveryPolicy {
	return agentRecoveryPolicyFromView(config.NewRootView(config.GetDefaults()))
})

// recoveryPolicy returns the provider's sample as-is: the configuration resolved its defaults and
// bounds when it was built. Without a provider (handlers built without a config) it is the KMS defaults.
func (r *agentRuntime[U]) recoveryPolicy() agentRecoveryPolicy {
	if r.recoveryPolicyProvider != nil {
		return r.recoveryPolicyProvider()
	}
	return defaultAgentRecoveryPolicy()
}

// publishRunTransitionAfterCommit reports a CAS-winning run-status transition only after the
// CAS-owning transaction commits: the realtime run-state event first, then the application observer.
// Runtime drives normally have no transaction, but honoring a hook-capable context keeps the public
// RunAgentTurn entrypoint correct for custom callers too.
func (r *agentRuntime[U]) publishRunTransitionAfterCommit(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	status proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
) {
	if r == nil || run == nil {
		return
	}
	chatpkg.PublishAgentRunTransitionAfterCommit[U](ctx, r.logger, agentRuntimeLoggerName, r.eventPublisher, r.runObserver, ports.AgentRunTransition{
		RunID: run.ID, ChannelID: run.ChannelID, ParentMessageID: run.ParentMessageID,
		Status: status, Reason: reason,
	}, r.settleRun)
}

// RunAgentTurn is the production entrypoint for the transcript-native agent loop. It drives a single
// ChatAgentRun from its next un-generated step until the run completes or pauses on an
// approval-required tool. It is called for both a FRESH turn (a session's initial user message
// created the run) and an APPROVAL RESUME (after a tool result was recorded and the run was nudged):
// the loop is identical, the only difference is the starting step it computes from the durable
// transcript via ai.NextStepIndex.
//
// It is server-side only; there is no public HTTP/proto surface. The caller (run starter / resume
// nudge) may supply a request context; the runtime preserves its values while replacing its
// cancellation with a bounded, service-owned lifecycle. Ambient database transactions and run-drive
// leases are control authority, not request values, and are stripped before the managed drive starts.
func (h *CommonServiceHandler[U]) RunAgentTurn(ctx context.Context, runID uint64) error {
	if h == nil || h.agentRuntime == nil {
		return commonai.ErrContinuationNotInitialized
	}
	return h.agentRuntime.runManagedTurn(common_persistence.WithoutRequestTransaction[U](ctx), runID)
}

func (r *agentRuntime[U]) runManagedTurn(triggerCtx context.Context, runID uint64) error {
	if r == nil {
		return commonai.ErrContinuationNotInitialized
	}
	policy := r.recoveryPolicy()
	r.managedMu.Lock()
	if r.managedStopping {
		r.managedMu.Unlock()
		return context.Canceled
	}
	if r.managedCtx == nil {
		r.managedCtx, r.managedCancel = context.WithCancelCause(context.Background())
	}
	if r.managedAttemptSem == nil {
		r.managedAttemptSem = make(chan struct{}, policy.managedAttemptCapacity)
	}
	root := r.managedCtx
	attemptSem := r.managedAttemptSem
	select {
	case attemptSem <- struct{}{}:
	default:
		r.managedMu.Unlock()
		return fmt.Errorf("agent runtime: managed drive capacity exhausted")
	}
	r.managedWG.Add(1)
	r.managedMu.Unlock()
	defer r.managedWG.Done()
	defer func() { <-attemptSem }()

	if triggerCtx == nil {
		triggerCtx = context.Background()
	}
	// The drive keeps the trigger's values but is cancelled only by its own timeout or the root,
	// whose cause (errAgentRuntimeStopping on Stop) the drive context inherits.
	lifecycleCtx, cancelLifecycle := context.WithCancelCause(context.WithoutCancel(triggerCtx))
	driveCtx, cancel := context.WithTimeout(lifecycleCtx, policy.driveTimeout)
	stopLifecycleCancel := context.AfterFunc(root, func() { cancelLifecycle(context.Cause(root)) })
	defer func() {
		stopLifecycleCancel()
		cancel()
		cancelLifecycle(nil)
	}()
	// Keep the root's shutdown signal independent of the drive deadline: once driveCtx times out,
	// its cancellation cause cannot change when Stop subsequently cancels the root.
	return r.driveTurn(driveCtx, runID, nil, root.Done(), 0)
}

func (r *agentRuntime[U]) startManagedDrives() {
	if r == nil {
		return
	}
	policy := r.recoveryPolicy()
	r.managedMu.Lock()
	defer r.managedMu.Unlock()
	if r.managedCtx != nil || r.managedStopping {
		return
	}
	r.managedCtx, r.managedCancel = context.WithCancelCause(context.Background())
	if r.managedAttemptSem == nil {
		r.managedAttemptSem = make(chan struct{}, policy.managedAttemptCapacity)
	}
	if r.abandonedDriveSem == nil {
		r.abandonedDriveSem = make(chan struct{}, policy.abandonedDriveCapacity)
	}
}

// abandonedDrives returns the abandoned-attempt semaphore, sized from policy on first use when
// the managed root has not started it (a workflow owner's drive in a runtime never started).
func (r *agentRuntime[U]) abandonedDrives(policy agentRecoveryPolicy) chan struct{} {
	r.managedMu.Lock()
	defer r.managedMu.Unlock()
	if r.abandonedDriveSem == nil {
		r.abandonedDriveSem = make(chan struct{}, policy.abandonedDriveCapacity)
	}
	return r.abandonedDriveSem
}

func (r *agentRuntime[U]) stopManagedDrives() {
	if r == nil {
		return
	}
	r.managedMu.Lock()
	// Stop is terminal for this handler lifecycle. A delayed post-commit callback must not lazily
	// recreate a new managed root after shutdown has returned.
	r.managedStopping = true
	cancel := r.managedCancel
	r.managedMu.Unlock()
	if cancel != nil {
		cancel(errAgentRuntimeStopping)
	}
	r.managedWG.Wait()
	r.managedMu.Lock()
	r.managedCtx = nil
	r.managedCancel = nil
	r.managedMu.Unlock()
}

// runTurn resolves a run's config + generator and drives the continuation loop, then settles the run
// status. A non-terminal run is required; a terminal/paused run is a no-op (returns nil) so a
// duplicate nudge or a race with the approval queue does not double-drive.
func (r *agentRuntime[U]) runTurn(ctx context.Context, runID uint64) error {
	return r.driveTurn(ctx, runID, nil, nil, 0)
}

// workflowToolRecovery settles a workflow-owned run's recoverable tool calls before its drive
// continues. Only the owner path (RunWorkflowAgentTurn) supplies one; managed drives pass nil.
type workflowToolRecovery func(context.Context) error

// driveTurn is runTurn with the owner's optional workflow tool recovery passed explicitly. Every
// drive path (managed, recovery, and workflow owner) runs here, so they share one abandonment
// protocol: each pass runs its claimed turn on a goroutine, and when ctx ends (deadline, Stop,
// cancellation, or a lost lease) and the call has not returned chat_agent_run_drive_abandon_grace
// later, the pass abandons the attempt (abandonDrive) instead of waiting for it. driveTurn therefore
// may also wait for bounded cleanup, revocation retries, or a full abandoned-attempt cap. shutdown
// is the service-owned root's signal, independent of ctx's deadline; nil leaves caller-owned drives
// unchanged. Shutdown interrupts retries and cap waits, but not an in-flight bounded cleanup write.
// graphShare bounds each attempt's active time to the drive's share of its graph budget pool (see
// runClaimedTurn); zero, which every drive but DriveGraph's passes, allows the whole remaining pool.
//
// Invariants:
//   - A1. At most one unrevoked attempt per run. The pass keeps its local registry entry until the
//     abandonment's revocation commits, except on shutdown when stopped heartbeats leave lease
//     expiry as the fallback. The durable lease fences every other claimer.
//   - A2. Revocation does not depend on the stuck call: once AbandonChatAgentRunDrive commits, every
//     token-checked write of the old attempt fails.
//   - A3. The driveAttempt state CAS gives active-time accounting to one owner: the attempt on
//     return or the supervisor on abandonment. Late completion cannot account it again; a failed
//     abandonment write during shutdown may leave that duration unrecorded.
//   - A4. A successful abandonment of a live run commits exactly one outcome: Retry, or Failed with
//     PROVIDER_TIMEOUT. A shutdown-caused abandonment (Stop, or DriveGraph suspending its graph) is
//     an immediate, uncounted retry, whatever the abandoned-attempt cap. Later
//     shutdown interrupts supervision without reclassifying that outcome; failed cleanup relies
//     on lease expiry instead of claiming an outcome was committed.
//   - A5. Drive goroutines never exceed the worker caps plus the abandoned-attempt cap: an abandoned
//     call holds an abandoned slot until it returns, or, when the cap is full, its pass waits for it
//     and keeps its worker slot (except on shutdown, which must not block Stop). A run has at most
//     maxAbandons+1 attempts outstanding at once.
//   - A6. Calls that return late still write their inference ledger rows: the continuation marks
//     them DriveCancelled from ctx.Err(), and the ledger write then skips the lease check.
func (r *agentRuntime[U]) driveTurn(ctx context.Context, runID uint64, recoverTools workflowToolRecovery, shutdown <-chan struct{}, graphShare time.Duration) error {
	if r == nil || r.persistence == nil {
		return commonai.ErrContinuationNotInitialized
	}
	var prevErr error
	for {
		renudge, err := r.driveTurnPass(ctx, runID, recoverTools, shutdown, graphShare)
		if err == nil {
			err = prevErr
		}
		// A request that found this drive busy may have arrived after the drive read the state it
		// acts on (for example while it deferred on an unresolved tool step), so drive once more.
		// If ctx is done the recovery sweep picks the run up instead.
		if !renudge || ctx.Err() != nil {
			return err
		}
		prevErr = err
	}
}

// driveAttempt is the fate of one claimed turn, decided once by CAS: live, then returned (by the
// attempt) or abandoned (by its pass). Whoever wins records the attempt's active time.
type driveAttempt struct {
	started time.Time
	state   atomic.Int32
	// abandonedSem is the abandoned-attempt semaphore; abandonedSlot is stored before the abandon
	// CAS, so an attempt that loses that CAS knows whether it must free a slot when its call returns.
	abandonedSem  chan struct{}
	abandonedSlot atomic.Bool
	// expired is closed, after expiredCause is set, when the attempt's own active deadline fires
	// (runClaimedTurn). The pass supervises it beside the drive context, so a call that ignores the
	// deadline is abandoned after the grace even when the caller's context has none.
	expired      chan struct{}
	expiredCause error
}

const (
	driveAttemptLive int32 = iota
	driveAttemptReturned
	driveAttemptAbandoned
)

// claimedTurnResult is runClaimedTurn's result, handed from the attempt goroutine to its pass.
type claimedTurnResult struct {
	retry bool
	err   error
}

// driveTurnPass is one acquire-claim-drive-release pass. renudge reports that a drive request
// found the run busy during the pass; an abandoned pass never reports it, because the committed
// abandonment outcome decides when the run is due again.
func (r *agentRuntime[U]) driveTurnPass(ctx context.Context, runID uint64, recoverTools workflowToolRecovery, shutdown <-chan struct{}, graphShare time.Duration) (renudge bool, err error) {
	release, acquired := r.tryAcquireRun(runID)
	if !acquired {
		// Another goroutine is already driving this run (e.g. the fast-path resume nudge racing
		// the approved-work backstop, or two sibling tool resolutions both passing the whole-step
		// gate). The durable run-row lease is the cross-replica authority; this map is only a local
		// contention optimization. The active drive re-reads the durable transcript each step.
		if appLogger.DebugEnabled(r.logger) {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Debug("agent runtime: run already being driven; skipping duplicate drive",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		return false, nil
	}
	abandoned := false
	defer func() {
		renudge = release() && !abandoned
	}()

	policy := r.recoveryPolicy() // sampled once per drive
	leaseOwner := uuid.New().String()
	claimDrive := r.persistence.ClaimChatAgentRunDrive
	if ports.WorkflowAgentRunFromContext(ctx) == runID {
		// Unlike the managed backstop, the owner must acquire waiting runs to
		// settle their tools. Ownership admission runs under the claimed drive budget.
		claimDrive = r.persistence.ClaimWorkflowAgentRunDrive
	}
	// The claim outlives ctx's cancellation, bounded by the database timeout: a cancelled UPDATE can
	// still commit, and a lease no drive knows it holds blocks every other drive until it expires. A
	// claim won after ctx was cancelled is released below like any other.
	claimCtx, cancelClaim := context.WithTimeout(context.WithoutCancel(ctx), policy.dbTimeout)
	claimed, err := claimDrive(claimCtx, runID, leaseOwner, policy.leaseDuration)
	cancelClaim()
	if err != nil {
		// A claim that errs may still have committed (its acknowledgment outran the timeout). The
		// release is scoped to leaseOwner, so it frees only a lease this claim won.
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), policy.dbTimeout)
		if _, releaseErr := r.persistence.ReleaseChatAgentRunDrive(releaseCtx, runID, leaseOwner, 0, proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED); releaseErr != nil {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to release a possibly committed drive claim",
				zap.String(logkeys.Stage, "release_agent_run_drive"),
				zap.String(logkeys.Fallback, "lease_expiry"),
				zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(releaseErr))
		}
		cancelRelease()
		return false, fmt.Errorf("agent runtime: claim run %d drive lease: %w", runID, err)
	}
	if !claimed {
		if appLogger.DebugEnabled(r.logger) {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Debug("agent runtime: durable run drive already owned or not due; skipping",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		return false, nil
	}

	driveBaseCtx, cancelDrive := context.WithCancel(ctx)
	r.bindActiveRunCancel(runID, cancelDrive)
	driveCtx := common_context.WithChatAgentRunDriveLease(driveBaseCtx, runID, leaseOwner)
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	leaseLost := make(chan error, 1)
	// Owned here and passed explicitly to the drive and its heartbeat; see runActivityState.
	activity := &runActivityState{}
	go r.heartbeatRunDrive(driveCtx, cancelDrive, runID, leaseOwner, policy, heartbeatStop, heartbeatDone, leaseLost, activity)
	stopHeartbeat := func() {
		close(heartbeatStop)
		<-heartbeatDone
	}

	attempt := &driveAttempt{started: time.Now(), abandonedSem: r.abandonedDrives(policy), expired: make(chan struct{})}
	done := make(chan claimedTurnResult, 1)
	go func() {
		retry, err := r.runClaimedTurn(driveCtx, runID, policy, activity, recoverTools, attempt, graphShare)
		if !attempt.state.CompareAndSwap(driveAttemptLive, driveAttemptReturned) &&
			attempt.state.Load() == driveAttemptAbandoned && attempt.abandonedSlot.Load() {
			<-attempt.abandonedSem
		}
		done <- claimedTurnResult{retry: retry, err: err}
	}()
	var result claimedTurnResult
	returned := false
	select {
	case result = <-done:
		returned = true
	case <-driveCtx.Done():
	case <-attempt.expired:
	}
	if !returned {
		// Cooperative calls unwind promptly and release the lease below. A call that ignores
		// cancellation or the drive's own deadline is abandoned after the grace instead of holding
		// this pass.
		grace := time.NewTimer(policy.abandonGrace)
		select {
		case result = <-done:
			grace.Stop()
		case <-grace.C:
			if r.abandonDrive(ctx, runID, leaseOwner, policy, attempt, done, stopHeartbeat, shutdown) {
				cancelDrive()
				abandoned = true
				select {
				case <-attempt.expired:
					// The deadline's cause (a spent budget, pool, or graph share) still names why the
					// drive ended, for an owner deciding what to drive next.
					return false, fmt.Errorf("%w: %w", errAgentRunDriveAbandoned, attempt.expiredCause)
				default:
					return false, errAgentRunDriveAbandoned
				}
			}
			// The attempt returned just as the grace ran out.
			result = <-done
		}
	}
	retry, driveErr := result.retry, result.err
	stopHeartbeat()
	cancelDrive()
	select {
	case lostErr := <-leaseLost:
		// Ownership is uncertain or known lost. Never clear the row: a recovery owner may already
		// hold it, and the expired token is the fencing evidence that stopped this drive's writes.
		if driveErr == nil || errors.Is(driveErr, context.Canceled) || errors.Is(driveErr, context.DeadlineExceeded) {
			driveErr = lostErr
		}
		return false, driveErr
	default:
	}

	retryAfter := time.Duration(0)
	if retry && !suspendedDrive(ctx) {
		// A suspended drive (Stop, or a graph drive's claim loss or Completed cleanup) failed
		// nothing: as with its abandonment, its release leaves the run due at once.
		retryAfter = policy.retryBackoff
	}
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), policy.dbTimeout)
	// ctx, not driveCtx: the drive context is already cancelled here, and only an interruption of the
	// caller (shutdown) should suppress the reason.
	_, releaseErr := r.persistence.ReleaseChatAgentRunDrive(releaseCtx, runID, leaseOwner, retryAfter, classifyDriveRetryReason(ctx, driveErr))
	cancelRelease()
	if releaseErr != nil {
		if driveErr == nil {
			return false, appLogger.WithErrorFields(releaseErr,
				zap.String(logkeys.Stage, "release_agent_run_drive"),
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to release durable run drive",
			zap.String(logkeys.Stage, "release_agent_run_drive"),
			zap.String(logkeys.Fallback, "preserve_primary_drive_error_and_lease_expiry"),
			zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(releaseErr))
	}
	return false, driveErr
}

// abandonDrive abandons an attempt whose call ignored cancellation past the abandon grace (see
// driveTurn). It reports false, changing nothing, when the attempt returned first. Otherwise it
// stops the heartbeat (lease expiry then backs the revocation up), commits the revocation and the
// run's outcome, retrying until that commits or another owner holds the run, publishes a failure,
// and, when the abandoned-attempt cap was full, waits for the call so the worker slot stays held.
// Shutdown, including one requested after a drive timeout, interrupts retries and cap waits. The
// first revocation is always attempted with its bounded database context; a failed cleanup leaves
// lease expiry as the fallback. Later shutdown never changes the attempt's original classification.
func (r *agentRuntime[U]) abandonDrive(
	ctx context.Context,
	runID uint64,
	leaseOwner string,
	policy agentRecoveryPolicy,
	attempt *driveAttempt,
	done <-chan claimedTurnResult,
	stopHeartbeat func(),
	shutdown <-chan struct{},
) bool {
	held := false
	select {
	case attempt.abandonedSem <- struct{}{}:
		held = true
	default:
	}
	attempt.abandonedSlot.Store(held)
	if !attempt.state.CompareAndSwap(driveAttemptLive, driveAttemptAbandoned) {
		if held {
			<-attempt.abandonedSem
		}
		return false
	}
	elapsed := time.Since(attempt.started)
	stopHeartbeat()
	abandonment := common_persistence.ChatAgentRunDriveAbandonment{
		Owner: leaseOwner, Elapsed: elapsed, Shutdown: suspendedDrive(ctx),
		RetryAfter: policy.retryBackoff, MaxAbandons: policy.maxAbandons, AtCap: !held,
	}
	stopping := func() bool {
		select {
		case <-shutdown:
			return true
		default:
			return abandonment.Shutdown
		}
	}
	logger := appLogger.For(ctx, r.logger, agentRuntimeLoggerName)
	var (
		outcome    common_persistence.ChatAgentRunDriveAbandonOutcome
		noticeID   uint64
		abandonErr error
	)
	for {
		abandonCtx, cancel := context.WithTimeout(context.Background(), policy.dbTimeout)
		outcome, noticeID, abandonErr = r.persistence.AbandonChatAgentRunDrive(abandonCtx, runID, abandonment)
		cancel()
		if abandonErr == nil {
			break
		}
		shutdownObserved := stopping()
		fallback := "retry_keeping_local_guard"
		if shutdownObserved {
			fallback = "leave_lease_for_expiry"
		}
		logger.Warn("agent runtime: failed to revoke abandoned drive",
			zap.String(logkeys.Stage, "abandon_agent_run_drive"),
			zap.String(logkeys.Fallback, fallback),
			zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(abandonErr))
		if shutdownObserved {
			break
		}
		timer := time.NewTimer(policy.retryBackoff)
		select {
		case <-timer.C:
		case <-shutdown:
		}
		timer.Stop()
		// Both channels may have become ready together. Never retry just because select chose
		// the timer when shutdown was also requested.
		if stopping() {
			break
		}
	}
	logger.Warn("agent runtime: drive call ignored cancellation; abandonment cleanup finished",
		zap.String(logkeys.Stage, "abandon_agent_run_drive"),
		zap.Uint64(appLogger.KeyAgentRunID, runID), appLogger.DurationMS(appLogger.KeyElapsedMS, elapsed),
		zap.Bool("shutdown", abandonment.Shutdown), zap.Bool("shutdown_observed", stopping()),
		zap.Bool("cleanup_succeeded", abandonErr == nil), zap.Bool("at_cap", abandonment.AtCap), zap.Int("outcome", int(outcome)))
	if abandonErr == nil && outcome == common_persistence.ChatAgentRunDriveAbandonFailed {
		publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policy.settleTimeout)
		run, err := r.persistence.GetChatAgentRunByID(publishCtx, runID)
		if err == nil && run != nil {
			r.publishRunTransitionAfterCommit(publishCtx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_TIMEOUT)
		} else {
			logger.Warn("agent runtime: failed to load abandoned run for its failure event",
				zap.String(logkeys.Stage, "publish_abandoned_agent_run_failure"),
				zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(err))
		}
		r.publishNoticeAfterCommit(publishCtx, noticeID)
		cancel()
	}
	if !held && !abandonment.Shutdown {
		select {
		case <-done:
		case <-shutdown:
			logger.Info("agent runtime: shutdown interrupted abandoned drive capacity wait",
				zap.String(logkeys.Stage, "abandon_agent_run_drive"),
				zap.Uint64(appLogger.KeyAgentRunID, runID),
				zap.Bool("shutdown", abandonment.Shutdown), zap.Bool("shutdown_observed", true))
		}
	}
	return true
}

// suspendedDrive reports whether ctx was cancelled to stop the service (Stop) or to suspend a
// graph (DriveGraph): an attempt abandoned under either is uncounted and leaves its run drivable,
// so neither ever fails a run. An abandonment at a drive's own deadline is still counted.
func suspendedDrive(ctx context.Context) bool {
	cause := context.Cause(ctx)
	return errors.Is(cause, errAgentRuntimeStopping) || errors.Is(cause, errGraphDriveSuspended)
}

// runClaimedTurn executes one drive after the cross-replica lease has been acquired and attached
// to ctx. The bool asks the outer lease release to schedule durable retry backoff. policy is the
// drive's sample of the runtime keys.
//
// In a graph with a budget pool the drive's active deadline is min(run remaining, graph share),
// where the share is graphShare capped by the pool's remaining time (the whole remaining pool when
// graphShare is zero). When the pool is used up, an interactive run fails with a GraphPool
// RunBudgetExceededError; an owner-driven run is left drivable and the drive returns the
// *GraphBudgetExhaustedError, for its owner to raise the pool or retire the graph. An owner-driven
// drive whose narrower share elapsed returns errGraphDriveShareElapsed, leaving the run drivable.
func (r *agentRuntime[U]) runClaimedTurn(ctx context.Context, runID uint64, policy agentRecoveryPolicy, activity *runActivityState, recoverTools workflowToolRecovery, attempt *driveAttempt, graphShare time.Duration) (bool, error) {

	if r.generatorFactory == nil {
		if appLogger.DebugEnabled(r.logger) {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Debug("agent runtime: no generator factory wired; deferring turn",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		return true, nil
	}

	// Loading, admission, crash recovery and generation are all active processing on one clock,
	// so a successful or retryable prefix can never renew the budget on the next drive.
	activeStarted := attempt.started
	run, err := r.persistence.GetChatAgentRunByID(ctx, runID)
	if err != nil || run == nil {
		return true, fmt.Errorf("agent runtime: load run %d: %w", runID, err)
	}
	// A WAITING run is claimable only so the stale-run scanner can reconcile a fully settled sealed
	// tool step after a crash between the last result commit and the live owner's aggregate gate.
	// Ordinary WAITING rows still have unresolved approval work and return without generation below.
	if !isDrivableRunStatus(run.Status) && run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL {
		if appLogger.DebugEnabled(r.logger) {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Debug("agent runtime: run not in a drivable status; skipping",
				zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Stringer(logkeys.Status, run.Status))
		}
		return false, nil
	}
	rejected := false
	defer func() {
		// An abandoned attempt's time was recorded with its revocation; never twice (driveTurn A3).
		// A drive turned away for not being the workflow owner did none of the run's work.
		if attempt.state.CompareAndSwap(driveAttemptLive, driveAttemptReturned) && !rejected {
			r.recordActiveDuration(ctx, runID, time.Since(activeStarted))
		}
	}()

	// Ownership is decided here, before the budget deadline and bounded by the drive's database
	// timeout, so the budget can never fail a run while a drive is still finding out whether it may
	// drive it. A drive the workflow owner did not start (the recovery sweep, a stray nudge) is turned
	// away without charging the run's active time. A failed lookup is a retryable admission failure
	// and is charged like any other prefix. The budget pool of a delegation graph (a delegating
	// root or a child) is read under the same bound.
	ownerDrive := ports.WorkflowAgentRunFromContext(ctx) == runID
	lookupCtx, cancelLookup := context.WithTimeout(ctx, policy.dbTimeout)
	agent, ownerCheck, hasAgent, err := r.runAgentDefinition(lookupCtx, run)
	var pool *common_persistence_models.ChatAgentGraphBudget
	if err == nil && hasAgent && (agent.IsDelegating() || ownerCheck.RootSessionID != ownerCheck.SessionID) {
		pool, err = r.persistence.GetChatAgentGraphBudget(lookupCtx, ownerCheck.RootSessionID)
	}
	cancelLookup()
	if err != nil {
		return true, err
	}
	workflowOwned := hasAgent && agent.IsWorkflowOwned()
	if !ownerDrive && workflowOwned {
		rejected = true
		return false, nil
	}

	var defaults ports.AgentRunBudget
	if r.runBudgetProvider != nil {
		defaults = r.runBudgetProvider()
	}
	budget := effectiveRunBudget(run, defaults, pool != nil)
	budget.streamRecoveryMaxAttempts = policy.streamMaxAttempts
	remaining := time.Duration(0)
	if run.ActiveDurationMs < budget.maxActiveDurationMs {
		remaining = time.Duration(budget.maxActiveDurationMs-run.ActiveDurationMs) * time.Millisecond
	}
	var budgetErr error = &commonai.RunBudgetExceededError{Kind: commonai.RunBudgetActiveDuration, Limit: budget.maxActiveDurationMs}
	if pool != nil {
		share, shareErr := pool.RemainingActive(), error(&common_persistence.GraphBudgetExhaustedError{Reason: proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION})
		if !workflowOwned {
			shareErr = graphPoolRunBudgetError(pool, proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION)
		}
		if graphShare > 0 && graphShare < share {
			share, shareErr = graphShare, errGraphDriveShareElapsed
		}
		// A pooled run's zero duration limit is no per-run limit: the pool bounds it.
		if budget.maxActiveDurationMs == 0 || share < remaining {
			remaining, budgetErr = share, shareErr
		}
	}
	// RunWorkflowAgentTurn marks every caller as the owner drive, so the marker names the entrypoint,
	// not the owner. The application's validator decides that under the same database bound: a stale
	// or revoked worker did none of the run's work, so it is turned away uncharged and can never let
	// a nearly spent budget fail its successor's run. An admitted owner's validation stays on the
	// run's clock, which started with the attempt.
	if workflowOwned {
		validateCtx, cancelValidate := context.WithTimeout(ctx, policy.dbTimeout)
		err := chatpkg.ValidateWorkflowGraphOwner(validateCtx, r.persistence, r.agentRegistry, agent, ownerCheck)
		cancelValidate()
		if err != nil {
			rejected = true
			return false, err
		}
	}
	// Only an owner-driven drive reaches a workflow agent's durable result check.
	hasAgent = hasAgent && ownerDrive
	// A workflow with durable result completion may need to repair a tool
	// acknowledgement after its model budget expired. Permit bounded database
	// admission/recovery; generation stays gated by the remaining budget.
	allowance := remaining
	if workflowOwned && agent.Workflow.ResultReady != nil && allowance < policy.resultRecoveryFloor {
		allowance = policy.resultRecoveryFloor
	}
	activeCtx, cancelActive := context.WithDeadlineCause(ctx, activeStarted.Add(allowance), budgetErr)
	defer cancelActive()
	defer context.AfterFunc(activeCtx, func() {
		if errors.Is(context.Cause(activeCtx), budgetErr) {
			attempt.expiredCause = budgetErr
			close(attempt.expired)
		}
	})()
	run, retry, runErr := r.admitAndDriveRun(activeCtx, run, agent, hasAgent, activeStarted.Add(remaining), budgetErr, budget, activity, recoverTools)
	if errors.Is(context.Cause(activeCtx), budgetErr) {
		// Our deadline fired, not the caller's: whatever the drive was doing, the run (or the graph,
		// or this drive's share of it) is spent.
		retry, runErr = false, budgetErr
	}
	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), policy.settleTimeout)
	defer cancelSettle()
	if errors.Is(runErr, errAgentRunNoLongerDrivable) {
		return false, nil
	}
	if exhausted, ok := errors.AsType[*common_persistence.GraphBudgetExhaustedError](runErr); ok {
		if workflowOwned {
			// The owner's step fails with the budget reason and the run stays drivable, so a raised
			// pool lets the next drive continue it.
			return false, exhausted
		}
		retry, runErr = false, graphPoolRunBudgetError(pool, exhausted.Reason)
	}
	if errors.Is(runErr, errGraphDriveShareElapsed) {
		return false, runErr
	}
	if ownerDrive && ctx.Err() != nil {
		return false, ctx.Err()
	}
	if runErr != nil {
		r.settleContinuationError(settleCtx, run, runErr)
		fields := []zap.Field{
			zap.String(logkeys.Operation, "drive_agent_continuation"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
		}
		if run.OrganizationID > 0 {
			fields = append(fields, zap.Int64(appLogger.KeyOrganizationID, run.OrganizationID))
		}
		return retry, appLogger.WithErrorFields(runErr, fields...)
	}
	return retry, nil
}

// admitAndDriveRun is the budgeted body of runClaimedTurn: non-owner admission, crash recovery,
// the durable result check, and generation. ctx carries the drive's active deadline (raised to the
// recovery floor for owners with durable results); generation itself never runs past generateUntil.
// The returned run is the latest loaded row, which runClaimedTurn settles against. Settlement is
// owned by runClaimedTurn so every exit path observes deadline expiry consistently.
func (r *agentRuntime[U]) admitAndDriveRun(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	agent ports.AgentDefinition,
	hasAgent bool,
	generateUntil time.Time,
	budgetErr error,
	budget effectiveAgentRunBudget,
	activity *runActivityState,
	recoverTools workflowToolRecovery,
) (*common_persistence_models.ChatAgentRun, bool, error) {
	runID := run.ID
	// runClaimedTurn already validated a workflow owner before the budget clock; every other drive is
	// admitted here.
	if !hasAgent || !agent.IsWorkflowOwned() {
		if _, err := r.admitWorkflow(ctx, runID); err != nil {
			if errors.Is(err, ErrWorkflowAgentRequiresOwner) {
				return run, false, nil
			}
			return run, true, err
		}
	}
	startStep, incomplete, err := r.recoveryStartStep(ctx, run)
	if err != nil {
		return run, true, err
	}
	if incomplete && ports.WorkflowAgentRunFromContext(ctx) == runID {
		if _, err := r.persistence.ResetIncompleteWorkflowAgentRunDrive(ctx, runID); err != nil {
			return run, false, err
		}
		startStep, incomplete, err = r.recoveryStartStep(ctx, run)
		if err != nil {
			return run, true, err
		}
	} else if incomplete {
		// A crash, deploy, or timeout can leave the step after the last seal streaming. Discard it
		// under the stream-recovery allowance and generate the step again. Anything else (tool
		// rows, a pinned root, an exhausted allowance) still fails the run as an incomplete step.
		recovery, recoverErr := r.persistence.RecoverInterruptedChatAgentRunStep(ctx, runID, budget.streamRecoveryMaxAttempts)
		switch {
		case recoverErr == nil:
			r.publishDiscardedStream(ctx, run, recovery.Discarded)
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Info("recovering agent generation interrupted before this drive",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Uint32("stream_recovery_attempt", recovery.Attempt))
			startStep, incomplete, err = r.recoveryStartStep(ctx, run)
			if err != nil {
				return run, true, err
			}
		case errors.Is(recoverErr, common_persistence.ErrChatAgentRunDriveLeaseLost):
			return run, false, recoverErr
		case errors.Is(recoverErr, common_persistence.ErrChatAgentRunStepSealInvalid),
			errors.Is(recoverErr, common_persistence.ErrChatMessageAgentSessionRootPinned),
			errors.Is(recoverErr, common_persistence.ErrAgentStreamRecoveryExhausted):
		default:
			return run, true, recoverErr
		}
	}
	if incomplete {
		failed, failErr := r.persistence.FailIncompleteChatAgentRunDrive(ctx, run.ID)
		if failErr != nil {
			return run, !errors.Is(failErr, common_persistence.ErrChatAgentRunDriveLeaseLost), failErr
		}
		if failed {
			r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INCOMPLETE_STEP)
		}
		return run, false, errIncompleteAgentRunStep
	}
	if ports.WorkflowAgentRunFromContext(ctx) == runID {
		if recoverTools != nil {
			if err := recoverTools(ctx); err != nil {
				return run, true, err
			}
			reloaded, err := r.persistence.GetChatAgentRunByID(ctx, runID)
			if err != nil {
				return run, true, err
			}
			if reloaded == nil {
				return run, false, common_persistence.ErrChatAgentRunNotFound
			}
			run = reloaded
			if !isDrivableRunStatus(run.Status) && run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL {
				return run, false, nil
			}
		}
	}
	// A committed result is authoritative even when a crash interrupted the
	// final tool acknowledgement. Recover tools first, then avoid routing or
	// spending another generation (including an already exhausted budget).
	if hasAgent {
		if resultReady := r.workflowResultReadyCallback(run.ID, agent); resultReady != nil {
			ready, readyErr := resultReady(ctx)
			if readyErr != nil {
				return run, true, readyErr
			}
			if ready {
				r.completeRun(ctx, run)
				return run, false, nil
			}
		}
	}
	if !time.Now().Before(generateUntil) {
		return run, false, budgetErr
	}
	activeCtx, cancelActive := context.WithDeadlineCause(ctx, generateUntil, budgetErr)
	defer cancelActive()
	retry, runErr := r.driveActiveRun(activeCtx, run, startStep, budget, activity)
	if errors.Is(context.Cause(activeCtx), budgetErr) {
		runErr = budgetErr
	}
	return run, retry, runErr
}

// driveActiveRun performs the work covered by the active-duration budget. Settlement is owned by
// runTurn so every exit path observes deadline expiry consistently.
func (r *agentRuntime[U]) driveActiveRun(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	startStep uint32,
	budget effectiveAgentRunBudget,
	activity *runActivityState,
) (bool, error) {
	stopTyping := r.startTyping(ctx, run)
	defer stopTyping()
	// Whatever path leaves this drive, nothing it was doing is still happening. Only an activity
	// still in force is cleared: see runActivityState.clear.
	defer func() { r.publishRunActivity(ctx, activity.clear()) }()

	prepared, err := r.prepareRun(ctx, run)
	if err != nil {
		return shouldRetryAgentRunDrive(err), err
	}
	if startStep > 0 && run.LastSealedStepIndex != nil &&
		common_persistence.ChatAgentRunStepDisposition(run.LastSealedStepDisposition) == common_persistence.ChatAgentRunStepDispositionToolLoop {
		resolution, reconcileErr := r.persistence.ReconcileSealedChatAgentRunToolStep(
			ctx, run.ID, *run.LastSealedStepIndex, prepared.resumeOnDenial,
		)
		if reconcileErr != nil {
			if errors.Is(reconcileErr, common_persistence.ErrChatAgentRunStepSealInvalid) {
				return r.failSealedStepCorruption(ctx, run, reconcileErr)
			}
			return shouldRetryAgentRunDrive(reconcileErr), reconcileErr
		}
		r.publishReconciledToolStepTransition(ctx, run, resolution)
		switch resolution {
		case common_persistence.ChatAgentRunToolStepResolutionWaiting:
			// Not a failure: the tool lane's nudge (or the sweep) wakes the run, and a retry delay
			// would make that nudge's claim refuse.
			return false, nil
		case common_persistence.ChatAgentRunToolStepResolutionCompleted:
			return false, nil
		case common_persistence.ChatAgentRunToolStepResolutionFailed:
			return false, errFailedAgentRunToolStep
		case common_persistence.ChatAgentRunToolStepResolutionContinue:
			// The aggregate transition made a recovered WAITING/RETRYABLE run RUNNING. It is now
			// safe to generate exactly the step after this seal.
		default:
			return r.failSealedStepCorruption(ctx, run, common_persistence.ErrChatAgentRunStepSealInvalid)
		}
	}
	if startStep > 0 {
		unresolved, err := r.hasUnresolvedToolCallsInStep(ctx, run.ID, startStep-1)
		if err != nil {
			return true, err
		}
		if unresolved {
			if appLogger.DebugEnabled(r.logger) {
				appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Debug("agent runtime: previous step still has unresolved tool calls; deferring generation",
					zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Uint32(appLogger.KeyAgentStepIndex, startStep-1))
			}
			// Waiting on the tool lane, not failing: no retry delay, so its nudge can claim the run.
			return false, nil
		}
	}

	prepared.cfg.SealStep = r.stepSealer(run, budget.maxGenerations, prepared.admitInput)
	for {
		runner := commonai.NewContinuationRunner(
			r.reader,
			prepared.generator,
			prepared.writer,
			r.executor,
			prepared.inference,
			commonai.WithContinuationLogger(r.logger),
			commonai.WithContinuationBudget(&persistentContinuationBudget{
				persistence:    r.persistence,
				logger:         r.logger,
				runID:          run.ID,
				maxGenerations: budget.maxGenerations,
				maxToolCalls:   budget.maxToolCalls,
				admitInput:     prepared.admitInput,
				onAdmitted: func(admitCtx context.Context, delivery common_persistence.ChatAgentQueuedInputDelivery) {
					// The delivery is committed: the session's queue just changed, and the input it
					// delivered is new messages at the session's location.
					if r.publishDeliveredInput != nil {
						r.publishDeliveredInput(admitCtx, common_util.Deref(run.AgentSessionID), run.ChannelID, run.ParentMessageID, delivery.MessageIDs())
					}
				},
			}),
		)
		prepared.cfg.Activity = func(activityCtx context.Context, reported commonai.RunActivity) {
			r.reportRunActivity(activityCtx, activity, run, reported)
		}
		result, runErr := runner.Run(ctx, prepared.cfg, startStep)
		if runErr != nil {
			var interrupted *commonai.RecoverableGenerationError
			if errors.As(runErr, &interrupted) && ctx.Err() == nil {
				recovery, err := r.persistence.PrepareChatAgentStreamRecovery(ctx, run.ID, interrupted.StepIndex, budget.streamRecoveryMaxAttempts)
				if err != nil {
					return false, fmt.Errorf("%w: prepare stream recovery: %w", commonai.ErrPartialStreamFailed, err)
				}
				r.publishDiscardedStream(ctx, run, recovery.Discarded)
				appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Info("recovering interrupted agent generation",
					zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Uint32(appLogger.KeyAgentStepIndex, interrupted.StepIndex),
					zap.Uint32("stream_recovery_attempt", recovery.Attempt), zap.Error(interrupted.Err))
				r.reportRunActivity(ctx, activity, run, commonai.RunActivity{
					Kind: proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_WAITING_TO_RETRY, StartedAt: time.Now(),
				})
				timer := time.NewTimer(r.recoveryPolicy().retryBackoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return true, ctx.Err()
				case <-timer.C:
				}
				r.reportRunActivity(ctx, activity, run, commonai.RunActivity{})
				prepared.writer, err = r.buildRunWriter(ctx, run)
				if err != nil {
					return shouldRetryAgentRunDrive(err), err
				}
				startStep = interrupted.StepIndex
				continue
			}
			// A claimed inline result can be atomically durable even when the commit acknowledgement is
			// lost. The continuation reports that as an InlineToolExecutionError, but its token-guarded
			// failure settlement then no-ops against EXECUTED. Detect that exact durable marker and keep
			// this already-active drive moving from the next step. This is the live-inline equivalent of
			// the approval scanner's persisted whole-step completion gate.
			if recoveredStep, recovered := r.durablyCommittedInlineFailureStep(ctx, run.ID, runErr); recovered {
				unresolved, countErr := r.hasUnresolvedToolCallsInStep(ctx, run.ID, recoveredStep)
				if countErr != nil {
					return true, countErr
				}
				if unresolved {
					// A parallel approval/recovery sibling still owns completion and will nudge this run when
					// the aggregate step becomes fully paired. No retry delay, which would refuse that nudge.
					return false, nil
				}
				startStep = recoveredStep + 1
				continue
			}
			return shouldRetryAgentRunDrive(runErr), runErr
		}

		// A TOOL_LOOP seal atomically selected WAITING when the full persisted batch contained an
		// approval card. Reconcile after inline execution because the approval may have been resolved
		// concurrently while a sibling tool was running; the last result must not leave a zero-work
		// WAITING run stranded.
		if result != nil && result.WaitingForApproval {
			if result.Steps <= 0 {
				return r.failSealedStepCorruption(ctx, run, common_persistence.ErrChatAgentRunStepSealInvalid)
			}
			settledStep := startStep + uint32(result.Steps) - 1
			resolution, reconcileErr := r.persistence.ReconcileSealedChatAgentRunToolStep(
				ctx, run.ID, settledStep, prepared.resumeOnDenial,
			)
			if reconcileErr != nil {
				if errors.Is(reconcileErr, common_persistence.ErrChatAgentRunStepSealInvalid) {
					return r.failSealedStepCorruption(ctx, run, reconcileErr)
				}
				return shouldRetryAgentRunDrive(reconcileErr), reconcileErr
			}
			r.publishReconciledToolStepTransition(ctx, run, resolution)
			switch resolution {
			case common_persistence.ChatAgentRunToolStepResolutionWaiting:
				return false, nil
			case common_persistence.ChatAgentRunToolStepResolutionCompleted:
				return false, nil
			case common_persistence.ChatAgentRunToolStepResolutionFailed:
				return false, errFailedAgentRunToolStep
			case common_persistence.ChatAgentRunToolStepResolutionContinue:
				startStep = settledStep + 1
				continue
			default:
				return r.failSealedStepCorruption(ctx, run, common_persistence.ErrChatAgentRunStepSealInvalid)
			}
		}

		// A FINAL seal already completed the run atomically, and this CAS is then a no-op. It is the
		// real transition (with its terminal settlement and drain) for a steered run whose waiting
		// input was gone at its next step, a ResultReady completion, and custom runners without a
		// sealer.
		if result != nil && result.Completed {
			r.completeRun(ctx, run)
		}
		return false, nil
	}
}

// failSealedStepCorruption turns a deterministic seal/shape mismatch into one fenced terminal
// transition with durable notice. Retrying cannot repair a batch whose seal no longer attests its
// rows, and repeated recovery would otherwise hot-loop forever.
func (r *agentRuntime[U]) failSealedStepCorruption(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause error) (bool, error) {
	if cause == nil {
		cause = common_persistence.ErrChatAgentRunStepSealInvalid
	}
	failed, noticeID, failErr := r.persistence.FailChatAgentRunWithNotice(ctx, run.ID,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INTERNAL)
	if failErr != nil {
		return !errors.Is(failErr, common_persistence.ErrChatAgentRunDriveLeaseLost), errors.Join(cause, failErr)
	}
	if failed {
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INTERNAL)
		r.publishNoticeAfterCommit(ctx, noticeID)
	}
	return false, cause
}

// publishReconciledToolStepTransition reports the transition ReconcileSealedChatAgentRunToolStep
// made, with the reason persistence derived for it.
func (r *agentRuntime[U]) publishReconciledToolStepTransition(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	resolution common_persistence.ChatAgentRunToolStepResolution,
) {
	switch resolution {
	case common_persistence.ChatAgentRunToolStepResolutionCompleted:
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED)
	case common_persistence.ChatAgentRunToolStepResolutionFailed:
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED)
	case common_persistence.ChatAgentRunToolStepResolutionContinue:
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED)
	}
}

// publishDiscardedStream announces the stream rows a committed recovery discarded.
func (r *agentRuntime[U]) publishDiscardedStream(ctx context.Context, run *common_persistence_models.ChatAgentRun, discarded []common_persistence_models.ChatMessage) {
	if r.eventPublisher == nil {
		return
	}
	for i := range discarded {
		message := r.converter.PersistenceChatMessageToService(&discarded[i])
		if err := ports.PublishChatEventAfterCommit(ctx, r.eventPublisher, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:       proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_DELETED,
				OccurredAt: time.Now(), ActorUserID: common_service_models.UserIdType(run.BotUserID), Message: message,
			}
		}); err != nil {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("failed to publish discarded stream", zap.Error(err))
		}
	}
}

func (r *agentRuntime[U]) recoveryStartStep(ctx context.Context, run *common_persistence_models.ChatAgentRun) (uint32, bool, error) {
	if run == nil {
		return 0, false, common_persistence.ErrChatAgentRunNotFound
	}
	artifacts, err := r.persistence.GetChatAgentRunStepArtifactState(ctx, run.ID)
	if err != nil {
		return 0, false, fmt.Errorf("agent runtime: inspect durable step state for run %d: %w", run.ID, err)
	}
	if artifacts == nil {
		return 0, false, fmt.Errorf("agent runtime: inspect durable step state for run %d returned nil", run.ID)
	}
	if !artifacts.HasArtifacts {
		return 0, run.LastSealedStepIndex != nil, nil
	}
	if run.LastSealedStepIndex == nil || artifacts.HasIncompleteStream || artifacts.MaxStepIndex != *run.LastSealedStepIndex {
		return 0, true, nil
	}
	if !common_persistence.ChatAgentRunStepDisposition(run.LastSealedStepDisposition).DrivableAfterSeal() {
		// A final seal atomically completes the run. Seeing it (or an unknown disposition) in a
		// drivable run is corruption or an interrupted old implementation and must not cause
		// another model call.
		return 0, true, nil
	}
	if *run.LastSealedStepIndex == ^uint32(0) {
		return 0, true, nil
	}
	return *run.LastSealedStepIndex + 1, false, nil
}

func shouldRetryAgentRunDrive(err error) bool {
	if err == nil || errors.Is(err, errIncompleteAgentRunStep) || errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost) {
		return false
	}
	if _, blocked := errors.AsType[*inference.BlockError](err); blocked {
		return false
	}
	if isTerminalRunCause(err) || errors.Is(err, commonai.ErrAgentStepSealFailed) {
		return false
	}
	if inlineErr, ok := errors.AsType[*commonai.InlineToolExecutionError](err); ok {
		return inlineErr.Retryable || errors.Is(inlineErr.Err, context.Canceled) || errors.Is(inlineErr.Err, context.DeadlineExceeded)
	}
	if batchErr, ok := errors.AsType[*commonai.InlineToolExecutionBatchError](err); ok {
		for _, failure := range batchErr.Failures {
			if failure != nil && (failure.Retryable || errors.Is(failure.Err, context.Canceled) || errors.Is(failure.Err, context.DeadlineExceeded)) {
				return true
			}
		}
		return false
	}
	return true
}

func (r *agentRuntime[U]) heartbeatRunDrive(
	ctx context.Context,
	cancelDrive context.CancelFunc,
	runID uint64,
	leaseOwner string,
	policy agentRecoveryPolicy,
	stop <-chan struct{},
	done chan<- struct{},
	lost chan<- error,
	activity *runActivityState,
) {
	defer close(done)
	ticker := time.NewTicker(policy.heartbeatInterval)
	defer ticker.Stop()
	// lastRenewed is when the newest renewal that is known to have succeeded began, on the local
	// monotonic clock; the claim happened just before this goroutine started.
	lastRenewed := time.Now()
	// An owner drive renews under its claim's retirement fence, which its workflow marker selects.
	renewBase := context.Background()
	if ports.WorkflowAgentRunFromContext(ctx) == runID {
		renewBase = ports.WithWorkflowAgentRun(renewBase, runID)
	}
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			attemptStart := time.Now()
			renewCtx, cancel := context.WithTimeout(renewBase, policy.dbTimeout)
			renewed, err := r.persistence.RenewChatAgentRunDrive(renewCtx, runID, leaseOwner, policy.leaseDuration)
			if err == nil && !renewed {
				// Terminal runs intentionally reject heartbeat renewal. That is revocation of future
				// write authority, but not a drive-lease error: the transition may have been committed
				// by this very drive immediately before a slow post-commit observer. Cancel any
				// remaining cooperative work (also covering cancellation on another replica) without
				// poisoning an otherwise successful terminal drive with ErrDriveLeaseLost.
				run, readErr := r.persistence.GetChatAgentRunByID(renewCtx, runID)
				if readErr == nil && run != nil && common_persistence.IsTerminalChatAgentRunStatus(run.Status) {
					cancel()
					cancelDrive()
					return
				}
			}
			cancel()
			if err != nil && time.Since(lastRenewed) < policy.leaseDuration-policy.dbTimeout {
				// A transient database error says nothing about ownership. Retry on the next beat
				// until the lease may genuinely have lapsed (the config check guarantees a retry fits).
				appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: drive lease renewal failed; retrying on next heartbeat",
					zap.String(logkeys.Stage, "renew_agent_run_drive"),
					zap.String(logkeys.Fallback, "retry_next_heartbeat"),
					zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(err))
				continue
			}
			if err == nil && renewed {
				lastRenewed = attemptStart
				// Repeat the activity in force on the same beat that proves supervision. A long model
				// wait emits no other event, so this is how a late-joining client learns about it.
				r.publishRunActivity(ctx, activity.snapshot())
				continue
			}
			if err == nil {
				err = common_persistence.ErrChatAgentRunDriveLeaseLost
			} else {
				err = fmt.Errorf("%w: renew: %w", common_persistence.ErrChatAgentRunDriveLeaseLost, err)
			}
			select {
			case lost <- err:
			default:
			}
			cancelDrive()
			return
		}
	}
}

// startRecovery starts the dedicated, default-on stale-run backstop. It is intentionally
// independent of the approval scanner configuration: it recovers a lost initial post-commit hook,
// a crashed provider call, and a completed tool result whose process died before the resume nudge.
func (r *agentRuntime[U]) startRecovery() {
	if r == nil || r.persistence == nil {
		return
	}
	r.startManagedDrives()
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	if r.recoveryCtx != nil {
		return
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	r.recoveryCtx = ctx
	r.recoveryCancel = cancel
	// The recovery concurrency is restart-bound: the worker semaphore is sized once here.
	r.recoverySem = make(chan struct{}, r.recoveryPolicy().recoveryConcurrency)
	r.recoveryWG.Add(1)
	go r.recoveryLoop(ctx)
}

// recoveryLoop sweeps once at start, then every chat_agent_run_recovery_scan_interval; a changed
// interval reschedules the next sweep.
func (r *agentRuntime[U]) recoveryLoop(ctx context.Context) {
	defer r.recoveryWG.Done()
	r.runRecoverySweep(ctx)
	hotpoll.Run(ctx, func() time.Duration { return r.recoveryPolicy().scanInterval }, func() { r.runRecoverySweep(ctx) })
}

func (r *agentRuntime[U]) runRecoverySweep(ctx context.Context) {
	if r == nil || r.persistence == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	policy := r.recoveryPolicy() // sampled once per sweep tick
	scanPageSize := policy.scanPageSize
	// The suspended-run pass finishes the retirements and child cancellations a crash or a stalled
	// worker left undone. It runs beside dispatch rather than on this loop, so a pass held up by a slow
	// callback never delays this or a later tick's dispatch; none of its runs can be drive-claimed.
	r.startSuspendedRunReconciliation(ctx, policy)
	dispatched := 0
	var afterID uint64
	for {
		readCtx, cancel := context.WithTimeout(ctx, policy.dbTimeout)
		runIDs, err := r.persistence.ListRecoverableChatAgentRunsAfterID(readCtx, afterID, scanPageSize)
		cancel()
		if err != nil {
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: stale-run recovery scan failed",
				zap.String(logkeys.Stage, "scan_recoverable_agent_runs"),
				zap.String(logkeys.Fallback, "retry_on_next_scan"),
				zap.Error(err))
			return
		}
		for _, runID := range runIDs {
			if runID > afterID {
				afterID = runID
			}
			if runID == 0 {
				continue
			}
			if r.isRunActive(runID) {
				// This is only a local scheduling fact. Never push drive_not_before forward:
				// another replica must remain free to claim the already-expired durable lease.
				continue
			}
			if dispatched >= policy.batchSize {
				return
			}
			if runID == 0 {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case r.recoverySem <- struct{}{}:
				dispatched++
				r.recoveryWG.Add(1)
				go func(id uint64) {
					defer r.recoveryWG.Done()
					defer func() { <-r.recoverySem }()
					shutdown := ctx.Done()
					driveCtx, cancelDrive := context.WithTimeout(ctx, policy.driveTimeout)
					defer cancelDrive()
					drive := r.recoveryDriver
					if drive == nil {
						drive = func(driveCtx context.Context, id uint64) error {
							return r.driveTurn(driveCtx, id, nil, shutdown, 0)
						}
					}
					// Keep this worker through abandonment cleanup and capacity waits; the root's
					// shutdown signal interrupts retries and capacity waits (see driveTurn).
					if err := drive(driveCtx, id); err != nil && !errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost) && !errors.Is(err, context.Canceled) {
						appLogger.LogOperationWarn(appLogger.For(ctx, r.logger, agentRuntimeLoggerName), "Agent runtime recovery drive failed", "recover_agent_run", err,
							zap.String(logkeys.Fallback, "leave_run_for_later_recovery"),
							zap.Uint64(appLogger.KeyAgentRunID, id))
					}
				}(runID)
			default:
				// Every worker slot is taken. Stop paging: the remaining due runs are re-listed by the
				// next tick, and reading further pages now would only skip them.
				return
			}
		}
		if len(runIDs) < scanPageSize {
			return
		}
	}
}

// startSuspendedRunReconciliation runs one suspended-run pass in the background, joined by
// stopRecovery through recoveryWG. At most one pass runs at a time: a tick that finds the previous
// pass still running skips it, so a pass held up by a slow callback never stacks more behind it.
func (r *agentRuntime[U]) startSuspendedRunReconciliation(ctx context.Context, policy agentRecoveryPolicy) {
	if r.reconcileSuspendedRuns == nil || !r.reconcileRunning.CompareAndSwap(false, true) {
		return
	}
	r.recoveryWG.Go(func() {
		defer r.reconcileRunning.Store(false)
		r.reconcileSuspendedRuns(ctx, policy)
	})
}

func (r *agentRuntime[U]) stopRecovery() {
	if r == nil {
		return
	}
	r.recoveryMu.Lock()
	if r.recoveryCtx == nil {
		r.recoveryMu.Unlock()
		return
	}
	cancel := r.recoveryCancel
	r.recoveryMu.Unlock()
	cancel(errAgentRuntimeStopping)
	r.recoveryWG.Wait()
	r.recoveryMu.Lock()
	r.recoveryCtx = nil
	r.recoveryCancel = nil
	r.recoverySem = nil
	r.recoveryMu.Unlock()
}

type effectiveAgentRunBudget struct {
	maxActiveDurationMs uint64
	maxGenerations      uint64
	maxToolCalls        uint64
	// streamRecoveryMaxAttempts is this drive's chat_agent_stream_recovery_max_attempts sample.
	streamRecoveryMaxAttempts int
}

// effectiveRunBudget resolves the drive's per-run limits: the run's captured limits narrowed by the
// current library ceilings (current). A run of a graph with a budget pool (pooled) takes its
// captured limits alone: the pool replaces the library ceilings, and a zero limit is none (for a
// count, the bigint maximum the reservation takes).
func effectiveRunBudget(run *common_persistence_models.ChatAgentRun, current ports.AgentRunBudget, pooled bool) effectiveAgentRunBudget {
	if pooled && run != nil {
		return effectiveAgentRunBudget{
			maxActiveDurationMs: run.MaxActiveDurationMs,
			maxGenerations:      minNonZero(run.MaxGenerations, math.MaxInt64),
			maxToolCalls:        minNonZero(run.MaxToolCalls, math.MaxInt64),
		}
	}
	currentDurationMs := uint64(current.MaxActiveDuration.Milliseconds())
	if run == nil {
		return effectiveAgentRunBudget{
			maxActiveDurationMs: currentDurationMs,
			maxGenerations:      current.MaxGenerations,
			maxToolCalls:        current.MaxToolCalls,
		}
	}
	return effectiveAgentRunBudget{
		maxActiveDurationMs: minNonZero(run.MaxActiveDurationMs, currentDurationMs),
		maxGenerations:      minNonZero(run.MaxGenerations, current.MaxGenerations),
		maxToolCalls:        minNonZero(run.MaxToolCalls, current.MaxToolCalls),
	}
}

// errGraphDriveShareElapsed ends an owner-driven drive whose share of its graph's active-duration
// pool ran out while the pool itself had time left: the run stays drivable for the next drive.
var errGraphDriveShareElapsed = errors.New("agent runtime: graph drive share elapsed")

// graphPoolRunBudgetError is the error an interactive run fails with when its graph's pool limit
// reason is used up.
func graphPoolRunBudgetError(pool *common_persistence_models.ChatAgentGraphBudget, reason proto_models.GraphBudgetReason) *commonai.RunBudgetExceededError {
	err := &commonai.RunBudgetExceededError{Kind: commonai.RunBudgetGraphPool, GraphReason: reason}
	if pool != nil {
		switch reason {
		case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_GENERATIONS:
			err.Limit = uint64(pool.MaxGenerations)
		case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_TOOL_CALLS:
			err.Limit = uint64(pool.MaxToolCalls)
		case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION:
			err.Limit = pool.MaxActiveMs
		}
	}
	return err
}

func minNonZero(captured, current uint64) uint64 {
	if captured == 0 {
		return current
	}
	if current == 0 || captured < current {
		return captured
	}
	return current
}

type persistentContinuationBudget struct {
	persistence    common_persistence.CommonPersistenceInterface
	logger         *zap.Logger
	runID          uint64
	maxGenerations uint64
	maxToolCalls   uint64
	// admitInput makes each step's reservation deliver the session's queued input (an IMMEDIATE run).
	admitInput bool
	// onAdmitted runs after a reservation that took input out of the queue has committed, with the
	// messages the delivery created.
	onAdmitted func(context.Context, common_persistence.ChatAgentQueuedInputDelivery)
}

var errAgentRunNoLongerDrivable = errors.New("agent run is no longer drivable")

// ReserveStep reserves the step's generation and, for an IMMEDIATE run, delivers the session's
// queued input into the run as messages in the same transaction, returning how many it delivered.
func (b *persistentContinuationBudget) ReserveStep(ctx context.Context) (int, error) {
	reserved, delivery, err := b.persistence.ReserveChatAgentRunStepAndAdmit(ctx, b.runID, b.maxGenerations, b.maxToolCalls, b.admitInput)
	if err != nil {
		return 0, err
	}
	if !reserved {
		return 0, b.classifyReservationFailure(ctx, commonai.RunBudgetGenerations, 1)
	}
	if delivery.Changed() && b.onAdmitted != nil {
		b.onAdmitted(ctx, delivery)
	}
	if len(delivery.Delivered) > 0 && appLogger.DebugEnabled(b.logger) {
		appLogger.For(ctx, b.logger, agentRuntimeLoggerName).Debug("agent runtime: queued input delivered at step start",
			zap.Uint64(appLogger.KeyAgentRunID, b.runID),
			zap.Int(appLogger.KeyCount, len(delivery.Delivered)))
	}
	return len(delivery.Delivered), nil
}

func (b *persistentContinuationBudget) ReserveGeneration(ctx context.Context) error {
	reserved, err := b.persistence.ReserveChatAgentRunBudget(
		ctx, b.runID, 1, 0, b.maxGenerations, b.maxToolCalls,
	)
	if err != nil {
		return err
	}
	if !reserved {
		return b.classifyReservationFailure(ctx, commonai.RunBudgetGenerations, 1)
	}
	return nil
}

func (b *persistentContinuationBudget) ReserveToolCalls(ctx context.Context, count uint64) error {
	reserved, err := b.persistence.ReserveChatAgentRunBudget(
		ctx, b.runID, 0, count, b.maxGenerations, b.maxToolCalls,
	)
	if err != nil {
		return err
	}
	if !reserved {
		return b.classifyReservationFailure(ctx, commonai.RunBudgetToolCalls, count)
	}
	return nil
}

func (b *persistentContinuationBudget) classifyReservationFailure(
	ctx context.Context,
	kind commonai.RunBudgetKind,
	delta uint64,
) error {
	run, err := b.persistence.GetChatAgentRunByID(ctx, b.runID)
	if err != nil {
		return err
	}
	if run == nil || !isDrivableRunStatus(run.Status) {
		return errAgentRunNoLongerDrivable
	}
	if kind == commonai.RunBudgetGenerations &&
		(run.GenerationCount >= b.maxGenerations || delta > b.maxGenerations-run.GenerationCount) {
		return &commonai.RunBudgetExceededError{Kind: kind, Limit: b.maxGenerations}
	}
	if kind == commonai.RunBudgetToolCalls &&
		(run.ToolCallCount >= b.maxToolCalls || delta > b.maxToolCalls-run.ToolCallCount) {
		return &commonai.RunBudgetExceededError{Kind: kind, Limit: b.maxToolCalls}
	}
	return errAgentRunNoLongerDrivable
}

func (r *agentRuntime[U]) startTyping(ctx context.Context, run *common_persistence_models.ChatAgentRun) func() {
	if r == nil || r.typingStarter == nil || run == nil || run.ChannelID == 0 || run.BotUserID == 0 || run.ParentMessageID != 0 {
		return func() {}
	}
	return r.typingStarter(ctx, &common_service_models.ChatChannel{
		ID: common_service_models.ChatChannelIdType(run.ChannelID),
	}, common_service_models.UserIdType(run.BotUserID))
}

// stepSealer records the run's model steps through SealChatAgentRunStep and publishes each new
// seal's transition. A final answer may steer only when the step can admit input (admitInput) and
// the runner leaves room for another step; the persistence checks the generation headroom under
// maxGenerations and the graph's pool, so a run that could not answer the waiting input completes
// and drains it instead.
func (r *agentRuntime[U]) stepSealer(run *common_persistence_models.ChatAgentRun, maxGenerations uint64, admitInput bool) commonai.AgentStepSealer {
	return func(sealCtx context.Context, seal commonai.AgentStepSeal) (commonai.AgentStepDisposition, error) {
		var disposition common_persistence.ChatAgentRunStepDisposition
		switch seal.Disposition {
		case commonai.AgentStepDispositionContinue:
			disposition = common_persistence.ChatAgentRunStepDispositionContinue
		case commonai.AgentStepDispositionToolLoop:
			disposition = common_persistence.ChatAgentRunStepDispositionToolLoop
		case commonai.AgentStepDispositionFinal:
			disposition = common_persistence.ChatAgentRunStepDispositionFinal
		default:
			return 0, common_persistence.ErrChatAgentRunStepSealInvalid
		}
		recorded, sealed, err := r.persistence.SealChatAgentRunStep(sealCtx, run.ID, seal.StepIndex, disposition, seal.ToolCallCount,
			maxGenerations, admitInput && seal.CanSteer, seal.ConsumedPendingMessageID)
		if err != nil {
			return 0, err
		}
		if sealed {
			r.publishSealedStepTransition(sealCtx, run, recorded, seal.ToolCallCount)
		}
		if recorded == common_persistence.ChatAgentRunStepDispositionSteered {
			return commonai.AgentStepDispositionSteered, nil
		}
		return seal.Disposition, nil
	}
}

// preparedRun bundles everything one runTurn needs after resolution.
type preparedRun struct {
	cfg            commonai.RunConfig
	generator      commonai.Generator
	writer         ports.AgentTranscriptWriter
	inference      commonai.InferenceRecorder
	resumeOnDenial bool
	// admitInput is whether each step admits the session's waiting input: the run's captured
	// input mode is IMMEDIATE. A QUEUE run leaves it for the follow-up its ending drains.
	admitInput bool
}

// prepareRun resolves the run's captured config snapshot, the registered agent + tools, the
// provider-bound generator, and the run-scoped transcript writer into a preparedRun. It is the
// "load captured snapshot + resolve registry by name + build tools/generator" half of the M6 loop
// contract, kept separate from the loop driving for testability.
func (r *agentRuntime[U]) prepareRun(ctx context.Context, run *common_persistence_models.ChatAgentRun) (*preparedRun, error) {
	if run == nil {
		return nil, fmt.Errorf("agent runtime: run is required")
	}
	if run.OrganizationID <= 0 {
		return nil, fmt.Errorf("%w: run %d has no organization", ErrImmutableAgentRuntimeConfiguration, run.ID)
	}
	if run.AgentSessionID == nil || *run.AgentSessionID == 0 {
		// Legacy/manual runs have no first-class session snapshot; the transcript-native loop is only
		// for registered agent sessions.
		return nil, fmt.Errorf("agent runtime: run %d has no agent session", run.ID)
	}
	session, err := r.persistence.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil || session == nil {
		return nil, fmt.Errorf("agent runtime: load session %d: %w", *run.AgentSessionID, err)
	}
	// Archived sessions may resume an existing run, but its durable identity must never drift.
	if session.ChannelID != run.ChannelID || session.ParentMessageID != run.ParentMessageID ||
		session.BotUserID != run.BotUserID || session.OrganizationID != run.OrganizationID {
		return nil, fmt.Errorf("%w: run %d does not match session %d identity", ErrImmutableAgentRuntimeConfiguration, run.ID, session.ID)
	}
	agent, ok := r.agentRegistry.GetAgentType(session.AgentType)
	if !ok {
		// Resumability error: the registered agent the run needs is gone.
		return nil, fmt.Errorf("%w: agent type %q", commonai.ErrToolMissingForPendingCall, session.AgentType)
	}
	if session.InferenceOperation != 0 && session.InferenceOperation != agent.Operation {
		return nil, fmt.Errorf("%w: session inference operation changed", ErrImmutableAgentRuntimeConfiguration)
	}

	// Load the run's CAPTURED config snapshot (not the session's live active config) so resume uses
	// the policy the run started with.
	configVersion := uint64(0)
	if run.AgentSessionConfigVersion != nil {
		configVersion = *run.AgentSessionConfigVersion
	}
	snapshot, toolRows, err := r.persistence.GetChatAgentSessionConfigSnapshot(ctx, session.ID, configVersion)
	if err != nil {
		return nil, fmt.Errorf("agent runtime: load config snapshot v%d: %w", configVersion, err)
	}
	approvalPolicy, compaction, reasoningPolicy, err := decodeCapturedRuntimePolicy(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: decode captured config snapshot v%d: %w", ErrImmutableAgentRuntimeConfiguration, configVersion, err)
	}
	requestedMaxModelSteps, err := chatpkg.DecodeMaxModelSteps(snapshot.ToolConfigPayload, agent.MaxModelSteps)
	if err != nil {
		return nil, fmt.Errorf("agent runtime: decode model-step budget from config snapshot v%d: %w", configVersion, err)
	}
	maxModelSteps := chatpkg.ClampMaxModelSteps(requestedMaxModelSteps, r.modelStepCeiling())

	tools, approvalRequired, err := r.resolveRunTools(session.ID, configVersion, toolRows)
	if err != nil {
		return nil, err
	}
	// A session override that stops the agent spawning withdraws the tools only a spawning parent has.
	if agent.IsDelegating() {
		delegation, err := agent.SessionDelegation(snapshot.DelegationOverridePayload)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve delegation override from config snapshot v%d: %w", ErrImmutableAgentRuntimeConfiguration, configVersion, err)
		}
		installed := ports.DelegationParentToolNames(delegation.Creation)
		tools = slices.DeleteFunc(tools, func(tool ports.AgentToolDefinition) bool {
			return ports.IsDelegationParentToolName(tool.Name) && !slices.Contains(installed, tool.Name)
		})
		if err := r.publishChildScopeSchemas(tools, delegation); err != nil {
			return nil, fmt.Errorf("%w: publish child scope schemas: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
	}
	triggeringUserID := common_service_models.UserIdType(0)
	if run.TriggeringUserID != nil {
		triggeringUserID = common_service_models.UserIdType(*run.TriggeringUserID)
	}
	if triggeringUserID == 0 {
		return nil, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_AUTHORITY_REVOKED}
	}
	// A workflow session's accountable human authorizes its inference; tools still run as the
	// triggering user.
	inferenceActorID := triggeringUserID
	if session.InferenceActorUserID != nil {
		inferenceActorID = common_service_models.UserIdType(*session.InferenceActorUserID)
	}
	contextID := uint64(0)
	if session.ContextID != nil {
		contextID = *session.ContextID
	}
	contextEntityID := uint64(0)
	if session.ContextEntityID != nil {
		contextEntityID = *session.ContextEntityID
	}
	if r.planner == nil {
		return nil, fmt.Errorf("%w: operation %d", apperrors.ErrAgentOperationRouteUnavailable, agent.Operation)
	}
	if session.IsInferenceBlocked() {
		return nil, &inference.BlockError{Reason: session.InferenceBlockedReason}
	}
	selection, selectionErr := session.ExecutionModelSelection()
	if selectionErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, selectionErr)
	}
	legacySelection, selectionErr := run.ExecutionModelSelection()
	if selectionErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, selectionErr)
	}
	if selection == nil && legacySelection != nil {
		legacyPin := common_persistence_models.ChatAgentSessionInferencePin{
			InferenceOperation: agent.Operation, InferencePinVersion: 1,
			ExecutionInferenceModelID: run.ExecutionInferenceModelID, ExecutionInferenceAPI: run.ExecutionInferenceAPI,
			ExecutionProviderID: run.ExecutionProviderID, ExecutionProviderModelID: run.ExecutionProviderModelID,
			SelectionSource: proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE,
			SelectingUserID: new(uint64(triggeringUserID)), SiteOverrideUserID: new(uint64(triggeringUserID)),
		}
		session, err = r.persistence.InitializeChatAgentSessionInferencePin(ctx, session.ID, legacyPin)
		if err != nil {
			return nil, err
		}
		selection, selectionErr = session.ExecutionModelSelection()
		if selectionErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, selectionErr)
		}
	}
	if selection != nil && legacySelection != nil {
		if err := run.ValidateExecutionModel(selection.InferenceModelID, *session.ExecutionProviderID, *session.ExecutionProviderModelID, *selection.API); err != nil {
			return nil, fmt.Errorf("%w: conflicting run and session model pins: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
	}
	if session.SiteOverrideUserID != nil {
		if r.checkOverrideActor == nil {
			return nil, fmt.Errorf("%w: model override authorizer unavailable", ErrImmutableAgentRuntimeConfiguration)
		}
		if err := r.checkOverrideActor(ctx, common_service_models.UserIdType(*session.SiteOverrideUserID)); err != nil {
			return nil, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_AUTHORITY_REVOKED, Cause: err}
		}
	}
	request := inference.GenerationRequest{Operation: int32(agent.Operation), ModelSelection: selection,
		OrganizationID: uint64(session.OrganizationID), ActorUserID: uint64(inferenceActorID), SiteOverrideUserID: common_util.Deref(session.SiteOverrideUserID), Pinned: selection != nil}
	planningCtx := agentkit.ContextWithoutActor[U](ctx)
	plan, planErr := r.planner.PlanGeneration(planningCtx, request)
	if planErr != nil {
		return nil, planErr
	}
	if plan == nil {
		return nil, fmt.Errorf("%w: operation %d planned no generation", apperrors.ErrAgentOperationRouteUnresolved, agent.Operation)
	}
	if !commonai.GenerationRetrierConfigured(plan.Retrier) {
		return nil, fmt.Errorf("%w: operation %d: %w",
			apperrors.ErrAgentOperationRouteUnavailable, agent.Operation, commonai.ErrGenerationRetrierRequired)
	}
	if selection == nil {
		pin := chatpkg.SessionInferencePin(plan, 0)
		session, err = r.persistence.InitializeChatAgentSessionInferencePin(ctx, session.ID, pin)
		if err != nil {
			return nil, err
		}
		if session.ExecutionInferenceModelID == nil || session.ValidateExecutionModel(plan.InferenceModelID, plan.ProviderID, plan.ProviderModelID, plan.API) != nil {
			request.ModelSelection, err = session.ExecutionModelSelection()
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
			}
			request.Pinned = true
			request.SiteOverrideUserID = common_util.Deref(session.SiteOverrideUserID)
			plan, err = r.planner.PlanGeneration(planningCtx, request)
			if err != nil {
				return nil, err
			}
			if plan == nil || !commonai.GenerationRetrierConfigured(plan.Retrier) {
				return nil, apperrors.ErrAgentOperationRouteUnresolved
			}
		}
	}

	if err := session.ValidateExecutionModel(plan.InferenceModelID, plan.ProviderID, plan.ProviderModelID, plan.API); err != nil {
		return nil, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_UNAVAILABLE,
			OrganizationID: uint64(session.OrganizationID), Operation: agent.Operation, InferenceModelID: common_util.Deref(session.ExecutionInferenceModelID), Cause: err}
	}
	policy := ports.ModelPolicy{
		ProviderID:        plan.ProviderID,
		OpenRouterRouting: plan.OpenRouterRouting,
		API:               plan.API,
		ModelID:           plan.ProviderModelID,
		Temperature:       plan.Temperature,
	}
	inferenceModelID := plan.InferenceModelID
	if r.modelPolicyResolver != nil {
		resolvedPolicy, resolveErr := r.modelPolicyResolver(planningCtx, AgentModelPolicyResolverInput{
			AgentType: agent.Type, OrganizationID: common_service_models.OrganizationIdType(session.OrganizationID),
			ActorUserID: inferenceActorID, AgentSessionID: session.ID, AgentRunID: run.ID,
		})
		if resolveErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrModelPolicyResolverFailed, resolveErr)
		}
		if resolvedPolicy == nil || resolvedPolicy.ProviderID == "" || resolvedPolicy.ModelID == "" {
			return nil, ErrResolvedModelPolicyIncomplete
		}
		if resolvedPolicy.ProviderID != plan.ProviderID || resolvedPolicy.ModelID != plan.ProviderModelID {
			return nil, fmt.Errorf("%w: %w: operation %d routed %s/%q, resolver returned %s/%q",
				ErrModelPolicyResolverFailed,
				ErrModelPolicyResolverRouteMismatch,
				agent.Operation,
				plan.ProviderID,
				plan.ProviderModelID,
				resolvedPolicy.ProviderID,
				resolvedPolicy.ModelID,
			)
		}
		policy = *resolvedPolicy
		policy.API = plan.API
		policy.OpenRouterRouting = plan.OpenRouterRouting
	}
	if selection != nil {
		policy.Temperature = plan.Temperature
	}
	// These knobs have no operation-route columns and remain agent-owned.
	if policy.MaxOutputTokens == nil {
		policy.MaxOutputTokens = agent.MaxOutputTokens
	}
	if policy.RequestReasoningTraces == nil {
		policy.RequestReasoningTraces = agent.RequestReasoningTraces
	}
	if compaction.Strategy == ports.AgentContextCheckpoint && policy.MaxOutputTokens == nil {
		cap := compaction.OutputReserveTokens
		policy.MaxOutputTokens = &cap
	}
	sessionMetadata := agentSessionMetadataFromJSON(session.MetadataJSON)
	parentAgentSessionID := common_util.Deref(session.ParentAgentSessionID)
	// runInfo hands the application this drive's durable identity. Each consumer gets its own
	// Metadata copy, and ModelPolicy is whatever the drive has resolved by that point.
	runInfo := func() ports.AgentRunInfo {
		return ports.AgentRunInfo{
			OrganizationID:            common_service_models.OrganizationIdType(run.OrganizationID),
			AgentType:                 agent.Type,
			ChannelID:                 run.ChannelID,
			ParentMessageID:           run.ParentMessageID,
			AgentRunID:                run.ID,
			AgentSessionID:            session.ID,
			AgentSessionConfigVersion: configVersion,
			ParentAgentSessionID:      parentAgentSessionID,
			TriggeringUserID:          triggeringUserID,
			SelectingUserID:           common_service_models.UserIdType(common_util.Deref(session.SelectingUserID)),
			SelectionSource:           session.SelectionSource,
			InferencePinVersion:       session.InferencePinVersion,
			ContextID:                 contextID,
			ContextEntityID:           contextEntityID,
			Metadata:                  common_util.CloneMapOrNil(sessionMetadata),
			ModelPolicy:               policy,
			MaxModelSteps:             maxModelSteps,
		}
	}
	if agent.ProviderConfigBuilder != nil {
		policy.ProviderConfig, err = resolveProviderConfig(ctx, agent.ProviderConfigBuilder, runInfo(), policy.ProviderConfig)
		if err != nil {
			return nil, fmt.Errorf("agent runtime: build provider config for run %d: %w", run.ID, err)
		}
	}
	// Every provider attempt of a workflow-owned agent revalidates ownership, and any agent may add
	// its own admission hook. The step and attempt identity arrive as explicit arguments from the
	// continuation runner and the route-bound retrier, so the hook never reads them from a context.
	retrier := newAgentAdmissionRetrier(plan.Retrier, func(attemptCtx context.Context, step commonai.GenerationStep, attempt commonai.GenerationAttempt) error {
		currentSession, err := r.persistence.GetChatAgentSessionByID(attemptCtx, session.ID)
		if err != nil {
			return err
		}
		if currentSession.IsInferenceBlocked() {
			return &inference.BlockError{Reason: currentSession.InferenceBlockedReason}
		}
		if currentSession.InferencePinVersion != session.InferencePinVersion {
			return fmt.Errorf("%w: inference pin changed during drive", ErrImmutableAgentRuntimeConfiguration)
		}
		if err := currentSession.ValidateExecutionModel(plan.InferenceModelID, plan.ProviderID, plan.ProviderModelID, plan.API); err != nil {
			return fmt.Errorf("%w: %w", ErrImmutableAgentRuntimeConfiguration, err)
		}
		ceiling := chatpkg.ClampMaxModelSteps(requestedMaxModelSteps, r.modelStepCeiling())
		if ceiling != 0 && step.Index >= ceiling {
			return commonai.ErrMaxModelStepsExceeded
		}
		if session.SiteOverrideUserID != nil {
			if err := r.checkOverrideActor(attemptCtx, common_service_models.UserIdType(*session.SiteOverrideUserID)); err != nil {
				return &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_AUTHORITY_REVOKED, Cause: err}
			}
		}
		if agent.IsWorkflowOwned() {
			if _, err := r.admitWorkflow(attemptCtx, run.ID); err != nil {
				return err
			}
		}
		if agent.ProviderAttemptAdmission == nil {
			return nil
		}
		// Fail closed: an application keys reservations and billing on this identity, so a
		// custom retrier that mints none must not be admitted with a blank one.
		if attempt.InvocationID == "" || attempt.Number < 1 {
			return fmt.Errorf("%w: invocation %q attempt %d", errAgentAttemptIdentityMissing, attempt.InvocationID, attempt.Number)
		}
		return agent.ProviderAttemptAdmission(attemptCtx, ports.AgentProviderAttempt{
			Run:              runInfo(),
			StepIndex:        step.Index,
			InvocationID:     attempt.InvocationID,
			AttemptNumber:    attempt.Number,
			Operation:        agent.Operation,
			ProviderID:       plan.ProviderID,
			ProviderModelID:  plan.ProviderModelID,
			InferenceModelID: plan.InferenceModelID,
		})
	})
	if compaction.Strategy == ports.AgentContextCheckpoint {
		sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	}
	effectiveModelConfig, err := modelPolicyConfig(policy, r.modelPolicyDefaults())
	if err != nil {
		return nil, err
	}
	toolManifestSHA256, err := commonai.CanonicalToolManifestWithProviderConfigSHA256(tools, effectiveModelConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: fingerprint provider tool manifest: %w", ErrImmutableAgentRuntimeConfiguration, err)
	}
	generator, err := r.generatorFactory(planningCtx, AgentGeneratorFactoryInput{
		OrganizationID:            common_service_models.OrganizationIdType(run.OrganizationID),
		AgentType:                 agent.Type,
		ChannelID:                 run.ChannelID,
		ParentMessageID:           run.ParentMessageID,
		AgentRunID:                run.ID,
		AgentSessionID:            session.ID,
		AgentSessionConfigVersion: configVersion,
		ParentAgentSessionID:      parentAgentSessionID,
		TriggeringUserID:          triggeringUserID,
		InferenceActorUserID:      inferenceActorID,
		ContextID:                 contextID,
		ContextEntityID:           contextEntityID,
		Metadata:                  common_util.CloneMapOrNil(sessionMetadata),
		Policy:                    policy,
		Tools:                     copyAgentToolDefinitions(tools),
		Retrier:                   retrier,
	})
	if err != nil {
		return nil, fmt.Errorf("agent runtime: build generator: %w", err)
	}

	if plan.API != 0 {
		identity, ok := generator.(interface {
			APIIdentity() (proto_models.InferenceAPI, string)
		})
		if !ok {
			return nil, fmt.Errorf("%w: custom generator must expose APIIdentity for an explicit API route", ErrImmutableAgentRuntimeConfiguration)
		}
		actual, _ := identity.APIIdentity()
		if actual != plan.API {
			return nil, fmt.Errorf("%w: generator did not honor the route API", ErrImmutableAgentRuntimeConfiguration)
		}
	}

	writer, err := r.buildRunWriter(ctx, run)
	if err != nil {
		return nil, err
	}

	cfg := commonai.RunConfig{
		ResultReady:               r.workflowResultReadyCallback(run.ID, agent),
		OrganizationID:            common_service_models.OrganizationIdType(run.OrganizationID),
		ChannelID:                 run.ChannelID,
		ParentMessageID:           run.ParentMessageID,
		AgentRunID:                run.ID,
		AgentSessionID:            session.ID,
		AgentSessionConfigVersion: configVersion,
		AgentType:                 agent.Type,
		Scope:                     ports.DelegationScope(sessionMetadata),
		BotUserID:                 run.BotUserID,
		TriggeringUserID:          triggeringUserID,
		InferenceActorUserID:      inferenceActorID,
		ProviderID:                policy.ProviderID,
		ProviderModelID:           policy.ModelID,
		InferenceModelID:          inferenceModelID,
		ToolManifestSHA256:        toolManifestSHA256,
		Tools:                     tools,
		ApprovalRequired:          approvalRequired,
		ApprovalExpiresAfter:      approvalPolicy.ApprovalExpiresAfter,
		HideReasoningTraces:       reasoningPolicy.HideReasoningTraces,
		MaxModelSteps:             requestedMaxModelSteps,
		ModelStepsUsed:            r.nextInferenceStepIndex(ctx, run.ID),
		// A delegated child's normal closing message is the result its parent reads on the main
		// timeline; only a thread run can broadcast, which the child always is.
		BroadcastClosingMessage: parentAgentSessionID != 0 && run.ParentMessageID != 0,
		ContextOptions: commonai.AgentContextOptions{
			CurrentAPI:           plan.API,
			CurrentModelID:       policy.ModelID,
			CurrentBotUserID:     run.BotUserID,
			SystemPrompt:         r.systemPromptProvider(session),
			MaxTokens:            int(compaction.MaxContextTokens),
			LastKnownInputTokens: r.lastKnownInputTokens(ctx, run.ID),
			CompactionEnabled:    compaction.Enabled,
			CompactAtInputTokens: int(compaction.CompactAtInputTokens),
			TargetSummaryTokens:  int(compaction.TargetSummaryTokens),
			ManualCompactionOnly: compaction.ManualOnly,
		},
	}

	// Both main-timeline parents and delegated children acknowledge pending wakes only when
	// their trigger is represented in a sealed generation's selected context. Parent broadcasts
	// keep normal trimming; the protected instruction rules below apply only to children.
	if session.IsMainTimeline() || parentAgentSessionID != 0 {
		cfg.PendingMessageID = func(readCtx context.Context) (uint64, error) {
			return r.persistence.GetChatAgentSessionPendingWakeMessageID(readCtx, session.ID)
		}
	}
	if parentAgentSessionID != 0 {
		parent, err := r.persistence.GetChatAgentSessionByID(ctx, parentAgentSessionID)
		if err != nil {
			return nil, err
		}
		if parent.ChannelID == session.ChannelID {
			cfg.ContextOptions.OrchestratorBotUserID = parent.BotUserID
		}
		cfg.ContextOptions.ConsumedPendingMessageID = run.ConsumedPendingMessageID
		if trigger := common_util.Deref(run.TriggeringMessageID); trigger > run.ConsumedPendingMessageID {
			cfg.ContextOptions.RequiredMessageID = trigger
		}
		cfg.UnconsumedChildMessageIDs = func(readCtx context.Context, throughID uint64) ([]uint64, error) {
			return r.persistence.ListUnconsumedChildMessageIDs(readCtx, session.ID, throughID)
		}
	}
	if identity, ok := generator.(interface {
		APIIdentity() (proto_models.InferenceAPI, string)
	}); ok {
		cfg.ContextOptions.CurrentAPI, cfg.ContextOptions.CurrentDestinationFingerprint = identity.APIIdentity()
	}

	if compaction.Strategy != "" || r.liveStateProvider(agent, session, run) != nil {
		if err := r.configureContextMaintenance(ctx, &cfg, agent, session, run, compaction, policy, retrier); err != nil {
			return nil, err
		}
	}
	return &preparedRun{
		cfg:            cfg,
		generator:      actorlessInferenceGenerator[U](r.guardWorkflowGenerator(ctx, run.ID, generator)),
		writer:         writer,
		inference:      r.inferenceRecorder(),
		resumeOnDenial: approvalPolicy.ResumeOnDenial,
		// A workflow-owned session takes no chat input, so its steps skip the admission locks.
		admitInput: !agent.IsWorkflowOwned() && snapshot != nil &&
			snapshot.InputMode == proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_IMMEDIATE,
	}, nil
}

func resolveProviderConfig(
	ctx context.Context,
	builder ports.AgentProviderConfigBuilder,
	run ports.AgentRunInfo,
	static *modelconfig.Config,
) (config *modelconfig.Config, err error) {
	if builder == nil {
		return static, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			config = nil
			err = fmt.Errorf("%w: %v", ErrProviderConfigBuilderFailed, recovered)
		}
	}()
	config = builder(ctx, run)
	if config == nil {
		return static, nil
	}
	return config, nil
}

// decodeCapturedRuntimePolicy reads run-scoped policy from the captured session config snapshot. Tool
// rows carry the authoritative per-tool enable/approval flags; these blobs carry the session-level
// approval window and compaction limits. Missing legacy blobs decode to the safe defaults from their
// codecs.
func decodeCapturedRuntimePolicy(snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot) (ports.AgentApprovalPolicy, ports.AgentCompactionRules, ports.AgentReasoningPolicy, error) {
	if snapshot == nil {
		return ports.AgentApprovalPolicy{ResumeOnDenial: true}, ports.AgentCompactionRules{}, ports.AgentReasoningPolicy{}, nil
	}
	approvalPolicy, err := chatpkg.DecodeApprovalPolicy(snapshot.ToolConfigPayload)
	if err != nil {
		return ports.AgentApprovalPolicy{}, ports.AgentCompactionRules{}, ports.AgentReasoningPolicy{}, err
	}
	reasoningPolicy, err := chatpkg.DecodeReasoningPolicy(snapshot.ToolConfigPayload)
	if err != nil {
		return ports.AgentApprovalPolicy{}, ports.AgentCompactionRules{}, ports.AgentReasoningPolicy{}, err
	}
	compaction, err := ports.DecodeCompactionPayload(snapshot.CompactionPayload)
	if err != nil {
		return ports.AgentApprovalPolicy{}, ports.AgentCompactionRules{}, ports.AgentReasoningPolicy{}, err
	}
	return approvalPolicy, compaction, reasoningPolicy, nil
}

// lastKnownInputTokens returns the most recent positive input_tokens value recorded for a run. The
// context builder treats this ledger-backed value as the canonical context-size signal; when there
// are no usable rows or the read fails, it falls back to estimator-based sizing.
func (r *agentRuntime[U]) lastKnownInputTokens(ctx context.Context, runID uint64) int {
	if r == nil || r.persistence == nil || runID == 0 {
		return 0
	}
	inputTokens, err := r.persistence.GetLatestChatInferenceInputTokens(ctx, runID)
	if err != nil {
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to read last known input tokens",
			zap.String(logkeys.Stage, "read_latest_inference_input_tokens"),
			zap.String(logkeys.Fallback, "estimate_context_size"),
			zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(err))
		return 0
	}
	return int(inputTokens)
}

const maxChatInferenceStepScanPageSize uint32 = 200

// resolveRunTools resolves the run's captured-enabled tool rows against the CURRENT tool registry
// and builds the per-tool approval-required map. A captured-enabled tool that is no longer
// registered is a resumability failure: continuing with fewer tools would silently change the run's
// historical contract and could make provider reconstruction diverge from the session snapshot.
func (r *agentRuntime[U]) resolveRunTools(
	sessionID uint64,
	configVersion uint64,
	toolRows []common_persistence_models.ChatAgentSessionToolConfig,
) ([]ports.AgentToolDefinition, map[string]bool, error) {
	tools := make([]ports.AgentToolDefinition, 0, len(toolRows))
	approvalRequired := make(map[string]bool, len(toolRows))
	for _, row := range toolRows {
		if !row.Enabled {
			continue
		}
		def, ok := r.agentToolRegistry.GetTool(row.ToolName)
		if !ok {
			return nil, nil, appLogger.InvariantError(
				"chat_captured_enabled_tool_registered",
				fmt.Errorf("%w: enabled tool %q missing from registry for session %d config version %d",
					commonai.ErrToolMissingForPendingCall, row.ToolName, sessionID, configVersion),
				zap.String(logkeys.Stage, "resolve_run_tools"),
				zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
				zap.Uint64(appLogger.KeyConfigVersion, configVersion),
				zap.String(appLogger.KeyToolName, row.ToolName),
				zap.String(logkeys.Expected, "captured enabled tool present in registry"),
				zap.String(logkeys.Actual, "tool missing from registry"),
			)
		}
		tools = append(tools, def)
		approvalRequired[row.ToolName] = row.RequiresApproval
	}
	return tools, approvalRequired, nil
}

// publishChildScopeSchemas gives the run's spawn_subagent, when it has the tool, the scope schemas of
// the child types its delegation declares, so the model sees what each child's scope must be.
func (r *agentRuntime[U]) publishChildScopeSchemas(tools []ports.AgentToolDefinition, delegation *ports.AgentDelegation) error {
	spawn := slices.IndexFunc(tools, func(tool ports.AgentToolDefinition) bool { return tool.Name == ports.DelegationToolSpawn })
	if spawn < 0 {
		return nil
	}
	schemas := make(map[string]jsontext.Value)
	for _, childType := range delegation.ChildAgentTypes {
		if child, ok := r.agentRegistry.GetAgentType(childType); ok && child.ScopePolicy != nil {
			schemas[childType] = child.ScopePolicy.Schema
		}
	}
	schema, err := botpkg.SpawnSubagentInputSchema(schemas)
	if err != nil {
		return err
	}
	tools[spawn].InputSchemaJSON = schema
	return nil
}

// systemPromptProvider uses only the immutable instructions captured at session creation.
func (r *agentRuntime[U]) systemPromptProvider(session *common_persistence_models.ChatAgentSession) commonai.SystemPromptProvider {
	prompt := session.EffectiveSystemPrompt
	return func(context.Context) (string, error) { return prompt, nil }
}

// workflowStateProvider evaluates live run-scoped state independently of session instructions.
func (r *agentRuntime[U]) workflowStateProvider(agent ports.AgentDefinition, session *common_persistence_models.ChatAgentSession, run *common_persistence_models.ChatAgentRun) commonai.SystemPromptProvider {
	builder := agent.WorkflowStateBuilder
	input := ports.AgentSystemPromptInput{
		OrganizationID:       common_service_models.OrganizationIdType(run.OrganizationID),
		AgentType:            agent.Type,
		ChannelID:            run.ChannelID,
		ParentMessageID:      run.ParentMessageID,
		AgentSessionID:       session.ID,
		ParentAgentSessionID: common_util.Deref(session.ParentAgentSessionID),
	}
	if run.TriggeringUserID != nil {
		input.ActorUserID = common_service_models.UserIdType(*run.TriggeringUserID)
	}
	if session.ContextID != nil {
		input.ContextID = *session.ContextID
	}
	if session.ContextEntityID != nil {
		input.ContextEntityID = *session.ContextEntityID
	}
	metadata := agentSessionMetadataFromJSON(session.MetadataJSON)
	return func(ctx context.Context) (string, error) {
		promptInput := input
		promptInput.Metadata = common_util.CloneMapOrNil(metadata)
		return builder(ctx, promptInput)
	}
}

func (r *agentRuntime[U]) nextInferenceStepIndex(ctx context.Context, runID uint64) uint32 {
	if r == nil || r.persistence == nil || runID == 0 {
		return 0
	}
	var maxStep uint32
	found := false
	readMax := func() error {
		// Built-in persistence uses one statement over the complete ledger. Keep
		// the paginated path for external adapters without the scalar capability.
		if reader, ok := r.persistence.(interface {
			GetMaxChatInferenceStepIndex(context.Context, uint64) (*uint32, error)
		}); ok {
			step, err := reader.GetMaxChatInferenceStepIndex(ctx, runID)
			if step != nil {
				maxStep, found = *step, true
			}
			return err
		}
		var cursor uint64
		for {
			rows, page, err := r.persistence.ListChatInferenceCallsByRun(ctx, runID, &proto_models.PaginationRequest{
				Limit:  maxChatInferenceStepScanPageSize,
				Cursor: cursor,
			})
			if err != nil {
				return err
			}
			for _, row := range rows {
				if row == nil || row.AgentStepIndex == nil {
					continue
				}
				if !found || *row.AgentStepIndex > maxStep {
					maxStep = *row.AgentStepIndex
					found = true
				}
			}
			if page == nil || !page.HasNextPage || page.NextCursor == 0 {
				break
			}
			cursor = page.NextCursor
		}
		return nil
	}
	if err := readMax(); err != nil {
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to read inference steps",
			zap.String(logkeys.Stage, "read_agent_inference_steps"),
			zap.String(logkeys.Fallback, "restart_inference_step_index_at_zero"),
			zap.Uint64(appLogger.KeyAgentRunID, runID), zap.Error(err))
		return 0
	}
	if !found {
		return 0
	}
	if maxStep == ^uint32(0) {
		return maxStep
	}
	return maxStep + 1
}

func (r *agentRuntime[U]) hasUnresolvedToolCallsInStep(ctx context.Context, runID uint64, stepIndex uint32) (bool, error) {
	count, err := r.persistence.CountUnresolvedToolCallsInStep(ctx, runID, stepIndex)
	if err != nil {
		return false, fmt.Errorf("agent runtime: count unresolved tool calls for run %d step %d: %w", runID, stepIndex, err)
	}
	return count > 0, nil
}

// loadChannel loads + converts the run's channel for the transcript writer (which requires a non-nil
// channel with a non-zero ID).
func (r *agentRuntime[U]) loadChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error) {
	pChannel, err := r.persistence.GetChatChannelByID(ctx, channelID)
	if err != nil || pChannel == nil {
		return nil, fmt.Errorf("agent runtime: load channel %d: %w", channelID, err)
	}
	channel := r.converter.PersistenceChatChannelToService(pChannel)
	if channel == nil || channel.ID == 0 {
		return nil, fmt.Errorf("agent runtime: channel %d could not be resolved", channelID)
	}
	return channel, nil
}

// buildRunWriter loads the run's channel and synthesizes the run-scoped transcript writer from
// the run row's identity fields (bot + triggering user). Both the turn setup (prepareRun) and the
// failure-notice path use it, so a writer can be built from the durable run alone — even when the
// failure happened before a turn writer existed.
func (r *agentRuntime[U]) buildRunWriter(ctx context.Context, run *common_persistence_models.ChatAgentRun) (ports.AgentTranscriptWriter, error) {
	if r.writerFactory == nil {
		return nil, commonai.ErrContinuationNotInitialized
	}
	channel, err := r.loadChannel(ctx, run.ChannelID)
	if err != nil {
		return nil, err
	}
	triggeringUserID := common_service_models.UserIdType(0)
	if run.TriggeringUserID != nil {
		triggeringUserID = common_service_models.UserIdType(*run.TriggeringUserID)
	}
	return r.writerFactory(ctx, channel, common_service_models.UserIdType(run.BotUserID), triggeringUserID), nil
}

// completeRun CASes the run RUNNING/RETRYABLE -> COMPLETED on a clean finish (no further tool
// requests). It is conditional so it never overrides a terminal run or a run a concurrent path moved
// to WAITING_FOR_APPROVAL; a no-op CAS is fine.
func (r *agentRuntime[U]) completeRun(ctx context.Context, run *common_persistence_models.ChatAgentRun) {
	runID := run.ID
	swapped, err := r.persistence.CompareAndSetChatAgentRunStatus(
		ctx, runID,
		common_persistence.DrivableChatAgentRunStatuses(),
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED,
	)
	if err != nil {
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Error("agent runtime: failed to complete run",
			zap.String(logkeys.Stage, "complete_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED),
			zap.Error(err))
		return
	}
	if swapped {
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED)
	}
}

// publishSealedStepTransition reports the status a durable step seal set. A FINAL seal completes
// the run. A tool-loop seal pauses it only when one of its calls needs approval, which the seal
// decides from durable rows, so the run is re-read rather than guessed at.
func (r *agentRuntime[U]) publishSealedStepTransition(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	disposition common_persistence.ChatAgentRunStepDisposition,
	toolCallCount uint32,
) {
	switch {
	case disposition == common_persistence.ChatAgentRunStepDispositionFinal:
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED)
	case disposition == common_persistence.ChatAgentRunStepDispositionToolLoop && toolCallCount > 0:
		sealedRun, err := r.persistence.GetChatAgentRunByID(ctx, run.ID)
		if err != nil || sealedRun == nil {
			return
		}
		if sealedRun.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL {
			r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED)
		}
	}
}

// terminalRunCauses are the causes that end a run for good: a re-drive resolves the same captured
// config, the same registry, and the same model route, so the outcome cannot change. Runs are
// drive-on-nudge (nothing sweeps a run that stopped mid-drive), so every cause listed here must be
// settled FAILED with the visible notice or the turn just stops and the user sees the typing
// indicator disappear with nothing behind it. Transient causes (a DB read in turn setup, a provider
// hiccup) are deliberately absent: they leave the run drivable so a later nudge — the approved-work
// backstop, or the next approval resume — can retry them.
var terminalRunCauses = []error{
	commonai.ErrToolMissingForPendingCall,
	commonai.ErrToolSchemaInvalid,
	commonai.ErrTooManySchemaInvalidSteps,
	commonai.ErrTooManyProtocolInvalidSteps,
	commonai.ErrPartialStreamFailed,
	commonai.ErrUnsupportedAssistantMedia,
	commonai.ErrUnsupportedAssistantCustomPart,
	commonai.ErrProviderStateCommitFailed,
	commonai.ErrPartialToolCallCommitFailed,
	commonai.ErrAgentStepSealFailed,
	commonai.ErrMalformedArgumentsRecoveryExhausted,
	commonai.ErrMalformedArgumentsRecoveryUnavailable,
	commonai.ErrEmptyAssistantTurn,
	commonai.ErrMaxOutputTokensExceeded,
	commonai.ErrCurrentToolStepExceedsContextBudget,
	commonai.ErrHydratedMediaExceedsContextBudget,
	commonai.ErrContextLimit,
	commonai.ErrCompactionInvalid,
	commonai.ErrCompactionUnavailable,
	commonai.ErrUnsupportedToolResultContent,
	commonai.ErrGenerationRetrierRequired,
	commonai.ErrToolExecutionAttemptsExceeded,
	ErrModelPolicyResolverFailed,
	ErrResolvedModelPolicyIncomplete,
	ErrImmutableAgentRuntimeConfiguration,
	apperrors.ErrAgentOperationRouteUnresolved,
	apperrors.ErrAgentOperationRouteUnavailable,
	ErrProviderConfigBuilderFailed,
}

func isTerminalRunCause(cause error) bool {
	// Provider adapters may expose deterministic-vs-transient classification without importing a
	// provider SDK here. errors.As traverses the ordinary wrapping performed by Genkit/generators.
	var classified interface{ Retryable() bool }
	if errors.As(cause, &classified) && !classified.Retryable() {
		return true
	}
	return slices.ContainsFunc(terminalRunCauses, func(sentinel error) bool { return errors.Is(cause, sentinel) })
}

func (r *agentRuntime[U]) settleContinuationError(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause error) {
	if cause == nil || run == nil {
		return
	}
	if blocked, ok := errors.AsType[*inference.BlockError](cause); ok {
		r.blockInferenceSession(ctx, run, blocked)
		return
	}
	if errors.Is(cause, commonai.ErrMaxModelStepsExceeded) {
		r.failRunForModelStepLimit(ctx, run, cause)
		return
	}
	if batchErr, ok := errors.AsType[*commonai.InlineToolExecutionBatchError](cause); ok {
		r.settleInlineToolExecutionErrors(ctx, run, batchErr.Failures, batchErr)
		return
	}
	if inlineErr, ok := errors.AsType[*commonai.InlineToolExecutionError](cause); ok {
		r.settleInlineToolExecutionError(ctx, run, inlineErr)
		return
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return
	}
	if budgetErr, ok := errors.AsType[*commonai.RunBudgetExceededError](cause); ok {
		r.stopRunForBudget(ctx, run, budgetErr)
		return
	}
	// Every sentinel here is unrecoverable for THIS run and nothing re-drives it: the drive triggers
	// are fresh-run creation, approval resume, and the approved-work backstop (which scans approved
	// but unexecuted TOOL_CALLs). A run that fails before producing a tool call has none of those, so
	// an unmatched sentinel wedges it in RUNNING forever behind a typing indicator that just stops.
	// The route sentinels belong here for that reason: repairing the route releases later runs, not
	// this one.
	if isTerminalRunCause(cause) {
		r.failRun(ctx, run, cause)
	} else if commonai.IsCompactionGenerationFailure(cause) || errors.Is(cause, commonai.ErrContinuationNoteWriteFailed) {
		r.markRunRetryable(ctx, run, cause)
	}
}

func (r *agentRuntime[U]) blockInferenceSession(ctx context.Context, run *common_persistence_models.ChatAgentRun, blocked *inference.BlockError) {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.recoveryPolicy().settleTimeout)
	defer cancel()
	session, err := r.persistence.GetChatAgentSessionByID(recordCtx, common_util.Deref(run.AgentSessionID))
	if err == nil {
		err = r.persistence.BlockChatAgentSessionInference(recordCtx, session.ID, session.InferencePinVersion, blocked.Reason)
	}
	if err != nil {
		appLogger.For(recordCtx, r.logger, agentRuntimeLoggerName).Error("failed to block inference session",
			zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
		return
	}
	appLogger.For(recordCtx, r.logger, agentRuntimeLoggerName).Info("inference session blocked",
		zap.Uint64("agent_session_id", session.ID), zap.Stringer("reason", blocked.Reason))
	r.markRunRetryable(recordCtx, run, blocked)
}

// durablyCommittedInlineFailureStep recognizes a commit-ambiguous live-inline error whose durable
// state proves that every reported failure actually crossed the atomic TOOL_RESULT + EXECUTED
// boundary. All failures must belong to this run and the same model step; a mixed batch (one durable
// commit plus one genuine failure/recovery lease) falls through to ordinary settlement. ExecutedAt
// is required because CommitClaimedToolCallResult writes it in the same transaction as the paired
// result, making it the durable completion marker rather than a loose enum observation.
func (r *agentRuntime[U]) durablyCommittedInlineFailureStep(ctx context.Context, runID uint64, cause error) (uint32, bool) {
	if r == nil || r.persistence == nil || runID == 0 || cause == nil {
		return 0, false
	}
	var failures []*commonai.InlineToolExecutionError
	if batchErr, ok := errors.AsType[*commonai.InlineToolExecutionBatchError](cause); ok {
		failures = batchErr.Failures
	} else {
		var inlineErr *commonai.InlineToolExecutionError
		if !errors.As(cause, &inlineErr) {
			return 0, false
		}
		failures = []*commonai.InlineToolExecutionError{inlineErr}
	}
	if len(failures) == 0 {
		return 0, false
	}

	var step uint32
	stepSet := false
	for _, failure := range failures {
		if failure == nil || failure.ToolCallMessageID == 0 {
			return 0, false
		}
		row, err := r.persistence.GetChatMessageByIDWithOptions(ctx, failure.ToolCallMessageID, common_persistence.ChatMessageReadOptions{
			VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript,
		})
		if err != nil || row == nil || row.ToolCall == nil || row.AgentRunID == nil || *row.AgentRunID != runID ||
			row.AgentStepIndex == nil || row.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL ||
			!isPersistenceInlineApprovalStatus(row.ToolCall.ApprovalStatus) ||
			row.ToolCall.ExecutionState != proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED ||
			row.ToolCall.ExecutedAt == nil {
			return 0, false
		}
		if !stepSet {
			step = *row.AgentStepIndex
			stepSet = true
		} else if step != *row.AgentStepIndex {
			return 0, false
		}
	}
	return step, stepSet
}

// failRunForModelStepLimit ends a run that spent its model-step budget. The notice is written with
// the transition: a step-limit stop reached from WAITING_FOR_APPROVAL (an approval resume that finds
// no budget left) must still tell the user why the assistant stopped.
func (r *agentRuntime[U]) failRunForModelStepLimit(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause error) {
	r.terminalizeRunWithNotice(ctx, run, cause, "settle_model_step_limit",
		common_persistence.ActiveChatAgentRunStatuses(),
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_MODEL_STEP_LIMIT)
}

func (r *agentRuntime[U]) settleInlineToolExecutionError(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause *commonai.InlineToolExecutionError) {
	if cause == nil || run == nil {
		return
	}
	r.settleInlineToolExecutionErrors(ctx, run, []*commonai.InlineToolExecutionError{cause}, cause)
}

// settleInlineToolExecutionErrors persists the outcome of every failed sibling before changing the
// owning run state. This order matters for parallel model-emitted calls: once the run becomes
// RETRYABLE the durable scanner may claim retryable rows, and once it becomes FAILED no scanner may
// claim any of them. A cancellation/deadline is retryable for the affected call even if an executor
// incorrectly labeled it terminal, except when the durable execution-attempt budget is exhausted.
func (r *agentRuntime[U]) settleInlineToolExecutionErrors(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	failures []*commonai.InlineToolExecutionError,
	cause error,
) {
	if run == nil || len(failures) == 0 {
		return
	}
	type classifiedFailure struct {
		failure   *commonai.InlineToolExecutionError
		retryable bool
	}
	terminalFailures := make([]classifiedFailure, 0, len(failures))
	retryableFailures := make([]classifiedFailure, 0, len(failures))
	for _, failure := range failures {
		if failure == nil {
			continue
		}
		attemptsExhausted := errors.Is(failure.Err, commonai.ErrToolExecutionAttemptsExceeded)
		retryable := failure.Retryable || (!attemptsExhausted &&
			(errors.Is(failure.Err, context.Canceled) || errors.Is(failure.Err, context.DeadlineExceeded)))
		classified := classifiedFailure{failure: failure, retryable: retryable}
		if retryable {
			retryableFailures = append(retryableFailures, classified)
		} else {
			terminalFailures = append(terminalFailures, classified)
		}
	}
	settlementFailed := false
	settledAny := false
	settle := func(item classifiedFailure, retryable bool) bool {
		failure := item.failure
		lastErr := capRuntimeExecutionError(failure.Error())
		if failure.ToolCallMessageID == 0 {
			appLogger.LogInvariantViolation(appLogger.For(ctx, r.logger, agentRuntimeLoggerName), "persisted_inline_tool_call_has_transcript_id", failure,
				zap.String(logkeys.Stage, "settle_inline_tool_execution_error"),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.String(logkeys.Expected, "tool_call_message_id > 0"),
				zap.Uint64(logkeys.Actual, failure.ToolCallMessageID),
				zap.String(logkeys.Fallback, "fail_run_closed"))
			settlementFailed = true
			return false
		}
		marked, err := r.persistence.MarkInlineToolCallExecutionFailure(ctx, failure.ToolCallMessageID, retryable, &lastErr)
		if err == nil && !marked && r.inlineFailureSettlementLostToRecovery(ctx, failure.ToolCallMessageID) {
			// The durable backstop claimed this formerly fresh row while its original live
			// execution was still finishing. Its lease now exclusively owns result/failure
			// settlement, so this stale unleased outcome must not fail the run or clear the fence.
			return false
		}
		if err != nil || !marked {
			settlementFailed = true
			appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to mark inline tool execution failure",
				zap.String(logkeys.Stage, "settle_inline_tool_execution_failure"),
				zap.String(logkeys.Fallback, "fail_agent_run_closed"),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyMessageID, failure.ToolCallMessageID),
				zap.Bool(appLogger.KeyRetryable, retryable),
				zap.Bool(appLogger.KeyMarked, marked),
				zap.Error(err),
				zap.NamedError(appLogger.KeyCause, failure))
			return false
		}
		settledAny = true
		return true
	}

	// Settle terminal candidates first. A terminal outcome that lost ownership to a recovery lease
	// is stale and does not make the run terminal; only a failure row this live path successfully
	// owns and closes may impose terminal policy on its retryable siblings.
	terminalOwned := false
	for _, item := range terminalFailures {
		terminalOwned = settle(item, false) || terminalOwned
	}
	for _, item := range retryableFailures {
		// If an owned sibling is terminal (or settlement integrity itself failed), close retryable
		// siblings terminally before failing the run so the scanner cannot claim a half-settled batch.
		settle(item, !terminalOwned && !settlementFailed)
	}
	if !settledAny {
		if settlementFailed {
			r.failRun(ctx, run, cause)
		}
		return
	}
	if terminalOwned || settlementFailed {
		r.failRun(ctx, run, cause)
		return
	}
	r.markRunRetryable(ctx, run, cause)
}

// inlineFailureSettlementLostToRecovery distinguishes the safe false-return from
// MarkInlineToolCallExecutionFailure (a recovery worker either owns an EXECUTING lease or has
// already committed the result) from every other failed settlement, which remains fail-closed.
// The read uses service-transcript visibility because tool rows may be hidden from ordinary actor
// reads. EXECUTED is intentionally recognized as well as EXECUTING: the recovery worker can cross
// its atomic result-commit boundary between the stale live settlement's false return and this
// readback.
func (r *agentRuntime[U]) inlineFailureSettlementLostToRecovery(ctx context.Context, messageID uint64) bool {
	if r == nil || r.persistence == nil || messageID == 0 {
		return false
	}
	row, err := r.persistence.GetChatMessageByIDWithOptions(ctx, messageID, common_persistence.ChatMessageReadOptions{
		VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil || row == nil || row.ToolCall == nil {
		return false
	}
	if row.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL ||
		!isPersistenceInlineApprovalStatus(row.ToolCall.ApprovalStatus) {
		return false
	}
	switch row.ToolCall.ExecutionState {
	case proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING:
		return row.ToolCall.ExecutionLeaseOwner != nil &&
			strings.TrimSpace(*row.ToolCall.ExecutionLeaseOwner) != "" &&
			row.ToolCall.ExecutionLeaseExpiresAt != nil
	case proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED:
		// CommitClaimedToolCallResult sets ExecutedAt in the same transaction as the paired
		// TOOL_RESULT, so this marker proves a recovery owner already completed settlement.
		return row.ToolCall.ExecutedAt != nil
	default:
		return false
	}
}

func isPersistenceInlineApprovalStatus(status proto_models.ToolCallApprovalStatus) bool {
	return status == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_UNSPECIFIED ||
		status == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED
}

func (r *agentRuntime[U]) markRunRetryable(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause error) {
	runID := run.ID
	reason := classifyRunStatusReason(cause)
	swapped, err := r.persistence.CompareAndSetChatAgentRunStatus(
		ctx, runID,
		[]proto_models.ChatAgentRunStatus{proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING},
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
		reason,
	)
	if err != nil {
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Error("agent runtime: failed to mark run retryable after inline tool error",
			zap.String(logkeys.Stage, "mark_agent_run_retryable"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE),
			zap.Error(err), zap.NamedError(appLogger.KeyCause, cause))
		return
	}
	if swapped {
		r.publishRunTransitionAfterCommit(ctx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE, reason)
	}
}

// failRun terminally fails a run on a non-recoverable cause from any non-terminal state, such as a
// captured enabled tool or agent type disappearing from the current registry.
func (r *agentRuntime[U]) failRun(ctx context.Context, run *common_persistence_models.ChatAgentRun, cause error) {
	r.terminalizeRunWithNotice(ctx, run, cause, "settle_resumability_failure", nil,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED, classifyRunStatusReason(cause))
}

// stopRunForBudget ends a run that spent its cumulative work budget. A run paused for approval is
// left alone: its budget is re-checked when it resumes.
func (r *agentRuntime[U]) stopRunForBudget(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	cause *commonai.RunBudgetExceededError,
) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.recoveryPolicy().settleTimeout)
	defer cancel()
	r.terminalizeRunWithNotice(settleCtx, run, cause, "settle_agent_run_budget",
		common_persistence.DrivableChatAgentRunStatuses(),
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED, classifyRunStatusReason(cause))
}

// terminalizeRunWithNotice is the one durable commit behind every runtime-decided terminal stop:
// close any dangling stream, move the run to its terminal status, write the visible notice for that
// ending (the generic SYSTEM_EVENT, or a delegated child's broadcast notice), and settle delegation,
// all in one transaction. With a drive token in ctx the transition is fenced; scanner/tool paths
// rely on their own token check immediately before calling this primitive. The notice used to be a
// separate best-effort write after the transition, which a crash in between silently dropped.
func (r *agentRuntime[U]) terminalizeRunWithNotice(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	cause error,
	stage string,
	fromStatuses []proto_models.ChatAgentRunStatus,
	toStatus proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
) {
	if run == nil {
		return
	}
	terminalized, noticeID, err := r.persistence.TerminalizeChatAgentRunWithNotice(ctx, run.ID, fromStatuses, toStatus, reason)
	if err != nil {
		appLogger.For(ctx, r.logger, agentRuntimeLoggerName).Error("agent runtime: failed to terminalize run with notice",
			zap.String(logkeys.Stage, stage),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Stringer(logkeys.Expected, toStatus),
			zap.Error(err), zap.NamedError(appLogger.KeyCause, cause))
		return
	}
	if terminalized {
		r.publishRunTransitionAfterCommit(ctx, run, toStatus, reason)
		r.publishNoticeAfterCommit(ctx, noticeID)
	}
}

// publishNoticeAfterCommit announces the generic notice a terminal write inserted. The notice is
// part of the terminal transaction, so without this publish a client learns only the run state and
// sees the explanation on its next read. A delegated child's notice is published by settlement.
func (r *agentRuntime[U]) publishNoticeAfterCommit(ctx context.Context, noticeID uint64) {
	if r == nil || r.publishRunNotice == nil || noticeID == 0 {
		return
	}
	r.publishRunNotice(ctx, noticeID)
}

func (r *agentRuntime[U]) recordActiveDuration(ctx context.Context, runID uint64, elapsed time.Duration) {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.recoveryPolicy().settleTimeout)
	defer cancel()
	if err := r.persistence.AddChatAgentRunActiveDuration(recordCtx, runID, elapsed); err != nil {
		appLogger.For(recordCtx, r.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to record active duration",
			zap.String(logkeys.Stage, "record_agent_run_active_duration"),
			zap.String(logkeys.Fallback, "retain_previous_active_duration"),
			zap.Uint64(appLogger.KeyAgentRunID, runID), appLogger.DurationMS(appLogger.KeyElapsedMS, elapsed), zap.Error(err))
	}
}

func capRuntimeExecutionError(s string) string {
	const maxLastExecutionError = 2048
	if len(s) > maxLastExecutionError {
		return s[:maxLastExecutionError]
	}
	return s
}

// inferenceRecorder returns the runtime's ai.InferenceRecorder backed by the persistence ledger, or
// nil when persistence is unavailable (which disables ledger recording without failing the loop).
func (r *agentRuntime[U]) inferenceRecorder() commonai.InferenceRecorder {
	if r.persistence == nil {
		return nil
	}
	return &persistenceInferenceRecorder[U]{persistence: r.persistence, logger: r.logger, converter: r.converter, eventPublisher: r.eventPublisher}
}

// isDrivableRunStatus reports whether a run is in a status the continuation loop may drive: only
// RUNNING and RETRYABLE. A paused (WAITING_FOR_APPROVAL) or terminal run is not driven here.
func isDrivableRunStatus(status proto_models.ChatAgentRunStatus) bool {
	return slices.Contains(common_persistence.DrivableChatAgentRunStatuses(), status)
}

// persistenceInferenceRecorder adapts the persistence inference ledger to ai.InferenceRecorder. It
// derives the idempotency CallID from (agent_run_id, agent_step_index, purpose) so a re-driven step
// does not double-count usage (RecordChatInferenceCall is ON CONFLICT(call_id) DO NOTHING and only
// bumps totals on a real insert).
type persistenceInferenceRecorder[U common_service_models.UserLike] struct {
	converter      common_converter.ConverterInterface
	eventPublisher ports.ChatEventPublisher
	persistence    common_persistence.CommonPersistenceInterface
	logger         *zap.Logger
}

// RecordInferenceCall maps an ai.InferenceCallRecord to a ChatInferenceCall ledger row and records it
// idempotently. A recording failure is logged but not returned: accounting must never fail the live
// agent loop.
func (rec *persistenceInferenceRecorder[U]) RecordInferenceCall(ctx context.Context, call commonai.InferenceCallRecord) error {
	if call.DriveCancelled {
		// The tokens a cancelled drive used are billed whatever state its run is in now, usually
		// stopped by a person, so the drive lease does not fence the row.
		ctx = common_context.ClearChatAgentRunDriveLease(ctx)
	}
	row := &common_persistence_models.ChatInferenceCall{
		OrganizationID:      int64(call.OrganizationID),
		ChannelID:           call.ChannelID,
		AgentType:           call.AgentType,
		CallID:              call.CallID(),
		Purpose:             call.Purpose,
		ProviderID:          call.ProviderID,
		ProviderModelID:     call.ProviderModelID,
		FinishReason:        call.FinishReason,
		LatencyMs:           call.LatencyMs,
		InputTokens:         int64(call.InputTokens),
		InputImages:         int64(call.InputImages),
		OutputTokens:        int64(call.OutputTokens),
		TotalTokens:         int64(call.TotalTokens),
		ThoughtsTokens:      int64(call.ThoughtsTokens),
		CachedContentTokens: int64(call.CachedContentTokens),
		AgentStepIndex:      new(call.AgentStepIndex),
	}
	// The run resolved its model from an exact inference-catalog row, so stamp that row id verbatim
	// rather than looking the (provider, model id) pair up again. The best-effort lookup remains for
	// calls that carry no exact id (test/custom generators); an uncatalogued model or a backend that
	// cannot resolve leaves the column NULL and never fails the write.
	if call.InferenceModelID != 0 {
		inferenceModelID := call.InferenceModelID
		row.InferenceModelID = &inferenceModelID
	} else {
		row.InferenceModelID = common_persistence.StampLedgerInferenceModelID(ctx, rec.persistence, call.ProviderID, call.ProviderModelID)
	}
	if call.AgentRunID != 0 {
		runID := call.AgentRunID
		row.AgentRunID = &runID
	}
	if call.AgentSessionID != 0 {
		sessionID := call.AgentSessionID
		row.AgentSessionID = &sessionID
	}
	if call.AgentSessionConfigVersion != 0 {
		configVersion := call.AgentSessionConfigVersion
		row.AgentSessionConfigVersion = &configVersion
	}
	if call.TriggeringUserID != 0 {
		triggeringUserID := uint64(call.TriggeringUserID)
		row.TriggeringUserID = &triggeringUserID
		row.ActorUserID = &triggeringUserID
		if call.InferenceActorUserID != 0 {
			row.ActorUserID = new(uint64(call.InferenceActorUserID))
		}
	} else if user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx); found && user != nil && (*user).GetID() != 0 {
		actorUserID := uint64((*user).GetID())
		row.ActorUserID = &actorUserID
	}
	if call.MessageID != 0 {
		msgID := call.MessageID
		row.MessageID = &msgID
	}
	if call.FinishMessage != "" {
		msg := call.FinishMessage
		row.FinishMessage = &msg
	}
	if err := chatpkg.RecordInferenceCallWithEvent(ctx, rec.persistence, rec.converter, rec.eventPublisher, rec.logger, row); err != nil {
		appLogger.For(ctx, rec.logger, agentRuntimeLoggerName).Error("agent runtime: failed to record inference call",
			zap.String(logkeys.Stage, "record_agent_inference_call"),
			zap.Uint64(appLogger.KeyAgentRunID, call.AgentRunID),
			zap.Uint64(appLogger.KeyMessageID, call.MessageID),
			zap.String(appLogger.KeyInferenceCallID, row.CallID),
			zap.Error(err))
	}
	return nil
}
