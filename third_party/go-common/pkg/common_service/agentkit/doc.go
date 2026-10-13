// Package agentkit provides the generic, app-independent plumbing that every
// application built on go-common's first-class Agent API otherwise copies into its
// own agentmanager package: strict tool-argument decoding, tool result and
// domain-error mapping, a scope-bounded read executor with built-in metrics and
// tracing, a TTL-cached triggering-user (actor) context, a registration harness for
// agent types and tools, and a per-key agent-start throttle.
//
// The goal is that an application's agentmanager shrinks to just its domain content:
// the AgentSpecs it registers, the tool schemas it exposes, and the executors that
// read its own systems. Everything cross-cutting lives here.
//
// See the [Workflow agents guide] for lifecycle callbacks, transactional progress,
// optional policies, worker composition, and browser observation/upload adapters.
//
// See [Shared agent image tools] for bounded attachment reads, image preparation,
// multipart results, and scoped result execution.
//
// [Workflow agents guide]: ../../../docs/backend/external/workflow-agents.md
// [Shared agent image tools]: ../../../docs/backend/external/agent-image-tools.md
//
// # Dependency inversion
//
// agentkit never imports an application's error, model, or metrics packages. The
// two seams an app must supply are inverted:
//
//   - Domain-visible error classification: pass a predicate
//     func(error) bool (typically an errors.As over the app's domain error types)
//     to ResultForError / ScopedRead.DomainVisible. A *ToolError returned by an
//     executor is always treated as model-visible regardless of the predicate.
//     Tool-error codes and metric outcomes are typed enums (ErrorCode / ToolOutcome),
//     not raw strings; the JSON wire fields still carry the enum's stable name.
//   - Metrics: implement ToolMetricsRecorder over the app's concrete metrics
//     handler and hand it to an Observer. Tracing uses go.opentelemetry.io/otel
//     directly and needs no app wiring.
//
// # Example
//
// A tool executor decodes its arguments, then runs inside RunScopedRead, which owns
// metrics, tracing, actor attachment, scope authorization, and error mapping:
//
//	type describeArgs struct {
//		Instructions string `json:"instructions,omitempty"`
//	}
//
//	func (m *Manager) ExecuteDescribe(ctx context.Context, inv ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
//		started := time.Now()
//		args, err := agentkit.DecodeToolArgs[describeArgs](inv.ArgumentsJSON)
//		if err != nil {
//			res := agentkit.ErrorResult(toolName, rendererKey, agentkit.GenericErrorCodeInvalidArguments, err.Error())
//			m.observer.RecordMetric(agentType, toolName, started, res, nil)
//			return res, nil
//		}
//		return agentkit.RunScopedRead(ctx, agentkit.ScopedRead[*postScope, describeArgs]{
//			ScopedReadOptions: agentkit.ScopedReadOptions[*postScope, describeArgs]{
//				Observer:    m.observer,
//				AgentType:   agentType,
//				ToolName:    toolName,
//				RendererKey: rendererKey,
//				Invocation:  inv,
//				Args:        args,
//				AttachActor: func(c context.Context, iv ports.AgentToolInvocation) (context.Context, error) {
//					return m.actors.Attach(c, iv.TriggeringUserID)
//				},
//				ResolveScope:  m.resolvePostScope,     // app: load + authorize
//				DomainVisible: m.isDomainVisibleError, // app predicate
//			},
//			Read: m.readPostImages, // app: the actual read
//		})
//	}

// ScopedRead and ScopedResultRead embed the same ScopedReadOptions value. Keyed
// literals put common hooks in ScopedReadOptions and keep the typed Read hook
// outside; field assignments such as read.Args continue to use promoted fields.
//
// Registration wires tools and agents against the go-common handler at startup:
//
//	registrar, err := agentkit.NewRegistrar(agentkit.RegistrarDependencies{
//		CommonRuntime:       commonServiceHandler, // satisfies agentkit.CommonRuntime
//		BotUsers:            botUserLookup,
//		ModelRuntimeFactory: modelRuntimeFactory,
//		Planner:             genbridge.NewControlPlanePlanner(controlPlane),
//		Tools:               m.ToolDefinitions(),
//		Agents:              m.agentSpecs(),        // []agentkit.AgentSpec
//	})
//	if err != nil {
//		return err
//	}
//	if err := registrar.Register(ctx); err != nil {
//		return err
//	}
//	// Later, before starting a run: registrar.RequireEnabled(agentType).
//
// Register is strictly two-phase: a plan phase performs every read and validation
// (bot-user lookups, per-agent generation plans) without mutating anything, then an
// apply phase wires the Planner and registers tools and agents in memory only. A
// failed Register therefore leaves the process untouched and can simply be retried;
// a successful one wires the planner for you, so a packaged startup never reaches
// serving traffic with a runtime that cannot plan a model. Applications that do not
// use the registrar must call commonServiceHandler.SetAgentGenerationPlanner
// themselves and then prove their routes with ValidateAgentRoutes after registering
// agents.
//
// The actor context and start throttle are independent helpers:
//
//	actors := agentkit.NewActorContext[app.ApplicationUser](lookup, cacheTTL, cacheMax)
//	throttle := agentkit.NewStartThrottle[uint64](window, limit)
//	if allowed, retryAfter := throttle.Allow(userID); !allowed {
//		return rateLimitError(agentkit.RetryAfterSeconds(retryAfter))
//	}
package agentkit
