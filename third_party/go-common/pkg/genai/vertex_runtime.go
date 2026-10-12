package genai

import (
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/genai/partmetadata"
	"github.com/firebase/genkit/go/ai"
	coreapi "github.com/firebase/genkit/go/core/api"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	googlegenai "google.golang.org/genai"
)

// vertexAIAPIKeyPlugin defines structured state used in the GenAI model resolution and runtime construction flow.
//
// Used in flow:
// - vertexAIAPIKeyPlugin is read or populated by backend components in this package to move requests through this stage.
type vertexAIAPIKeyPlugin struct {
	APIKey    string
	ProjectID string
	Location  string

	mu      sync.Mutex
	initted bool
	client  *googlegenai.Client

	// mediaHTTPTransport is an explicit test seam. Production always uses the hardened direct
	// transport built by newVertexAIHTTPMediaClient.
	mediaHTTPTransport http.RoundTripper
}

const (
	vertexAIToolResponseOutputSynthesizedMetadata       = "go-common.tool_response_output_synthesized"
	vertexAIMaxDownloadedMediaBytes               int64 = 20 * 1024 * 1024
	vertexAIMaxTotalDownloadedMediaBytes          int64 = 50 * 1024 * 1024
	vertexAIMaxHTTPMediaParts                           = 4096
	vertexAIMaxHTTPMediaDownloads                       = 64
	vertexAIMaxHTTPMediaRedirects                       = 5
	vertexAIHTTPMediaDownloadTimeout                    = 30 * time.Second
)

// newVertexAIAPIKeyPlugin performs its package-specific work in the GenAI model resolution and runtime construction flow.
//
// Parameters:
// - apiKey (string): Supplies input consumed by newVertexAIAPIKeyPlugin during this step of the flow.
// - projectID (string): Supplies the explicit Vertex AI project, when configured.
// - location (string): Supplies the explicit Vertex AI location, when configured.
func newVertexAIAPIKeyPlugin(apiKey string, projectID string, location string) *vertexAIAPIKeyPlugin {
	return &vertexAIAPIKeyPlugin{
		APIKey:    strings.TrimSpace(apiKey),
		ProjectID: strings.TrimSpace(projectID),
		Location:  strings.TrimSpace(location),
	}
}

// Name performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (p *vertexAIAPIKeyPlugin) Name() string {
	return vertexAIProviderName
}

// Init performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (p *vertexAIAPIKeyPlugin) Init(ctx context.Context) []coreapi.Action {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.initted {
		panic("vertexAIAPIKeyPlugin.Init already called")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	client, err := googlegenai.NewClient(ctx, &googlegenai.ClientConfig{
		Backend:  googlegenai.BackendVertexAI,
		APIKey:   p.APIKey,
		Project:  p.ProjectID,
		Location: p.Location,
		HTTPClient: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	})
	if err != nil {
		panic(fmt.Errorf("vertexAIAPIKeyPlugin.Init: %w", err))
	}

	p.client = client
	p.initted = true
	return nil
}

// ListActions performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (p *vertexAIAPIKeyPlugin) ListActions(context.Context) []coreapi.ActionDesc {
	return []coreapi.ActionDesc{}
}

// ResolveAction performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (p *vertexAIAPIKeyPlugin) ResolveAction(atype coreapi.ActionType, name string) coreapi.Action {
	if atype != coreapi.ActionTypeModel || !p.initted {
		return nil
	}

	modelID := strings.TrimSpace(strings.TrimPrefix(name, vertexAIProviderName+"/"))
	if modelID == "" || !vertexAIModelIDSupportsGenerateContent(modelID) {
		return nil
	}
	requestModelID, ok := p.vertexAIRequestModelID(modelID)
	if !ok {
		return nil
	}

	modelFn := func(ctx context.Context, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
		if p.client == nil {
			return nil, fmt.Errorf("vertex ai client is not initialized")
		}

		contents, cfg, err := buildVertexAIGenerateRequest(req)
		if err != nil {
			return nil, err
		}

		if cb != nil {
			cfg = vertexAIConfigWithCompleteFunctionCalls(cfg)
			var accumulator vertexAIStreamAccumulator
			for resp, err := range p.client.Models.GenerateContentStream(ctx, requestModelID, contents, cfg) {
				if err != nil {
					return nil, err
				}
				accumulator.add(resp)
				chunk := vertexAIResponseToModelResponseChunk(resp)
				if chunk == nil || len(chunk.Content) == 0 {
					continue
				}
				if err := cb(ctx, chunk); err != nil {
					return nil, err
				}
			}
			return accumulator.modelResponse(req), nil
		}

		resp, err := p.client.Models.GenerateContent(ctx, requestModelID, contents, cfg)
		if err != nil {
			return nil, err
		}
		return vertexAIResponseToModelResponse(req, resp), nil
	}

	// Vertex FileData only accepts provider-supported URIs such as gs://. Inline
	// ordinary HTTP(S) media before request translation, including media nested
	// inside multipart function responses.
	modelFn = vertexAIDownloadRequestMediaWithTransport(modelFn, p.mediaHTTPTransport)

	model := ai.NewModel(
		coreapi.NewName(vertexAIProviderName, modelID),
		new(vertexAIModelOptions(modelID)),
		modelFn,
	)
	if model == nil {
		return nil
	}
	return model.(coreapi.Action)
}

// vertexAIRequestModelID returns the model resource sent to the Google Gen AI SDK. The SDK
// interprets a short tuned endpoint such as "endpoints/123" as a publisher/model pair, so it must
// be expanded before the request. Express-mode clients have no project/location with which to
// construct that resource and therefore cannot safely resolve the short form.
func (p *vertexAIAPIKeyPlugin) vertexAIRequestModelID(modelID string) (string, bool) {
	if !strings.HasPrefix(modelID, "endpoints/") {
		return modelID, true
	}

	projectID := strings.TrimSpace(p.ProjectID)
	location := strings.TrimSpace(p.Location)
	if p.client != nil {
		clientConfig := p.client.ClientConfig()
		if configuredProject := strings.TrimSpace(clientConfig.Project); configuredProject != "" {
			projectID = configuredProject
		}
		if configuredLocation := strings.TrimSpace(clientConfig.Location); configuredLocation != "" {
			location = configuredLocation
		}
	}
	if projectID == "" || location == "" {
		return "", false
	}
	return fmt.Sprintf("projects/%s/locations/%s/%s", projectID, location, modelID), true
}

// vertexAIConfigWithCompleteFunctionCalls disables incremental FunctionCall.PartialArgs for
// streaming requests. Genkit's ToolRequest representation expects one complete argument object;
// asking Vertex for partial JSON-path fragments would otherwise turn one logical call into several
// incomplete requests in the accumulated final response.
func vertexAIConfigWithCompleteFunctionCalls(cfg *googlegenai.GenerateContentConfig) *googlegenai.GenerateContentConfig {
	if cfg == nil || cfg.ToolConfig == nil || cfg.ToolConfig.FunctionCallingConfig == nil ||
		cfg.ToolConfig.FunctionCallingConfig.StreamFunctionCallArguments == nil ||
		!*cfg.ToolConfig.FunctionCallingConfig.StreamFunctionCallArguments {
		return cfg
	}
	cloned := *cfg
	toolConfig := *cfg.ToolConfig
	functionConfig := *cfg.ToolConfig.FunctionCallingConfig
	disabled := false
	functionConfig.StreamFunctionCallArguments = &disabled
	toolConfig.FunctionCallingConfig = &functionConfig
	cloned.ToolConfig = &toolConfig
	return &cloned
}

func vertexAIDownloadRequestMediaWithTransport(next ai.ModelFunc, transport http.RoundTripper) ai.ModelFunc {
	return func(ctx context.Context, input *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
		cloned := cloneVertexAIModelRequest(input)
		if cloned != nil {
			if ctx == nil {
				ctx = context.Background()
			}
			downloadCtx, cancel := context.WithTimeout(ctx, vertexAIHTTPMediaDownloadTimeout)
			defer cancel()
			downloader := newVertexAIHTTPMediaDownloader(transport)
			for _, message := range cloned.Messages {
				if message == nil {
					continue
				}
				if err := downloader.inlineParts(downloadCtx, message.Content); err != nil {
					return nil, err
				}
			}
		}
		return next(ctx, cloned, cb)
	}
}

func cloneVertexAIModelRequest(input *ai.ModelRequest) *ai.ModelRequest {
	if input == nil {
		return nil
	}
	cloned := *input
	cloned.Messages = make([]*ai.Message, len(input.Messages))
	seenParts := make(map[*ai.Part]*ai.Part)
	for i, message := range input.Messages {
		if message == nil {
			continue
		}
		messageClone := message.Clone()
		messageClone.Content = make([]*ai.Part, len(message.Content))
		for j, part := range message.Content {
			messageClone.Content[j] = cloneVertexAIPartWithSeen(part, seenParts)
		}
		cloned.Messages[i] = messageClone
	}
	return &cloned
}

func cloneVertexAIPart(part *ai.Part) *ai.Part {
	return cloneVertexAIPartWithSeen(part, make(map[*ai.Part]*ai.Part))
}

func cloneVertexAIPartWithSeen(part *ai.Part, seen map[*ai.Part]*ai.Part) *ai.Part {
	if part == nil {
		return nil
	}
	if cloned, ok := seen[part]; ok {
		return cloned
	}
	cloned := part.Clone()
	seen[part] = cloned
	if part.ToolResponse != nil {
		response := *part.ToolResponse
		response.Content = make([]*ai.Part, len(part.ToolResponse.Content))
		for i, nested := range part.ToolResponse.Content {
			response.Content[i] = cloneVertexAIPartWithSeen(nested, seen)
		}
		cloned.ToolResponse = &response
	}
	return cloned
}

type vertexAIHTTPMediaDownloader struct {
	// newClient builds the HTTP client for one download, bound to that part's network policy.
	// Parts in one request can carry different policies, so no client is shared across them.
	newClient       func(vertexAIHTTPMediaNetworkPolicy) *http.Client
	maxParts        int
	maxDownloads    int
	maxPartBytes    int64
	maxTotalBytes   int64
	downloads       int
	visitedParts    int
	downloadedBytes int64
}

func newVertexAIHTTPMediaDownloader(transport http.RoundTripper) *vertexAIHTTPMediaDownloader {
	return &vertexAIHTTPMediaDownloader{
		newClient: func(policy vertexAIHTTPMediaNetworkPolicy) *http.Client {
			return newVertexAIHTTPMediaClient(transport, policy)
		},
		maxParts:      vertexAIMaxHTTPMediaParts,
		maxDownloads:  vertexAIMaxHTTPMediaDownloads,
		maxPartBytes:  vertexAIMaxDownloadedMediaBytes,
		maxTotalBytes: vertexAIMaxTotalDownloadedMediaBytes,
	}
}

func (d *vertexAIHTTPMediaDownloader) inlineParts(ctx context.Context, parts []*ai.Part) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for i, part := range parts {
		if part == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("vertex ai: HTTP media traversal canceled: %w", err)
		}
		if d.visitedParts >= d.maxParts {
			return fmt.Errorf("vertex ai: HTTP media part count exceeds %d", d.maxParts)
		}
		d.visitedParts++
		if part.IsMedia() && vertexAIShouldDownloadHTTPMedia(part.Text) {
			downloaded, err := d.download(ctx, part)
			if err != nil {
				return err
			}
			parts[i] = downloaded
			part = downloaded
		}
		if part.ToolResponse != nil {
			if err := d.inlineParts(ctx, part.ToolResponse.Content); err != nil {
				return err
			}
		}
	}
	return nil
}

func vertexAIShouldDownloadHTTPMedia(value string) bool {
	u, err := url.Parse(strings.TrimSpace(value))
	if err != nil || (!strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https")) {
		return false
	}
	switch strings.ToLower(u.Hostname()) {
	case "generativelanguage.googleapis.com", "www.youtube.com", "youtube.com", "youtu.be":
		return false
	default:
		return true
	}
}

func (d *vertexAIHTTPMediaDownloader) download(ctx context.Context, part *ai.Part) (*ai.Part, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	parsed, err := url.Parse(strings.TrimSpace(part.Text))
	displayURL := vertexAIHTTPMediaURLDisplay(parsed)
	if err != nil {
		return nil, fmt.Errorf("vertex ai: invalid HTTP media URL %q", displayURL)
	}
	policy := vertexAIHTTPMediaNetworkPolicy{allowPrivate: partmetadata.IsTrustedHTTPSource(part.Metadata)}
	if err := vertexAIValidateHTTPMediaURL(ctx, parsed, policy, false, net.DefaultResolver); err != nil {
		return nil, err
	}
	if d == nil || d.newClient == nil {
		return nil, fmt.Errorf("vertex ai: HTTP media downloader is not configured")
	}
	if d.downloads >= d.maxDownloads {
		return nil, fmt.Errorf("vertex ai: HTTP media download count exceeds %d", d.maxDownloads)
	}
	remainingBytes := d.maxTotalBytes - d.downloadedBytes
	if remainingBytes <= 0 {
		return nil, fmt.Errorf("vertex ai: HTTP media exceeds %d-byte aggregate limit", d.maxTotalBytes)
	}
	readLimit := min(d.maxPartBytes, remainingBytes)
	if readLimit <= 0 {
		return nil, fmt.Errorf("vertex ai: HTTP media download limit is invalid")
	}
	d.downloads++

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("vertex ai: invalid HTTP media URL %q", displayURL)
	}
	response, err := d.newClient(policy).Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return nil, fmt.Errorf("vertex ai: download media %q: %w", displayURL, vertexAIHTTPMediaErrorCause(err))
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"vertex ai: download media %q returned status %d (%s)",
			displayURL,
			response.StatusCode,
			http.StatusText(response.StatusCode),
		)
	}
	if response.ContentLength > d.maxPartBytes {
		return nil, fmt.Errorf("vertex ai: media %q exceeds %d-byte per-part limit", displayURL, d.maxPartBytes)
	}
	if response.ContentLength > remainingBytes {
		return nil, fmt.Errorf("vertex ai: media %q exceeds %d-byte aggregate limit", displayURL, d.maxTotalBytes)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, readLimit+1))
	if err != nil {
		return nil, fmt.Errorf("vertex ai: read media %q: %w", displayURL, err)
	}
	if int64(len(data)) > d.maxPartBytes {
		return nil, fmt.Errorf("vertex ai: media %q exceeds %d-byte per-part limit", displayURL, d.maxPartBytes)
	}
	if int64(len(data)) > remainingBytes {
		return nil, fmt.Errorf("vertex ai: media %q exceeds %d-byte aggregate limit", displayURL, d.maxTotalBytes)
	}
	d.downloadedBytes += int64(len(data))

	contentType := strings.TrimSpace(part.ContentType)
	if contentType == "" {
		contentType, _, _ = strings.Cut(response.Header.Get("Content-Type"), ";")
		contentType = strings.TrimSpace(contentType)
	}
	if contentType == "" {
		contentType = vertexAIMIMEType("")
	}
	downloaded := part.Clone()
	downloaded.ContentType = contentType
	downloaded.Text = "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(data)
	return downloaded, nil
}

type vertexAIHTTPMediaResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type vertexAIHTTPMediaNetworkPolicy struct {
	allowPrivate bool
}

// vertexAIHTTPMediaRoundTripper validates every request and redirect hop of one download against
// the policy of the part being downloaded.
type vertexAIHTTPMediaRoundTripper struct {
	next        http.RoundTripper
	resolver    vertexAIHTTPMediaResolver
	resolveHost bool
	policy      vertexAIHTTPMediaNetworkPolicy
}

func (t *vertexAIHTTPMediaRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, fmt.Errorf("vertex ai: invalid HTTP media request")
	}
	if err := vertexAIValidateHTTPMediaURL(
		request.Context(),
		request.URL,
		t.policy,
		t.resolveHost,
		t.resolver,
	); err != nil {
		return nil, err
	}
	return t.next.RoundTrip(request)
}

// vertexAIHTTPMediaDialer pins each connection of one download to addresses its policy allows.
type vertexAIHTTPMediaDialer struct {
	resolver    vertexAIHTTPMediaResolver
	dialContext func(context.Context, string, string) (net.Conn, error)
	policy      vertexAIHTTPMediaNetworkPolicy
}

func (d *vertexAIHTTPMediaDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("vertex ai: invalid HTTP media network address")
	}
	addresses, err := vertexAIResolveHTTPMediaHost(
		ctx,
		d.resolver,
		host,
		network,
		d.policy,
	)
	if err != nil {
		return nil, err
	}
	var dialErrors []error
	for _, resolved := range addresses {
		connection, dialErr := d.dialContext(ctx, network, net.JoinHostPort(resolved.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		dialErrors = append(dialErrors, dialErr)
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("vertex ai: connect to HTTP media host: %w", errors.Join(dialErrors...))
}

func newVertexAIHTTPMediaClient(injectedTransport http.RoundTripper, policy vertexAIHTTPMediaNetworkPolicy) *http.Client {
	return newVertexAIHTTPMediaClientWithResolver(injectedTransport, net.DefaultResolver, policy)
}

// newVertexAIHTTPMediaClientWithResolver builds a client for one download whose round tripper,
// dialer, and redirect check are all bound to policy. The production transport is built per
// download with keep-alives disabled, so no connection outlives the policy it was dialed under.
// An injected test transport skips the dialer; the round tripper then resolves and validates the
// host itself under the same policy.
func newVertexAIHTTPMediaClientWithResolver(
	injectedTransport http.RoundTripper,
	resolver vertexAIHTTPMediaResolver,
	policy vertexAIHTTPMediaNetworkPolicy,
) *http.Client {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	transport := injectedTransport
	resolveHost := true
	if transport == nil {
		networkDialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		mediaDialer := &vertexAIHTTPMediaDialer{resolver: resolver, dialContext: networkDialer.DialContext, policy: policy}
		// Do not pool a trusted private-storage connection for a later untrusted URL with the same
		// hostname. Every request and redirect must pass through the policy-aware dialer.
		transport = &http.Transport{
			Proxy:                 nil,
			DialContext:           mediaDialer.DialContext,
			DisableKeepAlives:     true,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          32,
			MaxIdleConnsPerHost:   4,
			MaxConnsPerHost:       4,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
			ExpectContinueTimeout: time.Second,
		}
		resolveHost = false
	}
	return &http.Client{
		Transport: &vertexAIHTTPMediaRoundTripper{
			next:        transport,
			resolver:    resolver,
			resolveHost: resolveHost,
			policy:      policy,
		},
		Timeout: vertexAIHTTPMediaDownloadTimeout,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= vertexAIMaxHTTPMediaRedirects {
				return fmt.Errorf("vertex ai: HTTP media redirect count exceeds %d", vertexAIMaxHTTPMediaRedirects)
			}
			if policy.allowPrivate && len(via) > 0 && !vertexAISameHTTPMediaOrigin(via[0].URL, request.URL) {
				return fmt.Errorf(
					"vertex ai: trusted media redirect from %q to %q is not same-origin",
					vertexAIHTTPMediaURLDisplay(via[0].URL),
					vertexAIHTTPMediaURLDisplay(request.URL),
				)
			}
			return vertexAIValidateHTTPMediaURL(request.Context(), request.URL, policy, false, resolver)
		},
	}
}

func vertexAIValidateHTTPMediaURL(
	ctx context.Context,
	mediaURL *url.URL,
	policy vertexAIHTTPMediaNetworkPolicy,
	resolveHost bool,
	resolver vertexAIHTTPMediaResolver,
) error {
	displayURL := vertexAIHTTPMediaURLDisplay(mediaURL)
	if mediaURL == nil || (!strings.EqualFold(mediaURL.Scheme, "http") && !strings.EqualFold(mediaURL.Scheme, "https")) {
		return fmt.Errorf("vertex ai: HTTP media URL %q must use http or https", displayURL)
	}
	if mediaURL.User != nil {
		return fmt.Errorf("vertex ai: HTTP media URL %q must not contain userinfo", displayURL)
	}
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(mediaURL.Hostname())), ".")
	if host == "" {
		return fmt.Errorf("vertex ai: HTTP media URL %q has no host", displayURL)
	}
	if vertexAIIsMetadataHostname(host) {
		return fmt.Errorf("vertex ai: HTTP media URL %q targets a metadata service", displayURL)
	}
	if address, err := netip.ParseAddr(host); err == nil {
		if !vertexAIHTTPMediaIPAllowed(address, policy.allowPrivate) {
			return fmt.Errorf("vertex ai: HTTP media URL %q targets a disallowed network address", displayURL)
		}
		return nil
	}
	if !policy.allowPrivate && (host == "localhost" || strings.HasSuffix(host, ".localhost")) {
		return fmt.Errorf("vertex ai: HTTP media URL %q targets a disallowed network address", displayURL)
	}
	if resolveHost {
		if _, err := vertexAIResolveHTTPMediaHost(ctx, resolver, host, "tcp", policy); err != nil {
			return fmt.Errorf("vertex ai: HTTP media URL %q has no allowed destination: %w", displayURL, err)
		}
	}
	return nil
}

func vertexAIResolveHTTPMediaHost(
	ctx context.Context,
	resolver vertexAIHTTPMediaResolver,
	host string,
	network string,
	policy vertexAIHTTPMediaNetworkPolicy,
) ([]netip.Addr, error) {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if address, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		address = address.Unmap()
		if !vertexAIHTTPMediaIPAllowed(address, policy.allowPrivate) {
			return nil, fmt.Errorf("destination address is disallowed")
		}
		return []netip.Addr{address}, nil
	}
	lookupNetwork := "ip"
	if strings.HasSuffix(network, "4") {
		lookupNetwork = "ip4"
	} else if strings.HasSuffix(network, "6") {
		lookupNetwork = "ip6"
	}
	addresses, err := resolver.LookupNetIP(ctx, lookupNetwork, host)
	if err != nil {
		return nil, fmt.Errorf("resolve destination host: %w", err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("destination host resolved no addresses")
	}
	result := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !vertexAIHTTPMediaIPAllowed(address, policy.allowPrivate) {
			return nil, fmt.Errorf("destination host resolved a disallowed address")
		}
		result = append(result, address)
	}
	return result, nil
}

func vertexAIHTTPMediaIPAllowed(address netip.Addr, allowPrivate bool) bool {
	address = address.Unmap()
	if !address.IsValid() || address.Zone() != "" || address.IsUnspecified() || address.IsMulticast() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
		return false
	}
	if allowPrivate && (address.IsPrivate() || address.IsLoopback()) {
		return true
	}
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() {
		return false
	}
	for _, prefix := range vertexAIBlockedHTTPMediaPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var vertexAIBlockedHTTPMediaPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func vertexAIIsMetadataHostname(host string) bool {
	switch strings.TrimSuffix(strings.ToLower(host), ".") {
	case "metadata", "metadata.google.internal", "metadata.google", "instance-data", "instance-data.ec2.internal":
		return true
	default:
		return false
	}
}

func vertexAISameHTTPMediaOrigin(first, second *url.URL) bool {
	if first == nil || second == nil || !strings.EqualFold(first.Scheme, second.Scheme) ||
		!strings.EqualFold(strings.TrimSuffix(first.Hostname(), "."), strings.TrimSuffix(second.Hostname(), ".")) {
		return false
	}
	return vertexAIHTTPMediaPort(first) == vertexAIHTTPMediaPort(second)
}

func vertexAIHTTPMediaPort(mediaURL *url.URL) string {
	if mediaURL == nil {
		return ""
	}
	if port := mediaURL.Port(); port != "" {
		return port
	}
	if strings.EqualFold(mediaURL.Scheme, "http") {
		return "80"
	}
	if strings.EqualFold(mediaURL.Scheme, "https") {
		return "443"
	}
	return ""
}

func vertexAIHTTPMediaURLDisplay(mediaURL *url.URL) string {
	if mediaURL == nil {
		return "<invalid>"
	}
	sanitized := &url.URL{
		Scheme: strings.ToLower(mediaURL.Scheme),
		Host:   mediaURL.Host,
	}
	return sanitized.String()
}

func vertexAIHTTPMediaErrorCause(err error) error {
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Err != nil {
		return urlError.Err
	}
	return err
}

// vertexAIStreamAccumulator combines streamed content deltas with terminal
// finish and usage metadata. Vertex commonly sends the latter in a final event
// whose candidate has no Content, so returning only that event loses the model
// output.
type vertexAIStreamAccumulator struct {
	finalResponse  *googlegenai.GenerateContentResponse
	finalCandidate *googlegenai.Candidate
	role           string
	parts          []*googlegenai.Part
	sawContent     bool
}

func (a *vertexAIStreamAccumulator) add(resp *googlegenai.GenerateContentResponse) {
	if resp == nil {
		return
	}
	a.finalResponse = resp
	if len(resp.Candidates) == 0 || resp.Candidates[0] == nil {
		return
	}
	candidate := resp.Candidates[0]
	a.finalCandidate = candidate
	if candidate.Content == nil {
		return
	}
	a.sawContent = true
	if candidate.Content.Role != "" {
		a.role = candidate.Content.Role
	}
	a.parts = append(a.parts, candidate.Content.Parts...)
}

func (a *vertexAIStreamAccumulator) modelResponse(req *ai.ModelRequest) *ai.ModelResponse {
	if a.finalResponse == nil {
		return &ai.ModelResponse{Request: req}
	}

	merged := *a.finalResponse
	if a.finalCandidate != nil {
		candidate := *a.finalCandidate
		if a.sawContent {
			candidate.Content = &googlegenai.Content{
				Role:  a.role,
				Parts: append([]*googlegenai.Part(nil), a.parts...),
			}
		}
		merged.Candidates = append([]*googlegenai.Candidate(nil), a.finalResponse.Candidates...)
		if len(merged.Candidates) == 0 {
			merged.Candidates = []*googlegenai.Candidate{&candidate}
		} else {
			merged.Candidates[0] = &candidate
		}
	}
	return vertexAIResponseToModelResponse(req, &merged)
}

func vertexAIResponseToModelResponseChunk(resp *googlegenai.GenerateContentResponse) *ai.ModelResponseChunk {
	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0] == nil || resp.Candidates[0].Content == nil {
		return nil
	}
	return &ai.ModelResponseChunk{
		Role:    ai.RoleModel,
		Index:   0,
		Content: aiPartsFromVertexAIContent(resp.Candidates[0].Content),
	}
}

// buildVertexAIGenerateRequest performs its package-specific work in the GenAI model resolution and runtime construction flow.
func buildVertexAIGenerateRequest(req *ai.ModelRequest) ([]*googlegenai.Content, *googlegenai.GenerateContentConfig, error) {
	if req == nil {
		return nil, nil, fmt.Errorf("model request is required")
	}

	cfg, err := vertexAIConfigFromRequest(req.Config)
	if err != nil {
		return nil, nil, err
	}
	if cfg.CandidateCount == 0 {
		cfg.CandidateCount = 1
	}
	if cfg.CandidateCount != 1 {
		return nil, nil, fmt.Errorf("vertex ai: multiple response candidates are unsupported (candidateCount=%d)", cfg.CandidateCount)
	}
	for _, tool := range cfg.Tools {
		if tool != nil && len(tool.FunctionDeclarations) > 0 {
			return nil, nil, fmt.Errorf("vertex ai: config-level function declarations are unsupported; register callable functions through the Genkit tools API")
		}
	}
	for _, tool := range req.Tools {
		if tool != nil && !vertexAIValidFunctionName(tool.Name) {
			return nil, nil, fmt.Errorf("vertex ai: invalid function name %q (must start with a letter or underscore, contain only letters, digits, underscores, dots, or dashes, and be at most 64 characters)", tool.Name)
		}
	}

	systemParts := make([]*googlegenai.Part, 0)
	contents := make([]*googlegenai.Content, 0, len(req.Messages))
	for _, msg := range req.Messages {
		if msg == nil {
			continue
		}

		parts, err := vertexAIPartsFromMessageParts(msg.Content)
		if err != nil {
			return nil, nil, err
		}
		if len(parts) == 0 {
			continue
		}

		if msg.Role == ai.RoleSystem {
			systemParts = append(systemParts, parts...)
			continue
		}
		contents = append(contents, googlegenai.NewContentFromParts(parts, vertexAIRoleFromAIMessage(msg.Role)))
	}

	if len(systemParts) > 0 {
		cfg.SystemInstruction = googlegenai.NewContentFromParts(systemParts, googlegenai.RoleUser)
	}

	if cfg.ResponseSchema != nil && cfg.ResponseJsonSchema != nil {
		return nil, nil, fmt.Errorf("vertex ai: responseSchema and responseJsonSchema are mutually exclusive")
	}
	if req.Output != nil {
		if len(req.Output.Schema) > 0 && (cfg.ResponseSchema != nil || cfg.ResponseJsonSchema != nil) {
			return nil, nil, fmt.Errorf("vertex ai: request output schema cannot be combined with a config-level response schema")
		}
		if outputMIMEType := responseMIMEType(req.Output); outputMIMEType != "" {
			cfg.ResponseMIMEType = outputMIMEType
		}
		if len(req.Output.Schema) > 0 && req.Output.Constrained {
			cfg.ResponseJsonSchema = req.Output.Schema
			if cfg.ResponseMIMEType == "" {
				cfg.ResponseMIMEType = "application/json"
			}
		}
	}

	if len(req.Tools) > 0 {
		cfg.Tools = mergeVertexAITools(append(
			append([]*googlegenai.Tool(nil), cfg.Tools...),
			buildVertexAITools(req.Tools)...,
		))
	}
	if req.ToolChoice != "" {
		cfg.ToolConfig, err = vertexAIToolConfig(cfg.ToolConfig, req.ToolChoice, req.Tools)
		if err != nil {
			return nil, nil, err
		}
	}

	return contents, cfg, nil
}

// mergeVertexAITools consolidates function declarations while retaining every
// native hosted tool supplied in GenerateContentConfig.
func mergeVertexAITools(tools []*googlegenai.Tool) []*googlegenai.Tool {
	var declarations []*googlegenai.FunctionDeclaration
	var native []*googlegenai.Tool
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		declarations = append(declarations, tool.FunctionDeclarations...)
		clone := *tool
		clone.FunctionDeclarations = nil
		if hasVertexAINativeTool(&clone) {
			native = append(native, &clone)
		}
	}
	if len(declarations) > 0 {
		native = append([]*googlegenai.Tool{{FunctionDeclarations: declarations}}, native...)
	}
	return native
}

// Keep this list aligned with googlegenai.Tool. The SDK field-coverage test
// catches additions so new native tools cannot be silently discarded.
func hasVertexAINativeTool(tool *googlegenai.Tool) bool {
	return tool.Retrieval != nil || tool.ComputerUse != nil ||
		tool.FileSearch != nil || tool.GoogleSearch != nil ||
		tool.GoogleMaps != nil || tool.CodeExecution != nil ||
		tool.EnterpriseWebSearch != nil || tool.GoogleSearchRetrieval != nil ||
		tool.ParallelAISearch != nil || tool.URLContext != nil ||
		tool.MCPServers != nil || tool.ExaAISearch != nil
}

func vertexAIValidFunctionName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		char := name[i]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_' {
			continue
		}
		if i > 0 && ((char >= '0' && char <= '9') || char == '.' || char == '-') {
			continue
		}
		return false
	}
	return true
}

func vertexAIToolConfig(existing *googlegenai.ToolConfig, choice ai.ToolChoice, tools []*ai.ToolDefinition) (*googlegenai.ToolConfig, error) {
	var mode googlegenai.FunctionCallingConfigMode
	switch choice {
	case ai.ToolChoiceAuto:
		mode = googlegenai.FunctionCallingConfigModeAuto
	case ai.ToolChoiceRequired:
		mode = googlegenai.FunctionCallingConfigModeAny
	case ai.ToolChoiceNone:
		mode = googlegenai.FunctionCallingConfigModeNone
	case "":
		return existing, nil
	default:
		return nil, fmt.Errorf("vertex ai tool choice mode %q is not supported", choice)
	}

	var cfg googlegenai.ToolConfig
	if existing != nil {
		cfg = *existing
	}
	var functionConfig googlegenai.FunctionCallingConfig
	if cfg.FunctionCallingConfig != nil {
		functionConfig = *cfg.FunctionCallingConfig
	}
	functionConfig.Mode = mode
	functionConfig.AllowedFunctionNames = nil
	if mode == googlegenai.FunctionCallingConfigModeAny {
		for _, tool := range tools {
			if tool != nil && strings.TrimSpace(tool.Name) != "" {
				functionConfig.AllowedFunctionNames = append(functionConfig.AllowedFunctionNames, tool.Name)
			}
		}
	}
	cfg.FunctionCallingConfig = &functionConfig
	return &cfg, nil
}

// vertexAIConfigFromRequest performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIConfigFromRequest(raw any) (*googlegenai.GenerateContentConfig, error) {
	if raw == nil {
		return &googlegenai.GenerateContentConfig{}, nil
	}

	switch typed := raw.(type) {
	case googlegenai.GenerateContentConfig:
		cfg := typed
		return &cfg, nil
	case *googlegenai.GenerateContentConfig:
		if typed == nil {
			return &googlegenai.GenerateContentConfig{}, nil
		}
		cfg := *typed
		return &cfg, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal vertex ai config: %w", err)
	}

	var cfg googlegenai.GenerateContentConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal vertex ai config: %w", err)
	}
	return &cfg, nil
}

// vertexAIPartsFromMessageParts performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIPartsFromMessageParts(parts []*ai.Part) ([]*googlegenai.Part, error) {
	result := make([]*googlegenai.Part, 0, len(parts))
	for partIndex, part := range parts {
		if part == nil {
			continue
		}

		var vertexPart *googlegenai.Part
		switch {
		case part.IsMedia():
			mediaPart, err := vertexAIMediaPartFromPart(part)
			if err != nil {
				return nil, fmt.Errorf("vertex ai request part %d: %w", partIndex, err)
			}
			if mediaPart == nil {
				return nil, fmt.Errorf("vertex ai request part %d: media source is empty", partIndex)
			}
			vertexPart = mediaPart
		case part.IsData():
			data, contentType, err := vertexAIInlineDataFromPart(part)
			if err != nil {
				return nil, err
			}
			vertexPart = googlegenai.NewPartFromBytes(data, contentType)
		case part.ToolRequest != nil:
			callPart := googlegenai.NewPartFromFunctionCall(
				part.ToolRequest.Name,
				mapFromAny(part.ToolRequest.Input),
			)
			// Prefer the exact provider ID, including a known empty value, so a signed
			// function-call part is not changed by internal ref normalization. Legacy
			// parts fall back to the Genkit ref.
			if callPart.FunctionCall != nil {
				if providerCallID, known := partmetadata.FunctionCallID(part.Metadata); known {
					callPart.FunctionCall.ID = providerCallID
				} else if part.ToolRequest.Ref != "" {
					callPart.FunctionCall.ID = part.ToolRequest.Ref
				}
			}
			vertexPart = callPart
		case part.ToolResponse != nil:
			responseMap := functionResponseMap(part.ToolResponse.Output)
			outputSynthesized, _ := part.Metadata[vertexAIToolResponseOutputSynthesizedMetadata].(bool)
			responseParts, err := vertexAIFunctionResponseParts(part.ToolResponse.Content, outputSynthesized)
			if err != nil {
				return nil, fmt.Errorf("vertex ai function response %q: %w", part.ToolResponse.Name, err)
			}
			var responsePart *googlegenai.Part
			if len(responseParts) > 0 {
				responsePart = googlegenai.NewPartFromFunctionResponseWithParts(part.ToolResponse.Name, responseMap, responseParts)
			} else {
				responsePart = googlegenai.NewPartFromFunctionResponse(part.ToolResponse.Name, responseMap)
			}
			// The response must use the same exact provider ID as its function call.
			// Legacy parts fall back to the internal Genkit ref.
			if responsePart.FunctionResponse != nil {
				if providerCallID, known := partmetadata.FunctionCallID(part.Metadata); known {
					responsePart.FunctionResponse.ID = providerCallID
				} else if part.ToolResponse.Ref != "" {
					responsePart.FunctionResponse.ID = part.ToolResponse.Ref
				}
			}
			vertexPart = responsePart
		case part.IsCustom():
			vertexPart = vertexAIPartFromCustom(part)
			if vertexPart == nil {
				return nil, fmt.Errorf("vertex ai request part %d: unsupported or malformed custom part", partIndex)
			}
		case part.IsReasoning():
			reasoningPart := googlegenai.NewPartFromText(part.Text)
			reasoningPart.Thought = true
			vertexPart = reasoningPart
		case part.IsText():
			vertexPart = googlegenai.NewPartFromText(part.Text)
		default:
			return nil, fmt.Errorf("vertex ai request part %d: unsupported part kind %d", partIndex, part.Kind)
		}

		if vertexPart == nil {
			continue
		}
		if signature := vertexAIThoughtSignature(part.Metadata); len(signature) > 0 {
			vertexPart.ThoughtSignature = signature
		}
		result = append(result, vertexPart)
	}
	return result, nil
}

func vertexAIPartFromCustom(part *ai.Part) *googlegenai.Part {
	if part == nil || !part.IsCustom() {
		return nil
	}
	if raw, ok := part.Custom["executableCode"].(map[string]any); ok {
		code, _ := raw["code"].(string)
		language, _ := raw["language"].(string)
		if code != "" || language != "" {
			return googlegenai.NewPartFromExecutableCode(code, googlegenai.Language(language))
		}
	}
	if raw, ok := part.Custom["codeExecutionResult"].(map[string]any); ok {
		outcome, _ := raw["outcome"].(string)
		output, _ := raw["output"].(string)
		if outcome != "" || output != "" {
			return googlegenai.NewPartFromCodeExecutionResult(googlegenai.Outcome(outcome), output)
		}
	}
	return nil
}

func vertexAIThoughtSignature(metadata map[string]any) []byte {
	if metadata == nil {
		return nil
	}
	switch signature := metadata["signature"].(type) {
	case []byte:
		return append([]byte(nil), signature...)
	case string:
		decoded, err := base64.StdEncoding.DecodeString(signature)
		if err == nil {
			return decoded
		}
	}
	return nil
}

func vertexAIInlineDataFromPart(part *ai.Part) ([]byte, string, error) {
	if part == nil {
		return nil, "", nil
	}
	value := part.Text
	contentType := strings.TrimSpace(part.ContentType)
	if part.IsData() {
		var err error
		value, err = partdata.String(part)
		if err != nil {
			return nil, "", fmt.Errorf("vertex ai: %w", err)
		}
		contentType = partdata.ContentType(part)
	}
	if common_util.HasDataURIScheme(strings.TrimSpace(value)) {
		return vertexAIDataURIBytes(value, contentType)
	}
	return []byte(value), vertexAIMIMEType(contentType), nil
}

func vertexAIDataURIBytes(value, declaredContentType string) ([]byte, string, error) {
	data, inferredContentType, err := common_util.DecodeDataURI(strings.TrimSpace(value))
	if err != nil {
		return nil, "", err
	}
	declaredContentType, err = vertexAICanonicalMIMEType(declaredContentType)
	if err != nil {
		return nil, "", err
	}
	inferredContentType, err = vertexAICanonicalMIMEType(inferredContentType)
	if err != nil {
		return nil, "", fmt.Errorf("vertex ai data URI: %w", err)
	}
	if declaredContentType != "" && inferredContentType != "" && !strings.EqualFold(declaredContentType, inferredContentType) {
		return nil, "", fmt.Errorf("vertex ai data URI content type %q does not match part content type %q", inferredContentType, declaredContentType)
	}
	if declaredContentType == "" {
		declaredContentType = inferredContentType
	}
	return data, vertexAIMIMEType(declaredContentType), nil
}

func vertexAICanonicalMIMEType(contentType string) (string, error) {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return "", nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType == "" {
		return "", fmt.Errorf("vertex ai content type %q is malformed", contentType)
	}
	return strings.ToLower(mediaType), nil
}

func vertexAIMIMEType(contentType string) string {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return "application/octet-stream"
	}
	return contentType
}

func vertexAIFunctionResponseParts(parts []*ai.Part, skipSynthesizedText bool) ([]*googlegenai.FunctionResponsePart, error) {
	result := make([]*googlegenai.FunctionResponsePart, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			continue
		}
		switch {
		case part.IsText():
			if skipSynthesizedText {
				continue
			}
			return nil, fmt.Errorf("text content must be represented in FunctionResponse.Output")
		case part.IsData():
			data, contentType, err := vertexAIInlineDataFromPart(part)
			if err != nil {
				return nil, err
			}
			result = append(result, googlegenai.NewFunctionResponsePartFromBytes(data, contentType))
		case part.IsMedia():
			if common_util.HasDataURIScheme(strings.TrimSpace(part.Text)) {
				data, contentType, err := vertexAIInlineDataFromPart(part)
				if err != nil {
					return nil, err
				}
				result = append(result, googlegenai.NewFunctionResponsePartFromBytes(data, contentType))
			} else {
				result = append(result, googlegenai.NewFunctionResponsePartFromURI(part.Text, vertexAIMIMEType(part.ContentType)))
			}
		default:
			return nil, fmt.Errorf("unsupported function response part kind %d", part.Kind)
		}
	}
	return result, nil
}

// vertexAIMediaPartFromPart performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIMediaPartFromPart(part *ai.Part) (*googlegenai.Part, error) {
	if part == nil || !part.IsMedia() {
		return nil, nil
	}

	contentType := strings.TrimSpace(part.ContentType)
	value := strings.TrimSpace(part.Text)
	if value == "" {
		return nil, nil
	}

	if common_util.HasDataURIScheme(strings.TrimSpace(value)) {
		payload, resolvedContentType, err := vertexAIDataURIBytes(value, contentType)
		if err != nil {
			return nil, err
		}
		return googlegenai.NewPartFromBytes(payload, resolvedContentType), nil
	}

	return googlegenai.NewPartFromURI(value, vertexAIMIMEType(contentType)), nil
}

// buildVertexAITools performs its package-specific work in the GenAI model resolution and runtime construction flow.
func buildVertexAITools(tools []*ai.ToolDefinition) []*googlegenai.Tool {
	result := make([]*googlegenai.Tool, 0, len(tools))
	for _, tool := range tools {
		if tool == nil || strings.TrimSpace(tool.Name) == "" {
			continue
		}

		declaration := &googlegenai.FunctionDeclaration{
			Name:                 tool.Name,
			Description:          tool.Description,
			ParametersJsonSchema: tool.InputSchema,
		}
		if len(tool.OutputSchema) > 0 {
			declaration.ResponseJsonSchema = tool.OutputSchema
		}

		result = append(result, &googlegenai.Tool{
			FunctionDeclarations: []*googlegenai.FunctionDeclaration{declaration},
		})
	}
	return result
}

// vertexAIToolChoiceMode performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIToolChoiceMode(choice ai.ToolChoice) googlegenai.FunctionCallingConfigMode {
	switch choice {
	case ai.ToolChoiceRequired:
		return googlegenai.FunctionCallingConfigModeAny
	case ai.ToolChoiceNone:
		return googlegenai.FunctionCallingConfigModeNone
	default:
		return googlegenai.FunctionCallingConfigModeAuto
	}
}

// vertexAIRoleFromAIMessage performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIRoleFromAIMessage(role ai.Role) googlegenai.Role {
	if role == ai.RoleModel {
		return googlegenai.RoleModel
	}
	return googlegenai.RoleUser
}

// vertexAIResponseToModelResponse performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIResponseToModelResponse(req *ai.ModelRequest, resp *googlegenai.GenerateContentResponse) *ai.ModelResponse {
	if resp == nil {
		return nil
	}

	response := &ai.ModelResponse{
		Request: req,
		Raw:     resp,
	}

	if len(resp.Candidates) > 0 && resp.Candidates[0] != nil {
		candidate := resp.Candidates[0]
		response.FinishMessage = candidate.FinishMessage
		response.FinishReason = aiFinishReasonFromVertex(candidate.FinishReason)
		if candidate.Content != nil {
			response.Message = &ai.Message{
				Role:    ai.RoleModel,
				Content: aiPartsFromVertexAIContent(candidate.Content),
			}
		}
	} else if resp.PromptFeedback != nil {
		response.FinishReason = ai.FinishReasonBlocked
		response.FinishMessage = resp.PromptFeedback.BlockReasonMessage
	}

	if resp.UsageMetadata != nil {
		response.Usage = &ai.GenerationUsage{
			InputTokens:         int(resp.UsageMetadata.PromptTokenCount),
			OutputTokens:        int(resp.UsageMetadata.CandidatesTokenCount),
			CachedContentTokens: int(resp.UsageMetadata.CachedContentTokenCount),
			ThoughtsTokens:      int(resp.UsageMetadata.ThoughtsTokenCount),
			TotalTokens:         int(resp.UsageMetadata.TotalTokenCount),
		}
	}

	return response
}

// aiPartsFromVertexAIContent performs its package-specific work in the GenAI model resolution and runtime construction flow.
func aiPartsFromVertexAIContent(content *googlegenai.Content) []*ai.Part {
	if content == nil {
		return nil
	}

	result := make([]*ai.Part, 0, len(content.Parts))
	for _, part := range content.Parts {
		if part == nil {
			continue
		}

		emitted := make([]*ai.Part, 0, 2)
		var emittedFunctionCall *ai.Part
		if part.Thought {
			emitted = append(emitted, ai.NewReasoningPart(part.Text, part.ThoughtSignature))
		} else if part.Text != "" {
			emitted = append(emitted, ai.NewTextPart(part.Text))
		}
		if part.FunctionCall != nil {
			requestPart := ai.NewToolRequestPart(&ai.ToolRequest{
				Name:    part.FunctionCall.Name,
				Input:   part.FunctionCall.Args,
				Partial: part.FunctionCall.WillContinue != nil && *part.FunctionCall.WillContinue,
				Ref:     part.FunctionCall.ID,
			})
			requestPart.Metadata = partmetadata.SetFunctionCallID(requestPart.Metadata, part.FunctionCall.ID)
			emitted = append(emitted, requestPart)
			emittedFunctionCall = requestPart
		}
		if part.FunctionResponse != nil {
			emitted = append(emitted, ai.NewToolResponsePart(&ai.ToolResponse{
				Name:    part.FunctionResponse.Name,
				Output:  toolResponseOutput(part.FunctionResponse.Response),
				Ref:     part.FunctionResponse.ID,
				Content: aiPartsFromVertexAIFunctionResponseParts(part.FunctionResponse.Parts),
			}))
		}
		if part.InlineData != nil {
			emitted = append(emitted, ai.NewMediaPart(
				part.InlineData.MIMEType,
				"data:"+part.InlineData.MIMEType+";base64,"+base64.StdEncoding.EncodeToString(part.InlineData.Data),
			))
		}
		if part.FileData != nil {
			emitted = append(emitted, ai.NewMediaPart(
				part.FileData.MIMEType,
				part.FileData.FileURI,
			))
		}
		if part.CodeExecutionResult != nil {
			emitted = append(emitted, ai.NewCustomPart(map[string]any{
				"codeExecutionResult": map[string]any{
					"outcome": string(part.CodeExecutionResult.Outcome),
					"output":  part.CodeExecutionResult.Output,
				},
			}))
		}
		if part.ExecutableCode != nil {
			emitted = append(emitted, ai.NewCustomPart(map[string]any{
				"executableCode": map[string]any{
					"language": string(part.ExecutableCode.Language),
					"code":     part.ExecutableCode.Code,
				},
			}))
		}

		if len(emitted) == 0 {
			if len(part.ThoughtSignature) > 0 && len(result) > 0 && result[len(result)-1].ToolRequest != nil {
				previous := result[len(result)-1]
				if previous.Metadata == nil {
					previous.Metadata = make(map[string]any)
				}
				previous.Metadata["signature"] = append([]byte(nil), part.ThoughtSignature...)
				continue
			}
			if len(part.ThoughtSignature) > 0 {
				emitted = append(emitted, ai.NewReasoningPart("", part.ThoughtSignature))
			} else {
				continue
			}
		}
		if len(part.ThoughtSignature) > 0 {
			signatureTarget := emitted[0]
			if emittedFunctionCall != nil {
				signatureTarget = emittedFunctionCall
			}
			if signatureTarget.Metadata == nil {
				signatureTarget.Metadata = make(map[string]any)
			}
			signatureTarget.Metadata["signature"] = append([]byte(nil), part.ThoughtSignature...)
		}
		result = append(result, emitted...)
	}
	return result
}

func aiPartsFromVertexAIFunctionResponseParts(parts []*googlegenai.FunctionResponsePart) []*ai.Part {
	result := make([]*ai.Part, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			continue
		}
		if part.InlineData != nil {
			result = append(result, ai.NewMediaPart(
				part.InlineData.MIMEType,
				common_util.EncodeDataURI(part.InlineData.MIMEType, part.InlineData.Data),
			))
		}
		if part.FileData != nil {
			result = append(result, ai.NewMediaPart(part.FileData.MIMEType, part.FileData.FileURI))
		}
	}
	return result
}

// responseMIMEType performs its package-specific work in the GenAI model resolution and runtime construction flow.
func responseMIMEType(output *ai.ModelOutputConfig) string {
	if output == nil {
		return ""
	}

	contentType := strings.TrimSpace(output.ContentType)
	format := strings.TrimSpace(output.Format)
	if format == ai.OutputFormatEnum || contentType == "text/enum" {
		return "text/x.enum"
	}
	switch format {
	case ai.OutputFormatJSON:
		if contentType == "" {
			return "application/json"
		}
	case ai.OutputFormatText:
		if contentType == "" {
			return "text/plain"
		}
	}
	return contentType
}

// aiFinishReasonFromVertex performs its package-specific work in the GenAI model resolution and runtime construction flow.
func aiFinishReasonFromVertex(reason googlegenai.FinishReason) ai.FinishReason {
	switch reason {
	case "", googlegenai.FinishReasonUnspecified:
		return ""
	case googlegenai.FinishReasonStop:
		return ai.FinishReasonStop
	case googlegenai.FinishReasonMaxTokens:
		return ai.FinishReasonLength
	case googlegenai.FinishReasonSafety,
		googlegenai.FinishReasonRecitation,
		googlegenai.FinishReasonLanguage,
		googlegenai.FinishReasonBlocklist,
		googlegenai.FinishReasonProhibitedContent,
		googlegenai.FinishReasonSPII,
		googlegenai.FinishReasonImageSafety,
		googlegenai.FinishReasonImageProhibitedContent,
		googlegenai.FinishReasonImageRecitation,
		googlegenai.FinishReason("MODEL_ARMOR"),
		googlegenai.FinishReason("ESCALATION"):
		return ai.FinishReasonBlocked
	case googlegenai.FinishReasonMalformedFunctionCall,
		googlegenai.FinishReasonUnexpectedToolCall,
		googlegenai.FinishReasonNoImage,
		googlegenai.FinishReasonImageOther,
		googlegenai.FinishReasonOther,
		googlegenai.FinishReason("MISSING_THOUGHT_SIGNATURE"),
		googlegenai.FinishReason("MALFORMED_RESPONSE"),
		googlegenai.FinishReason("TOO_MANY_TOOL_CALLS"):
		return ai.FinishReasonOther
	default:
		return ai.FinishReasonUnknown
	}
}

// mapFromAny performs its package-specific work in the GenAI model resolution and runtime construction flow.
func mapFromAny(value any) map[string]any {
	if value == nil {
		return nil
	}
	if mapped, ok := value.(map[string]any); ok {
		return mapped
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}

	var mapped map[string]any
	if err := json.Unmarshal(data, &mapped); err != nil {
		return nil
	}
	return mapped
}

// functionResponseMap performs its package-specific work in the GenAI model resolution and runtime construction flow.
func functionResponseMap(value any) map[string]any {
	if value == nil {
		return map[string]any{"output": nil}
	}
	if mapped, ok := value.(map[string]any); ok {
		if len(mapped) == 0 {
			return map[string]any{"output": mapped}
		}
		return mapped
	}
	return map[string]any{"output": value}
}

// toolResponseOutput performs its package-specific work in the GenAI model resolution and runtime construction flow.
func toolResponseOutput(value map[string]any) any {
	if len(value) == 1 {
		if output, ok := value["output"]; ok {
			return output
		}
	}
	return value
}
