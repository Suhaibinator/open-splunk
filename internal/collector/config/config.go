package config

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Config is the fully parsed collector configuration. It is the single value
// the daemon is constructed from. A Config never contains secret material; see
// the package documentation for the token-handling contract.
type Config struct {
	Server     ServerConfig      `yaml:"server"`
	State      StateConfig       `yaml:"state"`
	Inputs     []InputConfig     `yaml:"inputs"`
	Processors []ProcessorConfig `yaml:"processors"`
}

// ServerConfig describes how to reach and authenticate to the ingestion server.
type ServerConfig struct {
	// Address is the "host:port" gRPC dial target.
	Address string `yaml:"address"`
	// Transport currently must be "grpc".
	Transport string `yaml:"transport"`
	// TokenFile is the path to the bearer token file. The token is read at dial
	// time and is never stored in a Config value or printed by diagnostics.
	TokenFile string `yaml:"token_file"`
	// TLS configures transport security. When disabled, plaintext gRPC is used
	// and is only permitted for local development.
	TLS TLSConfig `yaml:"tls"`
	// Compression is the gRPC compressor name, e.g. "gzip". Empty means none.
	Compression string `yaml:"compression"`
}

// TLSConfig configures collector-to-server transport security.
type TLSConfig struct {
	Enabled bool `yaml:"enabled"`
	// CAFile is a PEM bundle used to verify the server certificate. Empty means
	// the system trust store. Its readability is NOT checked by Validate; only
	// path-level coherence is enforced.
	CAFile string `yaml:"ca_file"`
	// ServerName overrides the SNI / certificate name verified during the
	// handshake. Empty means the host portion of ServerConfig.Address.
	ServerName string `yaml:"server_name"`
	// AllowInsecureRemote permits plaintext (tls.enabled=false) to a non-loopback
	// server.address. Without it, plaintext to a remote host is rejected because
	// the bearer token would be transmitted in cleartext. Setting it is an
	// explicit, audited acknowledgement of that risk (e.g. a trusted private
	// network or a sidecar TLS terminator).
	AllowInsecureRemote bool `yaml:"allow_insecure_remote"`
}

// StateConfig describes the collector's durable local state.
type StateConfig struct {
	// Directory holds the WAL segments, checkpoint store, and dead-letter file.
	Directory string `yaml:"directory"`
	// MaxQueueBytes bounds the on-disk durable queue. When the queue is full the
	// collector stops reading inputs (backpressure) rather than dropping data.
	MaxQueueBytes ByteSize `yaml:"max_queue_bytes"`
}

// InputConfig is one file-monitoring input. The trusted metadata fields (Index,
// Source, Sourcetype, Host, Fields) are attached to every event and can never
// be overridden by payload content.
type InputConfig struct {
	ID         string            `yaml:"id"`
	Type       string            `yaml:"type"`
	Include    []string          `yaml:"include"`
	Exclude    []string          `yaml:"exclude"`
	Format     string            `yaml:"format"`
	StartAt    string            `yaml:"start_at"`
	Index      string            `yaml:"index"`
	Source     string            `yaml:"source"`
	Sourcetype string            `yaml:"sourcetype"`
	Host       string            `yaml:"host"`
	Fields     map[string]string `yaml:"fields"`
	// Multiline, when set, enables multi-line event assembly for this input.
	Multiline *MultilineConfig `yaml:"multiline"`
	// MaxEventBytes caps a single framed event; zero means the package default.
	MaxEventBytes ByteSize `yaml:"max_event_bytes"`
	// PollInterval controls the file tailing poll cadence; zero means default.
	PollInterval Duration `yaml:"poll_interval"`
}

// MultilineConfig configures multi-line framing for an input.
type MultilineConfig struct {
	// LineStartPattern is a regular expression; a physical line that matches it
	// begins a new logical event. Non-matching lines continue the current one.
	LineStartPattern string `yaml:"line_start_pattern"`
	// MaxLines bounds the number of physical lines assembled into one event.
	MaxLines int `yaml:"max_lines"`
	// FlushAfter emits an incomplete event after this much reader inactivity.
	FlushAfter Duration `yaml:"flush_after"`
}

// ProcessorConfig is one entry in the ordered processor chain. Which fields are
// meaningful depends on Type ("allow", "deny", "rename", "redact").
type ProcessorConfig struct {
	Type string `yaml:"type"`
	// Fields lists the dynamic field names an allow/deny/redact processor acts on.
	Fields []string `yaml:"fields"`
	// Replacement is the redaction replacement string (redact only).
	Replacement string `yaml:"replacement"`
	// From and To name the source and destination fields for a rename processor.
	From string `yaml:"from"`
	To   string `yaml:"to"`
}

// ByteSize is a byte count parsed from human strings such as "10GiB", "512MB",
// or a bare integer. Binary (GiB/MiB/KiB) and decimal (GB/MB/KB) units are both
// accepted; a bare number is bytes. Values are stored as an exact uint64.
type ByteSize uint64

// UnmarshalYAML parses a byte-size string or integer into a ByteSize. It accepts
// binary units (KiB/MiB/GiB/TiB/PiB, powers of 1024), decimal units
// (KB/MB/GB/TB/PB, powers of 1000), an explicit "B", and a bare integer number
// of bytes. Negative values and values that overflow uint64 are rejected.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("max byte size must be a scalar, got a %s", nodeKindName(node.Kind))
	}
	v, err := parseByteSize(node.Value)
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}

// String renders the byte count using a binary unit when it divides evenly,
// otherwise as a plain byte count.
func (b ByteSize) String() string {
	n := uint64(b)
	switch {
	case n == 0:
		return "0B"
	case n%(1<<30) == 0:
		return fmt.Sprintf("%dGiB", n/(1<<30))
	case n%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", n/(1<<20))
	case n%(1<<10) == 0:
		return fmt.Sprintf("%dKiB", n/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// Duration wraps time.Duration with YAML string parsing (e.g. "5s", "250ms").
type Duration int64

// UnmarshalYAML parses a Go duration string into a Duration. A bare integer
// (no unit) is rejected, because a unitless number is ambiguous.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar, got a %s", nodeKindName(node.Kind))
	}
	s := strings.TrimSpace(node.Value)
	if s == "" || node.Tag == "!!null" {
		*d = 0
		return nil
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(dur)
	return nil
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// String renders the duration using Go duration syntax.
func (d Duration) String() string { return time.Duration(d).String() }

// Load reads the YAML file at path, applies ${ENV} substitution over the raw
// bytes, unmarshals it with unknown fields rejected, fills defaults, and calls
// Validate. Undefined environment variables referenced as ${VAR} are an error;
// a literal dollar sign is written as $$. The returned Config never contains the
// ingestion token.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	sub, err := substituteEnv(raw)
	if err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(sub))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}
	return &cfg, nil
}

// applyDefaults fills in the small set of defaults this package owns. Sizes and
// durations left at zero are interpreted as "use the package default" by the
// consuming packages and are not defaulted here.
func (c *Config) applyDefaults() {
	if c.Server.Transport == "" {
		c.Server.Transport = "grpc"
	}
	for i := range c.Inputs {
		in := &c.Inputs[i]
		if in.Type == "" {
			in.Type = "file"
		}
		if in.Format == "" {
			in.Format = "ndjson"
		}
		if in.StartAt == "" {
			in.StartAt = "end"
		}
	}
}

// Validate reports the first precise configuration error, or nil. Each error
// names the offending field or input id. It checks required fields, the
// transport/format/start_at enumerations, include-glob presence and syntax,
// input-id uniqueness, TLS coherence (paths only; ca_file readability is not
// checked here), processor type/field requirements, and multiline pattern
// compilation.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Server.Address) == "" {
		return errors.New("server.address is required")
	}
	if c.Server.Transport != "grpc" {
		return fmt.Errorf("server.transport must be %q, got %q", "grpc", c.Server.Transport)
	}
	if strings.TrimSpace(c.Server.TokenFile) == "" {
		return errors.New("server.token_file is required")
	}
	if !c.Server.TLS.Enabled && (c.Server.TLS.CAFile != "" || c.Server.TLS.ServerName != "") {
		return errors.New("server.tls: ca_file/server_name are set but tls.enabled is false")
	}
	if !c.Server.TLS.Enabled && !c.Server.TLS.AllowInsecureRemote && !isLoopbackAddress(c.Server.Address) {
		return errors.New("server.tls.enabled is false and server.address is not loopback: the bearer token would be sent in cleartext; enable TLS or set server.tls.allow_insecure_remote to override")
	}
	if strings.TrimSpace(c.State.Directory) == "" {
		return errors.New("state.directory is required")
	}
	if len(c.Inputs) == 0 {
		return errors.New("at least one input is required")
	}
	seen := make(map[string]int, len(c.Inputs))
	for i, in := range c.Inputs {
		id := in.ID
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("inputs[%d].id is required", i)
		}
		if prev, ok := seen[id]; ok {
			return fmt.Errorf("duplicate input id %q (inputs[%d] and inputs[%d])", id, prev, i)
		}
		seen[id] = i
		if in.Type != "file" {
			return fmt.Errorf("input %q: type must be %q, got %q", id, "file", in.Type)
		}
		switch in.Format {
		case "ndjson", "raw":
		default:
			return fmt.Errorf("input %q: format must be \"ndjson\" or \"raw\", got %q", id, in.Format)
		}
		switch in.StartAt {
		case "beginning", "end":
		default:
			return fmt.Errorf("input %q: start_at must be \"beginning\" or \"end\", got %q", id, in.StartAt)
		}
		if len(in.Include) == 0 {
			return fmt.Errorf("input %q: at least one include glob is required", id)
		}
		if strings.TrimSpace(in.Index) == "" {
			return fmt.Errorf("input %q: index is required", id)
		}
		for _, g := range in.Include {
			if _, err := filepath.Match(g, ""); err != nil {
				return fmt.Errorf("input %q: invalid include glob %q: %v", id, g, err)
			}
		}
		for _, g := range in.Exclude {
			if _, err := filepath.Match(g, ""); err != nil {
				return fmt.Errorf("input %q: invalid exclude glob %q: %v", id, g, err)
			}
		}
		if in.Multiline != nil {
			if strings.TrimSpace(in.Multiline.LineStartPattern) == "" {
				return fmt.Errorf("input %q: multiline.line_start_pattern is required", id)
			}
			if _, err := regexp.Compile(in.Multiline.LineStartPattern); err != nil {
				return fmt.Errorf("input %q: multiline.line_start_pattern: %v", id, err)
			}
			if in.Multiline.MaxLines < 0 {
				return fmt.Errorf("input %q: multiline.max_lines must be >= 0", id)
			}
		}
	}
	for i, p := range c.Processors {
		switch p.Type {
		case "allow", "deny", "redact":
			if len(p.Fields) == 0 {
				return fmt.Errorf("processors[%d] (%s): at least one field is required", i, p.Type)
			}
		case "rename":
			if strings.TrimSpace(p.From) == "" {
				return fmt.Errorf("processors[%d] (rename): from is required", i)
			}
			if strings.TrimSpace(p.To) == "" {
				return fmt.Errorf("processors[%d] (rename): to is required", i)
			}
		case "":
			return fmt.Errorf("processors[%d]: type is required", i)
		default:
			return fmt.Errorf("processors[%d]: unknown type %q", i, p.Type)
		}
	}
	return nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// maxTokenBytes bounds the size of a token file. A larger file almost certainly
// means the wrong path was configured.
const maxTokenBytes = 4 * 1024

// LoadToken reads and returns the bearer token from path, trimming a trailing
// newline and/or carriage return. It errors if the file is empty or larger than
// 4 KiB. Errors mention the path but never the token contents. The returned
// secret is never stored in a Config value.
func LoadToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file %q: %w", path, err)
	}
	if len(data) > maxTokenBytes {
		return "", fmt.Errorf("token file %q exceeds %d bytes", path, maxTokenBytes)
	}
	tok := strings.TrimRight(string(data), "\r\n")
	if tok == "" {
		return "", fmt.Errorf("token file %q is empty", path)
	}
	return tok, nil
}

// String returns a redacted, diagnostic-safe rendering of the configuration. It
// never reads the token file and includes only the token_file path, so its
// output is safe to write to logs and diagnostics.
func (c *Config) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "server:\n")
	fmt.Fprintf(&b, "  address: %s\n", c.Server.Address)
	fmt.Fprintf(&b, "  transport: %s\n", c.Server.Transport)
	fmt.Fprintf(&b, "  token_file: %s\n", c.Server.TokenFile)
	if c.Server.Compression != "" {
		fmt.Fprintf(&b, "  compression: %s\n", c.Server.Compression)
	}
	fmt.Fprintf(&b, "  tls: {enabled: %t, ca_file: %s, server_name: %s}\n",
		c.Server.TLS.Enabled, c.Server.TLS.CAFile, c.Server.TLS.ServerName)
	fmt.Fprintf(&b, "state:\n")
	fmt.Fprintf(&b, "  directory: %s\n", c.State.Directory)
	fmt.Fprintf(&b, "  max_queue_bytes: %s\n", c.State.MaxQueueBytes)
	fmt.Fprintf(&b, "inputs: (%d)\n", len(c.Inputs))
	for _, in := range c.Inputs {
		fmt.Fprintf(&b, "  - id=%s type=%s format=%s start_at=%s include=%v exclude=%v\n",
			in.ID, in.Type, in.Format, in.StartAt, in.Include, in.Exclude)
		fmt.Fprintf(&b, "    index=%s source=%s sourcetype=%s host=%s fields=%v\n",
			in.Index, in.Source, in.Sourcetype, in.Host, in.Fields)
		if in.MaxEventBytes != 0 {
			fmt.Fprintf(&b, "    max_event_bytes=%s\n", in.MaxEventBytes)
		}
		if in.PollInterval != 0 {
			fmt.Fprintf(&b, "    poll_interval=%s\n", in.PollInterval)
		}
		if in.Multiline != nil {
			fmt.Fprintf(&b, "    multiline: {line_start_pattern=%q max_lines=%d flush_after=%s}\n",
				in.Multiline.LineStartPattern, in.Multiline.MaxLines, in.Multiline.FlushAfter)
		}
	}
	fmt.Fprintf(&b, "processors: (%d)\n", len(c.Processors))
	for i, p := range c.Processors {
		fmt.Fprintf(&b, "  - [%d] type=%s fields=%v from=%s to=%s replacement=%q\n",
			i, p.Type, p.Fields, p.From, p.To, p.Replacement)
	}
	return b.String()
}

// substituteEnv replaces ${VAR} references in raw with the corresponding
// environment variable value, before YAML parsing. A literal dollar sign is
// written as $$. A reference to an undefined variable is an error. Any other
// use of $ is passed through unchanged.
func substituteEnv(raw []byte) ([]byte, error) {
	s := string(raw)
	var out bytes.Buffer
	out.Grow(len(s))
	for i := 0; i < len(s); {
		c := s[i]
		if c != '$' {
			out.WriteByte(c)
			i++
			continue
		}
		if i+1 < len(s) && s[i+1] == '$' {
			out.WriteByte('$')
			i += 2
			continue
		}
		if i+1 < len(s) && s[i+1] == '{' {
			rel := strings.IndexByte(s[i+2:], '}')
			if rel < 0 {
				return nil, fmt.Errorf("unterminated ${...} reference at byte %d", i)
			}
			name := s[i+2 : i+2+rel]
			if name == "" {
				return nil, fmt.Errorf("empty ${} variable reference at byte %d", i)
			}
			val, ok := os.LookupEnv(name)
			if !ok {
				return nil, fmt.Errorf("undefined environment variable %q referenced in config", name)
			}
			out.WriteString(val)
			i = i + 2 + rel + 1
			continue
		}
		// Bare '$' not part of a recognized construct: pass through literally.
		out.WriteByte('$')
		i++
	}
	return out.Bytes(), nil
}

// parseByteSize parses a human byte-size string into an exact uint64 count.
func parseByteSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("byte size cannot be negative: %q", s)
	}
	s = strings.TrimPrefix(s, "+")

	i := 0
	for i < len(s) && (isDigit(s[i]) || s[i] == '.') {
		i++
	}
	numStr := s[:i]
	unit := strings.TrimSpace(s[i:])
	if numStr == "" {
		return 0, fmt.Errorf("missing numeric value in byte size %q", s)
	}
	mult, ok := byteSizeUnit(unit)
	if !ok {
		return 0, fmt.Errorf("unknown byte size unit %q in %q", unit, s)
	}
	if mult == 1 {
		if strings.Contains(numStr, ".") {
			return 0, fmt.Errorf("fractional byte count %q requires a unit", s)
		}
		v, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
		}
		return v, nil
	}
	f, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	product := f * float64(mult)
	if product < 0 || product >= float64(math.MaxUint64) {
		return 0, fmt.Errorf("byte size %q overflows uint64", s)
	}
	return uint64(product), nil
}

func byteSizeUnit(unit string) (uint64, bool) {
	switch strings.ToLower(unit) {
	case "", "b":
		return 1, true
	case "kb":
		return 1000, true
	case "mb":
		return 1000 * 1000, true
	case "gb":
		return 1000 * 1000 * 1000, true
	case "tb":
		return 1000 * 1000 * 1000 * 1000, true
	case "pb":
		return 1000 * 1000 * 1000 * 1000 * 1000, true
	case "kib":
		return 1 << 10, true
	case "mib":
		return 1 << 20, true
	case "gib":
		return 1 << 30, true
	case "tib":
		return 1 << 40, true
	case "pib":
		return 1 << 50, true
	default:
		return 0, false
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func nodeKindName(k yaml.Kind) string {
	switch k {
	case yaml.DocumentNode:
		return "document"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.MappingNode:
		return "mapping"
	case yaml.ScalarNode:
		return "scalar"
	case yaml.AliasNode:
		return "alias"
	default:
		return "unknown"
	}
}
