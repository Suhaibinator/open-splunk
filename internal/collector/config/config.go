package config

import (
	"errors"

	yaml "go.yaml.in/yaml/v3"
)

// errNotImplemented is returned by contract stubs during the skeleton phase.
var errNotImplemented = errors.New("collector/config: not implemented")

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
	// the system trust store.
	CAFile string `yaml:"ca_file"`
	// ServerName overrides the SNI / certificate name verified during the
	// handshake. Empty means the host portion of ServerConfig.Address.
	ServerName string `yaml:"server_name"`
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
// accepted; a bare number is bytes.
type ByteSize uint64

// UnmarshalYAML parses a byte-size string or integer into a ByteSize.
func (b *ByteSize) UnmarshalYAML(_ *yaml.Node) error {
	return errNotImplemented
}

// Duration wraps time.Duration with YAML string parsing (e.g. "5s", "250ms").
type Duration int64

// UnmarshalYAML parses a Go duration string into a Duration.
func (d *Duration) UnmarshalYAML(_ *yaml.Node) error {
	return errNotImplemented
}

// Load reads the YAML file at path, applies ${ENV} substitution over the raw
// bytes, unmarshals it, fills defaults, and calls Validate. The returned Config
// never contains the ingestion token.
func Load(path string) (*Config, error) {
	return nil, errNotImplemented
}

// Validate reports the first precise configuration error, or nil. It checks
// required fields, transport/format enumerations, glob presence, TLS coherence,
// input ID uniqueness, and processor field requirements.
func (c *Config) Validate() error {
	return errNotImplemented
}

// LoadToken reads and returns the bearer token from path, trimming trailing
// newlines. It is called by the sender at dial time; the returned secret is
// never stored in a Config value.
func LoadToken(path string) (string, error) {
	return "", errNotImplemented
}

// String returns a redacted, diagnostic-safe rendering of the configuration.
// It must never read the token file or emit secret material.
func (c *Config) String() string {
	return ""
}
