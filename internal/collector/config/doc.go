// Package config defines the Open Splunk Collector configuration model and its
// loading, environment substitution, secret handling, and validation contracts.
//
// The YAML shape mirrors configs/examples/collector.yaml: a server block (dial
// address, transport, token_file, tls, compression), a state block (durable
// queue directory and max_queue_bytes), a list of inputs, and an ordered list
// of processors. [Load] reads a file, performs ${ENV} substitution, applies
// defaults, and then calls [Config.Validate]; callers that already hold raw
// bytes may unmarshal directly and call Validate themselves.
//
// # Secrets
//
// Config never holds the ingestion token. [ServerConfig.TokenFile] is a path
// only; the token bytes are read at dial time (see [LoadToken]) by the sender
// and are never retained in a Config value. [Config.String] and any marshal of
// Config are therefore safe to write to logs and diagnostics: they contain a
// path, not a secret. If a future field ever carries secret material it must be
// redacted by String and excluded from diagnostic dumps; that invariant is part
// of this package's contract.
//
// # Sizes and durations
//
// [ByteSize] parses human byte strings such as "10GiB" and "512MB"; [Duration]
// parses Go duration strings such as "5s". Both are typed so validation errors
// can point at the offending field.
//
// # Dependency direction
//
// config depends only on the standard library and go.yaml.in/yaml/v3. It must
// not import any other internal/collector package. The root collector daemon
// translates a *Config into the option structs of framing, input, wal, and
// sender; those packages never import config, keeping the graph acyclic.
package config
