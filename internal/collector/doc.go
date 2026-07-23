// Package collector is the root of the Open Splunk Collector: it decodes framed
// source bytes into canonical LogEvents (see decoder.go), applies the processor
// chain (allow/deny/rename/redact), and orchestrates inputs, the durable queue,
// and gRPC delivery through the [Daemon].
//
// # Sub-packages and dependency direction
//
// The collector is split so that six packages can be implemented independently.
// The import graph is acyclic and only this root package imports across it:
//
//	collector/framing   <- collector/input
//	collector/wal       <- collector/sender
//	collector/config, framing, input, wal, sender  <- collector (root)
//
// Nothing imports the root collector package. framing depends only on stdlib;
// input depends on framing; wal depends only on stdlib and the generated
// protobuf; sender depends on wal; config depends only on stdlib and YAML. The
// root Daemon (daemon.go) is the single wiring point that depends on all of
// them and translates a *config.Config into each package's option structs.
//
// # Delivery contract
//
// The queue (collector/wal) is the durability boundary: events are appended and
// fsynced before transmission, batch identity is minted once at append and is
// stable across retries, and file checkpoints (collector/input) advance only
// after the server returns a terminal disposition with its negotiated
// ClickHouse-committed durability and the entire earlier WAL prefix is
// terminal. Delivery is at-least-once; the server deduplicates by stable event
// ID.
package collector
