package collector

import (
	"context"

	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

// Daemon orchestrates the collector: it builds a decoder, framer, and processor
// pipeline per input, tails files, decodes and processes events, appends them to
// the durable queue, and runs the sender that delivers batches to the server.
//
// It is the only type that depends on config, framing, input, wal, and sender;
// those packages do not depend on one another except input -> framing and
// sender -> wal, and none depends on this package, so the graph is acyclic.
type Daemon struct {
	cfg         *config.Config
	inputs      []input.Manager
	checkpoints input.CheckpointStore
	queue       wal.Queue
	sender      *sender.Sender
}

// Option customizes Daemon construction (clock, logger, framer overrides, etc.).
type Option func(*daemonOptions)

// daemonOptions holds resolved construction options.
type daemonOptions struct {
	// framing carries any framer defaults the daemon applies per input; it keeps
	// the framing dependency explicit at the orchestration layer.
	framing framing.Options
}

// New constructs a Daemon from cfg. It opens the checkpoint store and durable
// queue under cfg.State.Directory, builds one input Manager per configured
// input, and constructs the sender from cfg.Server.
func New(cfg *config.Config, opts ...Option) (*Daemon, error) {
	return nil, errNotImplemented
}

// Run starts every input, the decode/process/append pipeline, and the sender,
// blocking until ctx is cancelled and shutdown completes.
func (d *Daemon) Run(ctx context.Context) error {
	return errNotImplemented
}
