package collector

import (
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/protobuf/proto"
)

func TestCommitTerminalCheckpointsAdvancesFromDurableBatchOrigin(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device:            7,
		Inode:             11,
		Generation:        2,
		Fingerprint:       strings.Repeat("ab", 32),
		FingerprintLength: 913,
	}
	if err := store.Set(input.Checkpoint{Identity: identity, Path: "/logs/app.log", Offset: 0}); err != nil {
		t.Fatalf("seed discovery checkpoint: %v", err)
	}
	marks := []wal.SourceCheckpointMark{
		checkpointSourceMark(1, identity, "/logs/app.log", 100, 1),
		checkpointSourceMark(2, identity, "/logs/app.log", 240, 2),
	}

	if err := commitTerminalCheckpoints(store, marks); err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	got, ok, err := store.Get(identity)
	if err != nil || !ok {
		t.Fatalf("Get = (%+v, %t, %v)", got, ok, err)
	}
	if got.Offset != 240 || got.LineNumber != 2 || got.Path != "/logs/app.log" {
		t.Fatalf("checkpoint = %+v, want offset 240 line 2 with source path", got)
	}
	if got.Identity != identity {
		t.Fatalf("identity = %+v, want full identity %+v", got.Identity, identity)
	}
}

func TestCommitTerminalCheckpointsFencesDelayedPreCopytruncateGeneration(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldIdentity := input.FileIdentity{
		Device: 5, Inode: 9, Generation: 1,
		Fingerprint: strings.Repeat("11", 32), FingerprintLength: 1024,
	}
	newIdentity := input.FileIdentity{
		Device: 5, Inode: 9, Generation: 2,
		Fingerprint: strings.Repeat("22", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{Identity: newIdentity, Path: "/logs/app.log", Offset: 20, LineNumber: 1}); err != nil {
		t.Fatalf("seed new generation checkpoint: %v", err)
	}

	if err := commitTerminalCheckpoints(store, []wal.SourceCheckpointMark{
		checkpointSourceMark(1, oldIdentity, "/logs/app.log", 900, 90),
	}); err != nil {
		t.Fatalf("commit old generation: %v", err)
	}
	got, ok, err := store.Get(newIdentity)
	if err != nil || !ok {
		t.Fatalf("Get = (%+v, %t, %v)", got, ok, err)
	}
	if got.Identity != newIdentity || got.Offset != 20 {
		t.Fatalf("delayed old generation crossed copytruncate fence: %+v", got)
	}
}

func checkpointSourceMark(sequence uint64, identity input.FileIdentity, path string, end, line uint64) wal.SourceCheckpointMark {
	return wal.SourceCheckpointMark{
		BatchSequence: sequence, FileIdentity: identity.String(),
		SourcePath: path, HasSourcePath: true,
		EndOffset: end, HasEndOffset: true, LineNumber: line,
		FingerprintLength: identity.FingerprintLength, HasFingerprintLength: true,
	}
}

func checkpointBatch(sequence uint64, identity input.FileIdentity, path string, start, end, line uint64) *opensplunkv1.EventBatch {
	return &opensplunkv1.EventBatch{
		BatchSequence: sequence,
		Events: []*opensplunkv1.LogEvent{{
			EventId: "event",
			Origin: &opensplunkv1.EventOrigin{
				InputId:               "input",
				FileIdentity:          proto.String(identity.String()),
				StartOffset:           proto.Uint64(start),
				EndOffset:             proto.Uint64(end),
				LineNumber:            proto.Uint64(line),
				SourcePath:            proto.String(path),
				FileFingerprintLength: proto.Uint32(identity.FingerprintLength),
			},
		}},
	}
}
