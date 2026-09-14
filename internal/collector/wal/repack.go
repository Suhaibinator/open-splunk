package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"fortio.org/safecast"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

// RepackingQueue may replace a batch only AFTER the server has durably rejected
// its exact original identity with REPACK_REQUIRED. A timeout, Ready hint, or
// local size check is never that proof. Original WAL bytes remain the backing
// storage and checkpoint barrier until all children have terminal outcomes.
type RepackingQueue interface {
	Repack(sequence uint64, maximumEvents uint32, maximumBytes uint64) error
}

const maximumRepackManifestBytes = 1 << 20

type repackChild struct {
	Sequence uint64 `json:"sequence"`
	BatchID  string `json:"batch_id"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
}

type repackPlan struct {
	Version       int           `json:"version"`
	Parent        repackChild   `json:"parent"`
	ParentSHA256  string        `json:"parent_sha256"`
	Segment       string        `json:"segment"`
	PayloadOffset int64         `json:"payload_offset"`
	PayloadLength uint32        `json:"payload_length"`
	CRC           uint32        `json:"crc"`
	Children      []repackChild `json:"children"`
}

type repackEnvelope struct {
	Plan   json.RawMessage `json:"plan"`
	SHA256 string          `json:"sha256"`
}

func repackFileName(sequence uint64) string { return fmt.Sprintf("repack-%020d.json", sequence) }

func repackDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func repackBatch(root *opensplunk.EventBatch, child repackChild) *opensplunk.EventBatch {
	// Detach the pointer array so a small in-flight child does not retain every
	// unrelated event decoded from the backing record.
	events := append([]*opensplunk.LogEvent(nil), root.GetEvents()[child.Start:child.End]...)
	batch := &opensplunk.EventBatch{CollectorId: root.GetCollectorId(), BatchId: child.BatchID,
		BatchSequence: child.Sequence, CreatedAt: root.GetCreatedAt(), Events: events,
		UncompressedSizeBytes: uncompressedEventBytes(events), EventIdsSha256: ComputeEventIDsDigest(events)}
	if unknown := root.ProtoReflect().GetUnknown(); len(unknown) > 0 {
		batch.ProtoReflect().SetUnknown(append([]byte(nil), unknown...))
	}
	return batch
}

func (q *queue) Repack(sequence uint64, maximumEvents uint32, maximumBytes uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if q.syncErr != nil {
		return q.syncErr
	}
	if maximumEvents == 0 || maximumBytes == 0 {
		return errors.New("collector/wal: invalid repacking limits")
	}
	if _, exists := q.repacks[sequence]; exists {
		return nil
	}
	index := slices.IndexFunc(q.unacked, func(d batchDesc) bool { return d.seq == sequence })
	if index < 0 {
		return fmt.Errorf("collector/wal: repacking parent %d is not queued", sequence)
	}
	d := q.unacked[index]
	parent, err := q.readBatch(d)
	if err != nil {
		return err
	}
	if len(parent.GetEvents()) < 2 {
		return errors.New("collector/wal: a singleton cannot be repacked")
	}
	view := repackChild{Sequence: sequence, BatchID: parent.GetBatchId(), End: len(parent.GetEvents())}
	if d.repacked != nil {
		view = *d.repacked
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(parent)
	if err != nil {
		return err
	}
	plan := repackPlan{Version: 1, Parent: view, ParentSHA256: repackDigest(encoded),
		Segment: d.segName, PayloadOffset: d.payloadOff, PayloadLength: d.payloadLen, CRC: d.crc}
	start := 0
	var size uint64
	for i, event := range parent.GetEvents() {
		bytes := safecast.MustConv[uint64](proto.Size(event))
		if i > start && (safecast.MustConv[uint64](i-start) >= uint64(maximumEvents) || size > maximumBytes || bytes > maximumBytes-size) {
			plan.Children = append(plan.Children, repackChild{BatchID: uuid.NewString(), Start: view.Start + start, End: view.Start + i})
			start, size = i, 0
		}
		size += bytes
	}
	plan.Children = append(plan.Children, repackChild{BatchID: uuid.NewString(), Start: view.Start + start, End: view.End})
	if len(plan.Children) < 2 {
		// A durable repack rejection can outlive the policy that produced it.
		// Even after limits are relaxed, the fenced identity must be replaced.
		middle := view.Start + len(parent.GetEvents())/2
		plan.Children = []repackChild{{BatchID: uuid.NewString(), Start: view.Start, End: middle}, {BatchID: uuid.NewString(), Start: middle, End: view.End}}
	}
	if uint64(len(plan.Children)) >= math.MaxUint64-q.nextSeq {
		return errors.New("collector/wal: repacking sequence exhausted")
	}
	// Virtual children consume sequence numbers but not physical WAL records.
	// Seal first so subsequent physical segments still have contiguous runs.
	if err := q.sealActiveLocked(); err != nil {
		return err
	}
	for i := range plan.Children {
		plan.Children[i].Sequence = q.nextSeq
		q.nextSeq++
	}
	if err := q.persistMetaLocked(); err != nil {
		q.syncErr = err
		return err
	}
	// The manifest is the atomic publication point. No new identities can be
	// delivered before it is durable; a crash afterwards reconstructs exactly it.
	if err := q.persistRepackPlan(plan); err != nil {
		q.syncErr = err
		return err
	}
	if q.repacks == nil {
		q.repacks = make(map[uint64]repackPlan)
	}
	q.repacks[sequence] = plan
	if err := q.persistMetaLocked(); err != nil {
		q.syncErr = err
		return err
	}
	if err := q.installRepackPlan(plan); err != nil {
		q.syncErr = err
		return err
	}
	q.signalLocked()
	return nil
}

func (q *queue) persistRepackPlan(plan repackPlan) error {
	data, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(repackEnvelope{Plan: data, SHA256: repackDigest(data)})
	if err != nil {
		return err
	}
	if len(encoded) > maximumRepackManifestBytes {
		return errors.New("collector/wal: repack manifest exceeds recovery bound")
	}
	file, err := os.CreateTemp(q.dir, "repack.tmp-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	n, err := file.Write(encoded)
	if err == nil && n != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(q.dir, repackFileName(plan.Parent.Sequence))); err != nil {
		return err
	}
	return q.syncDirectory(q.dir)
}

func readRepackPlan(path string) (repackPlan, error) {
	var plan repackPlan
	info, err := os.Lstat(path)
	if err != nil {
		return plan, err
	}
	if !info.Mode().IsRegular() || info.Size() > maximumRepackManifestBytes {
		return plan, errors.New("collector/wal: invalid repack manifest file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return plan, err
	}
	var envelope repackEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return plan, err
	}
	if repackDigest(envelope.Plan) != envelope.SHA256 {
		return plan, errors.New("collector/wal: repack manifest checksum mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Plan))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return plan, err
	}
	if plan.Version != 1 || len(plan.Children) < 2 || plan.Parent.Sequence == 0 ||
		filepath.Base(path) != repackFileName(plan.Parent.Sequence) {
		return plan, errors.New("collector/wal: invalid repack manifest")
	}
	return plan, nil
}

func (q *queue) recoverRepacks() error {
	initialNext := q.nextSeq
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "repack-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		plan, err := readRepackPlan(filepath.Join(q.dir, entry.Name()))
		if err != nil {
			return err
		}
		if plan.Children[len(plan.Children)-1].Sequence <= q.lastAcked {
			if err := os.Remove(filepath.Join(q.dir, entry.Name())); err != nil {
				return err
			}
			continue
		}
		if err := q.installRepackPlan(plan); err != nil {
			return err
		}
	}
	slices.SortFunc(q.unacked, func(a, b batchDesc) int {
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
	if q.nextSeq != initialNext || len(q.repacks) > 0 {
		return q.persistMetaLocked()
	}
	return nil
}

func (q *queue) repackReferences() []repackReference {
	var references []repackReference
	for parent, plan := range q.repacks {
		through := plan.Children[len(plan.Children)-1].Sequence
		if through > q.lastAcked {
			references = append(references, repackReference{Parent: parent, Through: through})
		}
	}
	slices.SortFunc(references, func(a, b repackReference) int {
		if a.Parent < b.Parent {
			return -1
		}
		if a.Parent > b.Parent {
			return 1
		}
		return 0
	})
	return references
}

func (q *queue) installRepackPlan(plan repackPlan) error {
	if _, valid := parseSegmentName(plan.Segment); !valid || plan.PayloadOffset < recordHeaderSize || uint64(plan.PayloadLength) > maximumRecordPayloadBytes {
		return errors.New("collector/wal: invalid repack backing record")
	}
	root, err := readRecordPayload(filepath.Join(q.dir, plan.Segment), plan.PayloadOffset, plan.PayloadLength, plan.CRC)
	if err != nil {
		return err
	}
	if root.GetCollectorId() != q.opts.CollectorID || !protocolid.Valid(plan.Parent.BatchID) || plan.Parent.Start < 0 || plan.Parent.End > len(root.GetEvents()) || plan.Parent.Start >= plan.Parent.End {
		return errors.New("collector/wal: invalid repack parent")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(repackBatch(root, plan.Parent))
	if err != nil || repackDigest(encoded) != plan.ParentSHA256 {
		return errors.New("collector/wal: repack parent identity mismatch")
	}
	position, sequence := plan.Parent.Start, plan.Parent.Sequence
	ids := make(map[string]struct{}, len(plan.Children))
	for _, child := range plan.Children {
		if child.Start != position || child.End <= child.Start || child.End > plan.Parent.End ||
			child.Sequence <= sequence || child.Sequence == math.MaxUint64 || !protocolid.Valid(child.BatchID) || child.BatchID == plan.Parent.BatchID {
			return errors.New("collector/wal: repack children do not partition the parent")
		}
		if _, duplicate := ids[child.BatchID]; duplicate {
			return errors.New("collector/wal: duplicate repack child identity")
		}
		ids[child.BatchID] = struct{}{}
		if q.hasSequenceLocked(child.Sequence) {
			return errors.New("collector/wal: repack child sequence is already allocated")
		}
		position, sequence = child.End, child.Sequence
	}
	if position != plan.Parent.End {
		return errors.New("collector/wal: repack children omit events")
	}
	segment := slices.IndexFunc(q.segments, func(seg *segInfo) bool { return seg.name == plan.Segment })
	if segment < 0 {
		return errors.New("collector/wal: repack backing segment is missing")
	}
	q.segments[segment].lastSeq = max(q.segments[segment].lastSeq, sequence)
	if q.repacks == nil {
		q.repacks = make(map[uint64]repackPlan)
	}
	q.repacks[plan.Parent.Sequence] = plan
	for i := range q.unacked {
		if q.unacked[i].seq == plan.Parent.Sequence {
			q.queuedEvents -= q.unacked[i].eventCount
			q.unacked[i].eventCount = 0
			break
		}
	}
	for _, child := range plan.Children {
		if child.Sequence <= q.lastAcked {
			continue
		}
		batch := repackBatch(root, child)
		d := batchDesc{seq: child.Sequence, segName: plan.Segment, payloadOff: plan.PayloadOffset,
			payloadLen: plan.PayloadLength, crc: plan.CRC, eventCount: uint64(len(batch.GetEvents())),
			createdAt: batch.GetCreatedAt().AsTime(), sourceMarks: checkpointMarksForBatch(child.Sequence, batch.GetEvents()), repacked: &child}
		q.unacked = append(q.unacked, d)
		q.queuedEvents += d.eventCount
	}
	q.nextSeq = max(q.nextSeq, sequence+1)
	slices.SortFunc(q.unacked, func(a, b batchDesc) int {
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
	return nil
}

// Compute parent barriers without mutating terminal state, so PrepareAck and
// Ack agree even when descendants complete out of order or are repacked again.
func (q *queue) repackTerminalParents(sequence uint64, cumulative bool) map[uint64]struct{} {
	if len(q.repacks) == 0 {
		return nil
	}
	parents := make([]uint64, 0, len(q.repacks))
	for parent := range q.repacks {
		parents = append(parents, parent)
	}
	slices.Sort(parents)
	completed := make(map[uint64]struct{}, len(parents))
	for _, parent := range slices.Backward(parents) {
		terminal := true
		for _, child := range q.repacks[parent].Children {
			_, acked := q.terminal[child.Sequence]
			_, descendantsDone := completed[child.Sequence]
			_, childIsParent := q.repacks[child.Sequence]
			hypothetical := !childIsParent && (child.Sequence == sequence || (cumulative && child.Sequence <= sequence))
			if !acked && !descendantsDone && child.Sequence > q.lastAcked && !hypothetical {
				terminal = false
				break
			}
		}
		if terminal {
			completed[parent] = struct{}{}
		}
	}
	return completed
}

func (q *queue) reclaimRepackPlans() error {
	changed := false
	for parent, plan := range q.repacks {
		if plan.Children[len(plan.Children)-1].Sequence > q.lastAcked {
			continue
		}
		if err := os.Remove(filepath.Join(q.dir, repackFileName(parent))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		delete(q.repacks, parent)
		changed = true
	}
	if changed {
		return q.syncDirectory(q.dir)
	}
	return nil
}
