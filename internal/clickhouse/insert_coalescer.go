package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

type reconstructedWriteGroup struct {
	rows              [][]any
	monthlyPartitions uint64
}

func (s *Store) reconstructWriteGroup(
	ctx context.Context,
	group visibility.WriteGroup,
) (reconstructedWriteGroup, error) {
	if err := visibility.ValidateWriteGroup(group, s.writeGroupLimits); err != nil {
		return reconstructedWriteGroup{}, fmt.Errorf("validate durable ClickHouse write group: %w", err)
	}
	rows := make([][]any, 0, group.RowCount)
	partitions := make(map[int64]struct{})
	var actualRows, actualBytes uint64
	for memberIndex, member := range group.Members {
		replayBatch, err := decodeStoreOutbox(member.Outbox)
		if err != nil {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"decode durable ClickHouse write-group member %d: %w",
				memberIndex,
				err,
			)
		}
		replayDigest, err := storePayloadDigest(replayBatch)
		if err != nil || replayDigest != member.PayloadSHA256 ||
			deduplicationToken(replayBatch) != member.BatchKey ||
			sequenceIdentityKey(replayBatch) != member.SequenceKey {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"durable ClickHouse write-group member %d identity does not match its reservation",
				memberIndex,
			)
		}
		rowCount, decodedBytes, err := reservationAccounting(replayBatch)
		if err != nil || rowCount != member.StoredRowCount || decodedBytes != member.DecodedEventBytes {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"durable ClickHouse write-group member %d accounting does not match its reservation",
				memberIndex,
			)
		}
		memberRows, err := s.rowsForBatch(ctx, replayBatch, &member)
		if err != nil {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"rebuild durable ClickHouse write-group member %d: %w",
				memberIndex,
				err,
			)
		}
		if len(memberRows) != int(rowCount) {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"durable ClickHouse write-group member %d rebuilt an invalid row count",
				memberIndex,
			)
		}
		if err := applyReservation(memberRows, member); err != nil {
			return reconstructedWriteGroup{}, fmt.Errorf(
				"apply durable ClickHouse write-group member %d visibility: %w",
				memberIndex,
				err,
			)
		}
		if math.MaxUint64-actualRows < uint64(len(memberRows)) ||
			math.MaxUint64-actualBytes < decodedBytes {
			return reconstructedWriteGroup{}, errors.New("durable ClickHouse write-group accounting overflows")
		}
		actualRows += uint64(len(memberRows))
		actualBytes += decodedBytes
		for rowIndex, row := range memberRows {
			if len(row) != len(eventInsertColumns) {
				return reconstructedWriteGroup{}, fmt.Errorf(
					"durable ClickHouse write-group member %d row %d has an invalid shape",
					memberIndex,
					rowIndex,
				)
			}
			eventTime, ok := row[3].(time.Time)
			if !ok {
				return reconstructedWriteGroup{}, fmt.Errorf(
					"durable ClickHouse write-group member %d row %d has invalid event time",
					memberIndex,
					rowIndex,
				)
			}
			year, month, _ := eventTime.Date()
			partitions[int64(year)*100+int64(month)] = struct{}{}
		}
		rows = append(rows, memberRows...)
	}
	if actualRows != group.RowCount || actualBytes != group.DecodedBytes ||
		actualRows > s.writeGroupLimits.MaxRows ||
		actualBytes > s.writeGroupLimits.MaxDecodedBytes {
		return reconstructedWriteGroup{}, errors.New("durable ClickHouse write-group reconstructed totals are invalid")
	}
	return reconstructedWriteGroup{
		rows:              rows,
		monthlyPartitions: uint64(len(partitions)),
	}, nil
}

func (s *Store) sendWriteGroup(
	ctx context.Context,
	group visibility.WriteGroup,
	attemptID string,
	reason visibility.WriteGroupFillReason,
) (resultErr error) {
	reconstructed, err := s.reconstructWriteGroup(ctx, group)
	if err != nil {
		return s.releaseWriteGroup(group.ID, attemptID, err)
	}
	origin := group.Members[0].CreatedAt
	if origin.IsZero() {
		origin = group.CreatedAt
	}
	if reason == visibility.WriteGroupFillRecovery {
		s.coalescingMetrics.ObserveRecoveredGroup()
	} else {
		s.coalescingMetrics.ObserveGroupFormed(
			coalescerFillReason(reason),
			uint64(group.MemberCount),
			group.RowCount,
			group.DecodedBytes,
			reconstructed.monthlyPartitions,
			group.CreatedAt.Sub(origin),
		)
	}

	prepared, err := s.connection.prepare(ctx, s.insertSQL, insertSettings(group.ID))
	if err != nil {
		return s.releaseWriteGroup(
			group.ID,
			attemptID,
			s.classifyError(fmt.Errorf("prepare ClickHouse write group: %w", err)),
		)
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		if closeErr := prepared.Close(); closeErr != nil {
			resultErr = errors.Join(
				resultErr,
				s.classifyError(fmt.Errorf("close ClickHouse write group: %w", closeErr)),
			)
		}
	}()

	for rowIndex, row := range reconstructed.rows {
		if err := prepared.Append(row...); err != nil {
			abortErr := prepared.Abort()
			return s.releaseWriteGroup(
				group.ID,
				attemptID,
				errors.Join(
					s.classifyError(fmt.Errorf("append ClickHouse write-group row %d: %w", rowIndex, err)),
					abortErr,
				),
			)
		}
	}
	if err := s.writeGroups.MarkWriteGroupSending(ctx, group.ID, attemptID); err != nil {
		abortErr := prepared.Abort()
		return s.releaseWriteGroup(
			group.ID,
			attemptID,
			errors.Join(
				s.finalizationFailure("mark ClickHouse write group sending", err),
				abortErr,
			),
		)
	}
	sendStarted := s.clock().UTC()
	s.coalescingMetrics.ObservePhysicalSend(group.RowCount, sendStarted.Sub(origin))
	if err := prepared.Send(); err != nil {
		s.coalescingMetrics.ObserveAmbiguity()
		abortErr := prepared.Abort()
		return s.releaseWriteGroup(
			group.ID,
			attemptID,
			errors.Join(
				s.classifyError(fmt.Errorf("send ClickHouse write group: %w", err)),
				abortErr,
			),
		)
	}
	committedAt := s.clock().UTC().Truncate(time.Microsecond)
	committedSequences, err := s.writeGroups.CommitWriteGroup(
		ctx,
		group.ID,
		attemptID,
		committedAt,
	)
	if err != nil {
		commitErr := s.finalizationFailure("commit ClickHouse write group", err)
		s.notifyWriteGroupHint(group)
		return s.releaseWriteGroup(group.ID, attemptID, commitErr)
	}
	expectedSequences := writeGroupSequences(group)
	s.notifyNativeWaiters(expectedSequences)
	for range group.MemberCount {
		s.noteTerminalReservation()
	}
	s.coalescingMetrics.ObserveGroupSuccess(committedAt.Sub(origin))
	if !slices.Equal(committedSequences, expectedSequences) {
		return errors.New("commit ClickHouse write group returned invalid member sequences")
	}
	if err := prepared.Close(); err != nil {
		closed = true
		return s.classifyError(fmt.Errorf("close committed ClickHouse write group: %w", err))
	}
	closed = true
	return nil
}

// notifyWriteGroupHint wakes the bounded in-memory waiter set in one pass after
// an outcome-ambiguous SQLite group commit. It deliberately performs no
// per-member durable reads: each native waiter re-registers before its own
// authoritative lookup, so this hint is safe whether zero, some, or all group
// members actually committed.
func (s *Store) notifyWriteGroupHint(group visibility.WriteGroup) {
	s.notifyNativeWaiters(writeGroupSequences(group))
}

func (s *Store) notifyNativeWaiters(sequences []uint64) {
	notificationCount := s.commitWaiters.notify(sequences)
	for range notificationCount {
		s.coalescingMetrics.RemoveNativeWaiter()
	}
	s.coalescingMetrics.ObserveNativeWaiterWakeups(notificationCount)
}

func (s *Store) notifyAllNativeWaiters() {
	notificationCount := s.commitWaiters.notifyAll()
	for range notificationCount {
		s.coalescingMetrics.RemoveNativeWaiter()
	}
	s.coalescingMetrics.ObserveNativeWaiterWakeups(notificationCount)
}

func writeGroupSequences(group visibility.WriteGroup) []uint64 {
	sequences := make([]uint64, len(group.Members))
	for index, member := range group.Members {
		sequences[index] = member.Sequence
	}
	return sequences
}

func (s *Store) releaseWriteGroup(groupID, attemptID string, operationErr error) error {
	defer s.wakeReconciler()
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.writeGroups.ReleaseWriteGroup(ctx, groupID, attemptID); err != nil {
		return errors.Join(
			operationErr,
			s.finalizationFailure("release ClickHouse write-group attempt", err),
		)
	}
	return operationErr
}

func coalescerFillReason(reason visibility.WriteGroupFillReason) CoalescerFillReason {
	switch reason {
	case visibility.WriteGroupFillRowTarget:
		return CoalescerFillRowTarget
	case visibility.WriteGroupFillByteTarget:
		return CoalescerFillByteTarget
	case visibility.WriteGroupFillHardBoundary:
		return CoalescerFillHardBoundary
	case visibility.WriteGroupFillLinger:
		return CoalescerFillLinger
	case visibility.WriteGroupFillDrain:
		return CoalescerFillDrain
	case visibility.WriteGroupFillRecovery:
		return CoalescerFillRecovery
	default:
		return CoalescerFillUnknown
	}
}
