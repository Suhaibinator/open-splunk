package common_service

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

const nativeChatNotificationProducer = "native_chat"

// nativeChatNotificationPageSize sizes producer snapshot pages and approver
// batches. Persistence rejects fan-out pages above the shared KMS bound, so the
// page size is that bound rather than an independent literal.
const nativeChatNotificationPageSize = config.MaxNotificationFanoutBatchRecipients

// nativeChatNotificationCursor is a keyset position for both inline and async
// recipient enumeration; async cursors are the only ones persisted.
type nativeChatNotificationCursor struct {
	After uint64 `json:"after"`
}

// nativeChatRecipientMetadata is the persisted per-candidate envelope carrying
// the recipient's membership stamp. Ineligible approvers are filtered before
// persistence and never stored.
type nativeChatRecipientMetadata struct {
	MembershipStamp models.NotificationTransactionStamp `json:"membership_stamp"`
}

// The native chat producer validates ChatNotificationSourcePayload envelopes.
func (h *CommonServiceHandler[U]) registerNativeNotificationProducers() error {
	return h.notificationHandler.RegisterNativeProducer(notificationnative.Native(), notification.Producer{
		Name:  nativeChatNotificationProducer,
		Types: []uint32{NotificationTypeChatUnread, NotificationTypeChatApprovalRequired},
		Validate: func(_ context.Context, in notification.EventInput) error {
			p, mentions, err := parseNativeChatNotificationSource(in.Payload)
			if err != nil {
				return err
			}
			if in.SourceKey != fmt.Sprintf("message:%d", p.MessageId) || (in.Type == NotificationTypeChatApprovalRequired && (p.AgentRunId == 0 || len(mentions) != 0)) {
				return notification.ErrInvalid
			}
			return nil
		},
		Snapshot: h.snapshotNativeChatNotification,
	}, h.prepareNativeChatNotification)
}

func (h *CommonServiceHandler[U]) snapshotNativeChatNotification(ctx context.Context, event notification.ProducerEvent, opaque []byte) (notification.RecipientPage, error) {
	var cursor nativeChatNotificationCursor
	if len(opaque) != 0 {
		if err := json.Unmarshal(opaque, &cursor); err != nil {
			return notification.RecipientPage{}, notification.ErrInvalid
		}
	}
	var err error
	event, _, err = nativeChatTargetEvent(event)
	if err != nil {
		return notification.RecipientPage{}, err
	}
	row, err := h.nativeNotificationMessage(ctx, event.Payload)
	if err != nil || row == nil {
		return notification.RecipientPage{Done: true}, err
	}
	approval := event.Type == NotificationTypeChatApprovalRequired
	if approval {
		return h.snapshotNativeChatApprovers(ctx, event, row, cursor)
	}
	if enabled, err := h.chatUnreadNotificationsEnabled(ctx, row.ChannelID); err != nil || !enabled {
		return notification.RecipientPage{Done: true}, err
	}
	page := notification.RecipientPage{}
	if event.Async {
		rows, err := h.commonPersistenceHandler.ListChatNotificationRecipientCandidates(ctx, row.ChannelID, cursor.After, nativeChatNotificationPageSize, event.SourceStamp, event.SourceSnapshot)
		if err != nil {
			return page, err
		}
		for _, recipient := range rows {
			metadata, err := json.Marshal(nativeChatRecipientMetadata{MembershipStamp: recipient.MembershipStamp})
			if err != nil {
				return page, err
			}
			page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: recipient.RecipientID, Metadata: metadata})
			cursor.After = recipient.RecipientID
		}
		page.Done = len(rows) < nativeChatNotificationPageSize
	} else {
		for len(page.Candidates) < nativeChatNotificationPageSize {
			limit := min(persistence.ChatNotificationCandidateBatchSize, nativeChatNotificationPageSize-len(page.Candidates))
			ids, err := h.commonPersistenceHandler.ListChatNotificationRecipientIDs(ctx, row.ChannelID, cursor.After, limit)
			if err != nil {
				return page, err
			}
			for _, id := range ids {
				page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: id})
			}
			if len(ids) > 0 {
				if ids[len(ids)-1] <= cursor.After {
					return page, notification.ErrInvalid
				}
				cursor.After = ids[len(ids)-1]
			}
			if len(ids) < limit {
				page.Done = true
				break
			}
		}
	}
	page.Candidates = slices.DeleteFunc(page.Candidates, func(candidate notification.RecipientCandidate) bool { return candidate.RecipientID == row.SenderID })
	page.Cursor, err = json.Marshal(cursor)
	return page, err
}

func (h *CommonServiceHandler[U]) snapshotNativeChatApprovers(ctx context.Context, event notification.ProducerEvent, row *models.ChatMessage, cursor nativeChatNotificationCursor) (notification.RecipientPage, error) {
	page := notification.RecipientPage{}
	now := time.Now()
	if nativeApprovalMessageResolved(row, now) {
		return notification.RecipientPage{Done: true}, nil
	}
	run, err := h.commonPersistenceHandler.GetChatAgentRunByID(ctx, *row.AgentRunID)
	if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
		return notification.RecipientPage{Done: true}, nil
	}
	if err != nil || run == nil {
		return notification.RecipientPage{Done: true}, err
	}
	// Resolution must precede the application callback, even for a later page.
	if nativeApprovalSourceResolved(row, run, now) {
		return notification.RecipientPage{Done: true}, nil
	}
	if h.notificationApproverPage != nil {
		return h.snapshotPagedNativeChatApprovers(ctx, event, row, run, cursor)
	}
	var recipients []uint64
	if run.TriggeringUserID != nil {
		recipients = append(recipients, *run.TriggeringUserID)
	}
	if h.notificationApprovers != nil {
		extra, err := h.notificationApprovers(ctx, row.ID)
		if err != nil {
			return page, err
		}
		recipients = append(recipients, extra...)
	}
	slices.Sort(recipients)
	recipients = slices.Compact(recipients)
	recipients = slices.DeleteFunc(recipients, func(id uint64) bool { return id == 0 || id == row.SenderID || id <= cursor.After })
	page.Done = len(recipients) == 0
	// Advance over ineligible candidates without persisting them. A nonterminal
	// page must include actual work, even if several raw pages were filtered out.
	for len(recipients) > 0 {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		batch := recipients[:min(len(recipients), nativeChatNotificationPageSize)]
		recipients = recipients[len(batch):]
		cursor.After = batch[len(batch)-1]
		page.Done = len(recipients) == 0
		if event.Async {
			eligible, err := h.commonPersistenceHandler.FilterChatNotificationRecipientCandidates(ctx, row.ChannelID, batch, event.SourceStamp, event.SourceSnapshot)
			if err != nil {
				return page, err
			}
			for _, recipient := range eligible {
				metadata, err := json.Marshal(nativeChatRecipientMetadata{MembershipStamp: recipient.MembershipStamp})
				if err != nil {
					return page, err
				}
				page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: recipient.RecipientID, Metadata: metadata})
			}
		} else {
			for _, id := range batch {
				page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: id})
			}
		}
		if len(page.Candidates) > 0 {
			break
		}
	}
	page.Cursor, err = json.Marshal(cursor)
	return page, err
}

func (h *CommonServiceHandler[U]) snapshotPagedNativeChatApprovers(ctx context.Context, event notification.ProducerEvent, row *models.ChatMessage, run *models.ChatAgentRun, cursor nativeChatNotificationCursor) (notification.RecipientPage, error) {
	for {
		if err := ctx.Err(); err != nil {
			return notification.RecipientPage{}, err
		}
		limit := nativeChatNotificationPageSize
		if run.TriggeringUserID != nil && *run.TriggeringUserID > cursor.After {
			limit--
		}
		ids, err := h.notificationApproverPage(ctx, row.ID, cursor.After, limit)
		if err != nil {
			return notification.RecipientPage{}, err
		}
		if len(ids) > limit {
			return notification.RecipientPage{}, notification.ErrInvalid
		}
		last := cursor.After
		for _, id := range ids {
			if id <= last {
				return notification.RecipientPage{}, notification.ErrInvalid
			}
			last = id
		}
		done := len(ids) < limit
		// The requesting user joins only once their position is reached. Adding
		// a larger ID earlier would skip the intervening application audience.
		if run.TriggeringUserID != nil && *run.TriggeringUserID > cursor.After && (done || *run.TriggeringUserID <= last) {
			ids = append(slices.Clone(ids), *run.TriggeringUserID)
			slices.Sort(ids)
			ids = slices.Compact(ids)
			last = max(last, *run.TriggeringUserID)
		}
		cursor.After = last
		ids = slices.DeleteFunc(ids, func(id uint64) bool { return id == 0 || id == row.SenderID })
		page := notification.RecipientPage{Done: done}
		if event.Async && len(ids) > 0 {
			eligible, err := h.commonPersistenceHandler.FilterChatNotificationRecipientCandidates(ctx, row.ChannelID, ids, event.SourceStamp, event.SourceSnapshot)
			if err != nil {
				return page, err
			}
			for _, recipient := range eligible {
				metadata, err := json.Marshal(nativeChatRecipientMetadata{MembershipStamp: recipient.MembershipStamp})
				if err != nil {
					return page, err
				}
				page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: recipient.RecipientID, Metadata: metadata})
			}
		} else {
			for _, id := range ids {
				page.Candidates = append(page.Candidates, notification.RecipientCandidate{RecipientID: id})
			}
		}
		if len(page.Candidates) > 0 || done {
			page.Cursor, err = json.Marshal(cursor)
			return page, err
		}
	}
}

func (h *CommonServiceHandler[U]) prepareNativeChatNotification(ctx context.Context, event notification.ProducerEvent, recipients []notification.RecipientCandidate) ([]notificationnative.PreparedInput[notification.EmitInput], error) {
	event, mentionIDs, err := nativeChatTargetEvent(event)
	if err != nil {
		return nil, err
	}
	payload, err := parseChatTarget(event.Payload)
	if err != nil {
		return nil, err
	}
	approval := event.Type == NotificationTypeChatApprovalRequired
	if approval {
		resolved, err := h.nativeApprovalResolved(ctx, event.Payload)
		if err != nil || resolved {
			return nil, err
		}
	}
	ids := make([]uint64, 0, len(recipients))
	metadata := map[uint64]nativeChatRecipientMetadata{}
	for _, recipient := range recipients {
		if recipient.RecipientID == 0 {
			continue
		}
		if event.Async {
			var stamp nativeChatRecipientMetadata
			if err := json.Unmarshal(recipient.Metadata, &stamp); err != nil {
				return nil, notification.ErrInvalid
			}
			metadata[recipient.RecipientID] = stamp
		}
		ids = append(ids, recipient.RecipientID)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	if !event.Async {
		if err := h.commonPersistenceHandler.LockChatActivityEmission(ctx, payload.ChannelId, ids); err != nil {
			return nil, err
		}
	}
	candidates := make([]notification.SourceCandidate, len(ids))
	for i, id := range ids {
		candidates[i] = notification.SourceCandidate{RecipientID: id, Payload: event.Payload}
	}
	var observed []persistence.ChatNotificationSourceState
	if event.Async {
		observed = make([]persistence.ChatNotificationSourceState, len(candidates))
	}
	decisions, err := h.evaluateNativeNotificationSourcesObserved(ctx, candidates, notification.SourceOptions{CheckResolution: approval}, approval, observed)
	if err != nil {
		return nil, notification.SourceAuthorizationError(err)
	}
	b := event.Payload
	if !approval {
		b, err = proto.Marshal(&pb.ChatNotificationPayload{ChannelId: payload.ChannelId})
		if err != nil {
			return nil, err
		}
	}
	inputs := make([]notificationnative.PreparedInput[notification.EmitInput], 0, len(ids))
	batchChecker, canBatchCheck := h.commonPersistenceHandler.(persistence.ChatNotificationRecipientBatchPersistence)
	checks := make([]persistence.ChatNotificationRecipientCheck, 0, len(ids))
	var checked []bool
	var checkErr error
	for i, id := range ids {
		if !decisions[i].Authorized || decisions[i].Resolved {
			continue
		}
		in := notificationnative.PreparedInput[notification.EmitInput]{Input: notification.EmitInput{OrganizationID: event.OrganizationID, RecipientID: id, Type: event.Type, Payload: b, FallbackText: event.FallbackText, SourceKey: event.SourceKey, SourceTime: event.SourceTime}}
		// Validate rejects approval events that carry mentions.
		if _, mentioned := slices.BinarySearch(mentionIDs, id); mentioned {
			in.Input.DeliveryTag = nativeChatMentionDeliveryTag
		}
		if event.Async {
			stamp := metadata[id].MembershipStamp
			source := observed[i]
			in.SkipRetirementFence = true
			// RecordMany invokes this DB-only read after acquiring the recipient/type
			// lock. Application authorization above never runs under that lock.
			if canBatchCheck {
				index := len(checks)
				checks = append(checks, persistence.ChatNotificationRecipientCheck{RecipientID: id, Observed: stamp, Source: source})
				in.FreshCheck = func(checkCtx context.Context) (bool, error) {
					if checked == nil && checkErr == nil {
						checked, checkErr = batchChecker.ValidateChatNotificationRecipients(checkCtx, payload.ChannelId, payload.MessageId, checks)
						if checkErr == nil && len(checked) != len(checks) {
							checkErr = notification.ErrInvalid
						}
					}
					if checkErr != nil {
						return false, checkErr
					}
					return checked[index], nil
				}
			} else {
				in.FreshCheck = func(checkCtx context.Context) (bool, error) {
					return h.commonPersistenceHandler.ValidateChatNotificationRecipient(checkCtx, payload.ChannelId, payload.MessageId, id, stamp, source)
				}
			}
		}
		inputs = append(inputs, in)
	}
	return inputs, nil
}
