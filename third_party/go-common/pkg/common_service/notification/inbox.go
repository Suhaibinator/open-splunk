package notification

import (
	"context"
	"errors"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// inboxConfig samples the configuration once per inbox operation. A handler
// without a store never reaches the store, so it reads the disabled defaults.
func (h *Handler) inboxConfig() *Config {
	if h == nil || h.store == nil {
		return DefaultConfig()
	}
	return h.deps.Config()
}

func (h *Handler) view(ctx context.Context, n *models.Notification, limits sourceLimits) (*pb.Notification, error) {
	decisions, err := h.evaluateRows(ctx, []models.Notification{*n}, SourceOptions{CheckResolution: true, IncludeView: true}, limits)
	if err != nil {
		return nil, err
	}
	return notificationView(n, decisions[0]), nil
}
func notificationView(n *models.Notification, decision SourceDecision) *pb.Notification {
	v := &pb.Notification{Id: n.ID, Type: n.Type, Revision: n.Revision, ReadRevision: n.ReadRevision, Resolved: n.Resolved, CreatedAt: timestamppb.New(n.CreatedAt), UpdatedAt: timestamppb.New(n.UpdatedAt), OccurrenceCount: n.OccurrenceCount}
	if !decision.Authorized {
		v.Unavailable = true
		v.FallbackText = "Notification unavailable"
		return v
	}
	v.Resolved = decision.Resolved
	v.Payload = append([]byte(nil), n.Payload...)
	v.FallbackText = n.FallbackText
	v.ActivityScope = n.ActivityScope
	// Built by the evaluator from the snapshot that authorized this row.
	if len(decision.View) > 0 {
		v.View = append([]byte(nil), decision.View...)
	}
	return v
}
func (h *Handler) Get(ctx context.Context, userID, id uint64) (*pb.Notification, error) {
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return nil, ErrDisabled
	}
	n, err := h.store.Get(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrNotFound
	}
	return h.view(ctx, n, sourceLimitsFor(cfg))
}
func (h *Handler) observeInbox(operation string, examined int, err error) {
	if h == nil {
		return
	}
	if metrics := h.telemetry.inbox; metrics != nil {
		outcome := "returned"
		if err != nil {
			outcome = "error"
		}
		metrics.RecordNotificationInboxCandidatesExamined(operation, outcome, examined)
	}
}

// List examines at most the configured candidate budget. Unread filtering can
// therefore produce a short or empty page with a continuation. That cursor is the last examined ID;
// clients must continue while nonzero, even when the current page has no entries.
func (h *Handler) List(ctx context.Context, userID uint64, req *pb.ListNotificationsRequest) (result *pb.ListNotificationsResponse, err error) {
	examined := 0
	defer func() { h.observeInbox("list", examined, err) }()
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return nil, ErrDisabled
	}
	if req == nil {
		req = &pb.ListNotificationsRequest{}
	}
	if req.ActivityScope != nil && !notificationkey.ValidActivityScope(*req.ActivityScope) {
		return nil, ErrInvalid
	}
	limit := int(req.Limit)
	if limit == 0 {
		limit = cfg.ListDefaultLimit
	}
	if limit > cfg.ListMaxLimit {
		return nil, ErrInvalid
	}
	result = &pb.ListNotificationsResponse{}
	cursor := req.BeforeId
	for examined < cfg.ListCandidateBudget && len(result.Notifications) < limit {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		fetch := min(cfg.InboxFetchRows, cfg.ListCandidateBudget-examined)
		var rows []models.Notification
		if req.ActivityScope != nil {
			rows, err = h.store.ListByActivityScope(ctx, userID, *req.ActivityScope, fetch, cursor, req.UnreadOnly)
		} else if req.UnreadOnly {
			rows, err = h.store.ListUnread(ctx, userID, fetch, cursor)
		} else {
			rows, err = h.store.List(ctx, userID, fetch, cursor)
		}
		if err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return result, nil
		}
		examined += len(rows)
		decisions, e := h.evaluateRows(ctx, rows, SourceOptions{CheckResolution: true, IncludeView: true}, sourceLimitsFor(cfg))
		if e != nil {
			return nil, e
		}
		for index := range rows {
			n := &rows[index]
			cursor = n.ID
			decision := decisions[index]
			if req.UnreadOnly && (!decision.Authorized || decision.Resolved || n.ReadRevision >= n.Revision) {
				continue
			}
			result.Notifications = append(result.Notifications, notificationView(n, decision))
			if len(result.Notifications) == limit {
				// Over-fetched rows remain reachable from the last admitted/examined ID.
				if index+1 < len(rows) || len(rows) == fetch {
					result.NextBeforeId = cursor
				}
				return result, nil
			}
		}
		if len(rows) < fetch {
			return result, nil
		}
		if examined >= cfg.ListCandidateBudget || len(result.Notifications) >= limit {
			result.NextBeforeId = cursor
			return result, nil
		}
	}
	return result, nil
}

// CountResult is either exact or a confirmed lower bound under current authorization.
type CountResult struct {
	UnreadCount uint64
	Complete    bool
}

var errCountBudget = errors.New("notification count operation budget exhausted")

// Count bounds source evaluation work. Only fully evaluated batches contribute
// to the result; callbacks must honor the supplied context.
func (h *Handler) Count(parent context.Context, userID uint64) (result CountResult, err error) {
	examined := 0
	reason := "error"
	defer func() {
		h.observeInbox("count", examined, err)
		if h == nil {
			return
		}
		if m := h.telemetry.count; m != nil {
			m.RecordNotificationCountCompletion(reason)
		}
	}()
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return CountResult{}, ErrDisabled
	}
	ctx, cancel := context.WithTimeoutCause(parent, cfg.CountOperationTimeout, errCountBudget)
	defer cancel()
	// Check the caller first, including when its cancellation races our budget.
	finishError := func(e error) (CountResult, error) {
		if parent.Err() != nil {
			return CountResult{}, parent.Err()
		}
		if errors.Is(context.Cause(ctx), errCountBudget) &&
			(errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled)) {
			reason = "time_budget"
			return result, nil
		}
		return CountResult{}, e
	}
	var cursor uint64
	for examined < cfg.CountCandidateBudget {
		if e := ctx.Err(); e != nil {
			return finishError(e)
		}
		fetch := min(cfg.InboxFetchRows, cfg.CountCandidateBudget-examined)
		rows, e := h.store.ListUnreadCandidates(ctx, userID, fetch, cursor)
		if e != nil {
			return finishError(e)
		}
		if e := ctx.Err(); e != nil {
			return finishError(e)
		}
		if len(rows) == 0 {
			reason = "exhausted"
			result.Complete = true
			return result, nil
		}
		examined += len(rows)
		decisions, e := h.evaluateRows(ctx, rows, SourceOptions{CheckResolution: true}, sourceLimitsFor(cfg))
		if e != nil {
			return finishError(e)
		}
		if e := ctx.Err(); e != nil {
			return finishError(e)
		}
		for index := range rows {
			n := &rows[index]
			cursor = n.ID
			if n.Revision > n.ReadRevision && !n.Resolved && decisions[index].Authorized && !decisions[index].Resolved {
				result.UnreadCount++
				if result.UnreadCount == uint64(cfg.CountUnreadLimit) {
					reason = "count_cap"
					return result, nil
				}
			}
		}
		if result.UnreadCount >= uint64(cfg.CountUnreadLimit) {
			reason = "count_cap"
			return result, nil
		}
		if len(rows) < fetch {
			reason = "exhausted"
			result.Complete = true
			return result, nil
		}
	}
	reason = "candidate_budget"
	return result, nil
}
