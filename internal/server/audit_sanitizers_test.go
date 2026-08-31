package server

import (
	"context"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
)

func auditSanitizerHandler() *apiHandler {
	return &apiHandler{maximumPageSize: defaultMaximumPageSize}
}

func TestSanitizeListAuditEventsRequest(t *testing.T) {
	t.Parallel()

	oversizedPage := uint32(audit.MaximumListPageSize + 1)
	zeroPage := uint32(0)
	paddedToken := " token "
	oversizedToken := strings.Repeat("t", maximumBoundedListRequestTokenBytes+1)
	paddedActor := " actor "
	oversizedActor := strings.Repeat("a", maximumAuditActorIDBytes+1)
	emptyActor := ""

	tests := []struct {
		name    string
		request *opensplunk.ListAuditEventsRequest
		want    string
	}{
		{
			name:    "empty request is accepted",
			request: &opensplunk.ListAuditEventsRequest{},
		},
		{
			name: "page size must be positive",
			request: &opensplunk.ListAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageSize: &zeroPage},
			},
			want: "audit event page size is invalid",
		},
		{
			name: "page size is bounded by the ledger maximum",
			request: &opensplunk.ListAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageSize: &oversizedPage},
			},
			want: "audit event page size is invalid",
		},
		{
			name: "page token must not be padded",
			request: &opensplunk.ListAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageToken: &paddedToken},
			},
			want: "audit event page token is invalid",
		},
		{
			name: "page token is bounded",
			request: &opensplunk.ListAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageToken: &oversizedToken},
			},
			want: "audit event page token is invalid",
		},
		{
			name: "action filter count is bounded",
			request: &opensplunk.ListAuditEventsRequest{
				ActionFilters: make(
					[]opensplunk.AuditAction,
					audit.MaximumActionFilters+1,
				),
			},
			want: "audit event action filters are invalid",
		},
		{
			name: "action filter must be known",
			request: &opensplunk.ListAuditEventsRequest{
				ActionFilters: []opensplunk.AuditAction{
					opensplunk.AuditAction_AUDIT_ACTION_UNSPECIFIED,
				},
			},
			want: "audit event action filter is invalid",
		},
		{
			name: "action filter must not repeat",
			request: &opensplunk.ListAuditEventsRequest{
				ActionFilters: []opensplunk.AuditAction{
					opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE,
					opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE,
				},
			},
			want: "audit event action filter is duplicated",
		},
		{
			name: "actor filter must not be padded",
			request: &opensplunk.ListAuditEventsRequest{
				ActorIdFilter: &paddedActor,
			},
			want: "audit event actor filter is invalid",
		},
		{
			name: "actor filter must not be empty",
			request: &opensplunk.ListAuditEventsRequest{
				ActorIdFilter: &emptyActor,
			},
			want: "audit event actor filter is invalid",
		},
		{
			name: "actor filter is bounded",
			request: &opensplunk.ListAuditEventsRequest{
				ActorIdFilter: &oversizedActor,
			},
			want: "audit event actor filter is invalid",
		},
		{
			name: "target filter must be known",
			request: &opensplunk.ListAuditEventsRequest{
				TargetKindFilter: opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_UNSPECIFIED.
					Enum(),
			},
			want: "audit event target filter is invalid",
		},
	}

	handler := auditSanitizerHandler()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := handler.sanitizeListAuditEventsRequest(
				context.Background(),
				test.request,
			)
			if sanitized != test.request {
				t.Fatal("sanitizer returned a different request pointer")
			}
			if test.want != "" {
				assertSanitizerRejection(t, err, test.want)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
		})
	}
}

func TestSanitizeListAuditEventsRequestKeepsAcceptedFilters(t *testing.T) {
	t.Parallel()

	actor := "browser-actor"
	pageSize := uint32(7)
	token := "cursor"
	request := &opensplunk.ListAuditEventsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &token,
			IncludeTotalSize: true,
		},
		ActionFilters: []opensplunk.AuditAction{
			opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE,
			opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA,
		},
		ActorIdFilter: &actor,
		TargetKindFilter: opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX.
			Enum(),
	}

	handler := auditSanitizerHandler()
	sanitized, err := handler.sanitizeListAuditEventsRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized.GetActorIdFilter() != actor {
		t.Fatalf("actor filter = %q, want %q", sanitized.GetActorIdFilter(), actor)
	}
	if sanitized.ActorIdFilter == &actor {
		t.Fatal("sanitizer retained the caller's actor filter backing string")
	}
	listRequest := auditListRequest(sanitized)
	if listRequest.PageSize != pageSize ||
		listRequest.PageToken != token ||
		!listRequest.IncludeTotal {
		t.Fatalf("projected page = %+v", listRequest)
	}
	if len(listRequest.ActionFilters) != 2 ||
		listRequest.ActorID == nil ||
		*listRequest.ActorID != actor ||
		listRequest.TargetKind == nil ||
		*listRequest.TargetKind != audit.TargetKindIndex {
		t.Fatalf("projected filters = %+v", listRequest)
	}
}

func TestSanitizeListAuditEventsRequestDefaultsThePage(t *testing.T) {
	t.Parallel()

	handler := auditSanitizerHandler()
	request := &opensplunk.ListAuditEventsRequest{}
	if _, err := handler.sanitizeListAuditEventsRequest(
		context.Background(),
		request,
	); err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if request.GetPage().GetPageSize() != defaultAuditListPageSize {
		t.Fatalf(
			"resolved page size = %d, want %d",
			request.GetPage().GetPageSize(),
			defaultAuditListPageSize,
		)
	}
	if request.GetPage().PageToken != nil {
		t.Fatal("sanitizer set a page token on a request without one")
	}
	listRequest := auditListRequest(request)
	if listRequest.PageSize != defaultAuditListPageSize ||
		listRequest.PageToken != "" ||
		listRequest.IncludeTotal {
		t.Fatalf("projected default page = %+v", listRequest)
	}
}

func TestSanitizeListSearchAttemptAuditEventsRequest(t *testing.T) {
	t.Parallel()

	oversizedPage := uint32(searchaudit.MaximumListPageSize + 1)
	zeroPage := uint32(0)
	paddedToken := " token "
	padded := " identity "
	oversized := strings.Repeat(
		"i",
		maximumSearchAttemptAuditIdentityBytes+1,
	)

	tests := []struct {
		name    string
		request *opensplunk.ListSearchAttemptAuditEventsRequest
		want    string
	}{
		{
			name:    "empty request is accepted",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{},
		},
		{
			name: "page size must be positive",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageSize: &zeroPage},
			},
			want: "search attempt audit page size is invalid",
		},
		{
			name: "page size is bounded by the ledger maximum",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageSize: &oversizedPage},
			},
			want: "search attempt audit page size is invalid",
		},
		{
			name: "page token must not be padded",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				Page: &opensplunk.PageRequest{PageToken: &paddedToken},
			},
			want: "search attempt audit page token is invalid",
		},
		{
			name: "actor filter must not be padded",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				ActorIdFilter: &padded,
			},
			want: "search attempt audit actor filter is invalid",
		},
		{
			name: "actor filter is bounded",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				ActorIdFilter: &oversized,
			},
			want: "search attempt audit actor filter is invalid",
		},
		{
			name: "owner filter must not be padded",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				OwnerIdFilter: &padded,
			},
			want: "search attempt audit owner filter is invalid",
		},
		{
			name: "owner filter is bounded",
			request: &opensplunk.ListSearchAttemptAuditEventsRequest{
				OwnerIdFilter: &oversized,
			},
			want: "search attempt audit owner filter is invalid",
		},
	}

	handler := auditSanitizerHandler()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := handler.
				sanitizeListSearchAttemptAuditEventsRequest(
					context.Background(),
					test.request,
				)
			if sanitized != test.request {
				t.Fatal("sanitizer returned a different request pointer")
			}
			if test.want != "" {
				assertSanitizerRejection(t, err, test.want)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
		})
	}
}

func TestSanitizeListSearchAttemptAuditEventsRequestProjectsFilters(t *testing.T) {
	t.Parallel()

	actor := "actor"
	owner := "owner"
	request := &opensplunk.ListSearchAttemptAuditEventsRequest{
		ActorIdFilter: &actor,
		OwnerIdFilter: &owner,
	}
	handler := auditSanitizerHandler()
	sanitized, err := handler.sanitizeListSearchAttemptAuditEventsRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized.ActorIdFilter == &actor || sanitized.OwnerIdFilter == &owner {
		t.Fatal("sanitizer retained the caller's filter backing strings")
	}
	listRequest := searchAttemptAuditListRequest(sanitized)
	if listRequest.PageSize != defaultSearchAttemptAuditListPageSize ||
		listRequest.ActorID == nil || *listRequest.ActorID != actor ||
		listRequest.OwnerID == nil || *listRequest.OwnerID != owner {
		t.Fatalf("projected request = %+v", listRequest)
	}
}
