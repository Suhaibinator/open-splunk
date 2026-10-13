// Package authorization evaluates application resource policy using GoCommon identity and membership.
package authorization

import (
	"context"
	"errors"
	"strconv"

	"fortio.org/safecast"
)

const (
	ViewerRole        int32 = 1
	AdminRole         int32 = 23
	OwnerRole         int32 = 44
	SiteAdminUserType int32 = 44
)

type Capability string

const (
	Search Capability = "search"
	Ingest Capability = "ingest"
	Manage Capability = "manage"
	Grants Capability = "manage_grants"
)

func (c Capability) Valid() bool { return c == Search || c == Ingest || c == Manage || c == Grants }

var (
	ErrUnauthorized  = errors.New("authorization: authentication required")
	ErrForbidden     = errors.New("authorization: permission denied")
	ErrUnassigned    = errors.New("authorization: index has no owning organization")
	ErrInvalidPolicy = errors.New("authorization: invalid policy")
)

type Subject struct {
	UserID   uint64
	UserType int32
}

func (s Subject) Valid() bool {
	return s.UserID > 0 && s.UserType > 0 && s.UserType <= SiteAdminUserType
}
func (s Subject) IsSiteAdmin() bool { return s.Valid() && s.UserType == SiteAdminUserType }
func (s Subject) OwnerID() string   { return strconv.FormatUint(s.UserID, 10) }

type contextKey struct{}
type Scope struct {
	Subject        Subject
	OrganizationID int64
	PhysicalTenant string
}

func WithScope(ctx context.Context, scope Scope) context.Context {
	return context.WithValue(ctx, contextKey{}, scope)
}
func FromContext(ctx context.Context) (Scope, bool) {
	if ctx == nil {
		return Scope{}, false
	}
	s, ok := ctx.Value(contextKey{}).(Scope)
	return s, ok && s.Subject.Valid()
}

// MembershipResolver must use current persisted GoCommon membership, including its lineage rules.
type MembershipResolver interface {
	EffectiveRole(context.Context, uint64, int64) (int32, bool, error)
}
type Rule struct {
	IndexID     string     `json:"-"`
	SubjectKind string     `json:"subject_kind"`
	SubjectID   uint64     `json:"subject_id"`
	Capability  Capability `json:"capability"`
	Effect      string     `json:"effect"`
}
type Store interface {
	OrganizationForIndex(context.Context, string) (int64, error)
	Rules(context.Context, string) ([]Rule, error)
}
type Service struct {
	store          Store
	memberships    MembershipResolver
	physicalTenant string
}

func NewService(store Store, memberships MembershipResolver, physicalTenant string) (*Service, error) {
	if store == nil || memberships == nil || physicalTenant == "" {
		return nil, ErrInvalidPolicy
	}
	if postgres, ok := store.(*PostgresStore); ok {
		authority, ok := memberships.(transactionAuthority)
		if !ok {
			return nil, ErrInvalidPolicy
		}
		postgres.transactionContext = authority.AuthorizationTransactionContext
		postgres.currentSubject = authority.CurrentSubjectInTransaction
	}
	return &Service{store: store, memberships: memberships, physicalTenant: physicalTenant}, nil
}
func (s *Service) PhysicalTenant() string { return s.physicalTenant }
func (s *Service) OrganizationForIndex(ctx context.Context, indexID string) (int64, error) {
	return s.store.OrganizationForIndex(ctx, indexID)
}
func (s *Service) OrganizationRole(ctx context.Context, subject Subject, organizationID int64) (int32, error) {
	if !subject.Valid() {
		return 0, ErrUnauthorized
	}
	if organizationID <= 0 {
		return 0, ErrForbidden
	}
	role, member, err := s.memberships.EffectiveRole(ctx, subject.UserID, organizationID)
	if err != nil {
		return 0, err
	}
	if !member || !ValidRole(role) {
		return 0, ErrForbidden
	}
	return role, nil
}
func ValidRole(role int32) bool { return role == ViewerRole || role == AdminRole || role == OwnerRole }
func (s *Service) AuthorizeOrganization(ctx context.Context, subject Subject, organizationID int64, minimumRole int32) error {
	if !subject.Valid() {
		return ErrUnauthorized
	}
	if subject.IsSiteAdmin() {
		return nil
	}
	role, err := s.OrganizationRole(ctx, subject, organizationID)
	if err != nil {
		return err
	}
	if role < minimumRole {
		return ErrForbidden
	}
	return nil
}
func (s *Service) AuthorizeIndex(ctx context.Context, subject Subject, organizationID int64, indexID string, capability Capability) error {
	if !subject.Valid() {
		return ErrUnauthorized
	}
	if !capability.Valid() || indexID == "" {
		return ErrInvalidPolicy
	}
	// Site administrators bypass membership and every resource override.
	if subject.IsSiteAdmin() {
		return nil
	}
	policy, err := s.policy(ctx, indexID)
	if err != nil {
		return err
	}
	if organizationID <= 0 || policy.OrganizationID != organizationID {
		return ErrForbidden
	}
	role, err := s.OrganizationRole(ctx, subject, policy.OrganizationID)
	if err != nil {
		return err
	}
	return Evaluate(subject, role, capability, policy.Rules)
}

// Evaluate requires membership to have been established by the caller. A deny always wins for non-siteadmins.
func Evaluate(subject Subject, role int32, capability Capability, rules []Rule) error {
	if !subject.Valid() {
		return ErrUnauthorized
	}
	if !capability.Valid() {
		return ErrInvalidPolicy
	}
	if subject.IsSiteAdmin() {
		return nil
	}
	if !ValidRole(role) {
		return ErrForbidden
	}
	allowed := role >= AdminRole || capability == Search
	for _, rule := range rules {
		if rule.Capability != capability {
			continue
		}
		if (rule.SubjectKind == "user" && rule.SubjectID == subject.UserID) || (rule.SubjectKind == "role" && rule.SubjectID == safecast.MustConv[uint64](role)) {
			if rule.Effect == "deny" {
				return ErrForbidden
			}
			if rule.Effect == "allow" {
				allowed = true
			}
		}
	}
	if !allowed {
		return ErrForbidden
	}
	return nil
}
func (s *Service) Rules(ctx context.Context, subject Subject, organizationID int64, indexID string) ([]Rule, error) {
	policy, _, err := s.GetPolicy(ctx, subject, organizationID, indexID)
	return policy.Rules, err
}
func validRule(r Rule) bool {
	return r.IndexID != "" && r.SubjectID > 0 && r.SubjectID <= 1<<63-1 && r.Capability.Valid() && (r.Effect == "allow" || r.Effect == "deny") && (r.SubjectKind == "user" || r.SubjectKind == "role" && (r.SubjectID == uint64(ViewerRole) || r.SubjectID == uint64(AdminRole) || r.SubjectID == uint64(OwnerRole)))
}
