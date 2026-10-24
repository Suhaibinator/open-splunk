// Package authz owns open-splunk's static role-based access model: the
// capability vocabulary, the organization-role to capability table, and the
// browser API route to required-capability table.
//
// The model is deliberately small and closed. Organization roles mirror
// go-common's effective OrganizationRole (member, moderator, admin, owner);
// the go-common site Admin user tier is the only other input. Capability ids
// are wire-stable strings shared verbatim with the TypeScript client.
package authz

// Capability names one permission a principal may hold in an organization.
// Its string value is the canonical id shared with the browser client.
type Capability string

// Member capabilities.
const (
	// CapabilitySearch runs searches and manages the caller's own search
	// jobs, retained results, exports, and history.
	CapabilitySearch Capability = "search"
	// CapabilityObjectsOwn manages the caller's own saved searches and
	// dashboards.
	CapabilityObjectsOwn Capability = "objects.own"
)

// Moderator capabilities.
const (
	// CapabilityKnowledgeWrite creates and changes knowledge objects.
	CapabilityKnowledgeWrite Capability = "knowledge.write"
	// CapabilityLookupsWrite creates and changes lookup tables.
	CapabilityLookupsWrite Capability = "lookups.write"
	// CapabilityAlertsWrite creates, changes, and runs alerts.
	CapabilityAlertsWrite Capability = "alerts.write"
	// CapabilityReportsWrite schedules and runs scheduled reports.
	CapabilityReportsWrite Capability = "reports.write"
)

// Admin capabilities.
const (
	// CapabilityIndexesAdmin administers indexes, their policies, statistics,
	// field catalogs, and the physical search plan surface.
	CapabilityIndexesAdmin Capability = "indexes.admin"
	// CapabilityIngestionAdmin administers ingestion tokens (canonical id
	// "ingest_tokens.admin").
	CapabilityIngestionAdmin Capability = "ingest_tokens.admin"
	// CapabilityCollectorsAdmin administers native collectors.
	CapabilityCollectorsAdmin Capability = "collectors.admin"
	// CapabilityHECAdmin reads HEC operational state.
	CapabilityHECAdmin Capability = "hec.admin"
	// CapabilityAppsAdmin administers apps.
	CapabilityAppsAdmin Capability = "apps.admin"
	// CapabilityAuditRead reads the audit trails.
	CapabilityAuditRead Capability = "audit.read"
	// CapabilityKnowledgeAdmin performs destructive knowledge operations such
	// as quarantine.
	CapabilityKnowledgeAdmin Capability = "knowledge.admin"
	// CapabilityMembersAdmin manages organization membership.
	CapabilityMembersAdmin Capability = "members.admin"
)

// Owner capabilities.
const (
	// CapabilityOrgAdmin administers the organization itself.
	CapabilityOrgAdmin Capability = "org.admin"
)

// Site administrator capabilities, held only through the go-common site
// Admin user tier and never through an organization role.
const (
	// CapabilityServerAdmin changes deployment settings and appearance.
	CapabilityServerAdmin Capability = "server.admin"
	// CapabilityUsersAdmin administers user accounts across the site.
	CapabilityUsersAdmin Capability = "users.admin"
	// CapabilityOrgsAdmin administers every organization on the site.
	CapabilityOrgsAdmin Capability = "orgs.admin"
)

// allCapabilities is the canonical order: role tiers ascending, then the
// site-only capabilities. Bit positions in CapabilitySet follow this order.
var allCapabilities = [...]Capability{
	CapabilitySearch,
	CapabilityObjectsOwn,
	CapabilityKnowledgeWrite,
	CapabilityLookupsWrite,
	CapabilityAlertsWrite,
	CapabilityReportsWrite,
	CapabilityIndexesAdmin,
	CapabilityIngestionAdmin,
	CapabilityCollectorsAdmin,
	CapabilityHECAdmin,
	CapabilityAppsAdmin,
	CapabilityAuditRead,
	CapabilityKnowledgeAdmin,
	CapabilityMembersAdmin,
	CapabilityOrgAdmin,
	CapabilityServerAdmin,
	CapabilityUsersAdmin,
	CapabilityOrgsAdmin,
}

// AllCapabilities returns every capability in canonical order. The result is
// a fresh slice the caller may modify.
func AllCapabilities() []Capability {
	result := make([]Capability, len(allCapabilities))
	copy(result, allCapabilities[:])
	return result
}

// Valid reports whether capability is one of the canonical ids.
func (capability Capability) Valid() bool {
	_, ok := capability.bit()
	return ok
}

func (capability Capability) bit() (uint32, bool) {
	for index, candidate := range allCapabilities {
		if candidate == capability {
			return 1 << uint(index), true
		}
	}
	return 0, false
}

// ParseCapability returns the capability whose canonical id is value.
func ParseCapability(value string) (Capability, bool) {
	capability := Capability(value)
	if !capability.Valid() {
		return "", false
	}
	return capability, true
}
