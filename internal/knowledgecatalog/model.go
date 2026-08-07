package knowledgecatalog

type registryRecord struct {
	TenantID               string       `gorm:"column:tenant_id"`
	KnowledgeObjectID      string       `gorm:"column:knowledge_object_id"`
	CurrentVersion         int64        `gorm:"column:current_version"`
	AppID                  string       `gorm:"column:app_id"`
	OwnerID                string       `gorm:"column:owner_id"`
	ObjectType             ObjectType   `gorm:"column:object_type"`
	Name                   string       `gorm:"column:name"`
	SharingScope           SharingScope `gorm:"column:sharing_scope"`
	State                  State        `gorm:"column:state"`
	DefinitionDigest       []byte       `gorm:"column:definition_digest"`
	CreatedAtUnixMicro     int64        `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro     int64        `gorm:"column:updated_at_unix_micro"`
	DisabledAtUnixMicro    *int64       `gorm:"column:disabled_at_unix_micro"`
	QuarantinedAtUnixMicro *int64       `gorm:"column:quarantined_at_unix_micro"`
	DeletedAtUnixMicro     *int64       `gorm:"column:deleted_at_unix_micro"`
	QuarantineReason       *string      `gorm:"column:quarantine_reason"`
}

func (registryRecord) TableName() string { return "knowledge_objects" }

type versionRecord struct {
	TenantID           string       `gorm:"column:tenant_id"`
	KnowledgeObjectID  string       `gorm:"column:knowledge_object_id"`
	ObjectVersion      int64        `gorm:"column:object_version"`
	AppID              string       `gorm:"column:app_id"`
	OwnerID            string       `gorm:"column:owner_id"`
	ObjectType         ObjectType   `gorm:"column:object_type"`
	Name               string       `gorm:"column:name"`
	SharingScope       SharingScope `gorm:"column:sharing_scope"`
	State              State        `gorm:"column:state"`
	DefinitionDigest   []byte       `gorm:"column:definition_digest"`
	DependencyCount    int64        `gorm:"column:dependency_count"`
	MutationKind       string       `gorm:"column:mutation_kind"`
	QuarantineReason   *string      `gorm:"column:quarantine_reason"`
	CreatedAtUnixMicro int64        `gorm:"column:created_at_unix_micro"`
}

func (versionRecord) TableName() string { return "knowledge_object_versions" }

type definitionBlobRecord struct {
	TenantID           string `gorm:"column:tenant_id"`
	DefinitionDigest   []byte `gorm:"column:definition_digest"`
	DefinitionProto    []byte `gorm:"column:definition_proto"`
	DefinitionBytes    int64  `gorm:"column:definition_bytes"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro"`
}

func (definitionBlobRecord) TableName() string { return "knowledge_definition_blobs" }

type projectionRecord struct {
	TenantID                   string       `gorm:"column:tenant_id"`
	KnowledgeObjectID          string       `gorm:"column:knowledge_object_id"`
	ObjectVersion              int64        `gorm:"column:object_version"`
	AppID                      string       `gorm:"column:app_id"`
	OwnerID                    string       `gorm:"column:owner_id"`
	ObjectType                 ObjectType   `gorm:"column:object_type"`
	Name                       string       `gorm:"column:name"`
	SharingScope               SharingScope `gorm:"column:sharing_scope"`
	State                      State        `gorm:"column:state"`
	DescriptionPresent         int64        `gorm:"column:description_present"`
	Description                string       `gorm:"column:description"`
	IndexSelectorCount         int64        `gorm:"column:index_selector_count"`
	HostSelectorCount          int64        `gorm:"column:host_selector_count"`
	SourceSelectorCount        int64        `gorm:"column:source_selector_count"`
	SourcetypeSelectorCount    int64        `gorm:"column:sourcetype_selector_count"`
	SelectorValueBytes         int64        `gorm:"column:selector_value_bytes"`
	CanonicalSelectorBytes     int64        `gorm:"column:canonical_selector_bytes"`
	ProjectionBytes            int64        `gorm:"column:projection_bytes"`
	SealProjectionBytes        int64        `gorm:"column:seal_projection_bytes"`
	SealCanonicalSelectorBytes int64        `gorm:"column:seal_canonical_selector_bytes"`
	DefinitionDigest           []byte       `gorm:"column:definition_digest"`
	DefinitionBytes            int64        `gorm:"column:definition_bytes"`
	DependencyCount            int64        `gorm:"column:dependency_count"`
	CreatedAtUnixMicro         int64        `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro         int64        `gorm:"column:updated_at_unix_micro"`
	DisabledAtUnixMicro        *int64       `gorm:"column:disabled_at_unix_micro"`
	QuarantinedAtUnixMicro     *int64       `gorm:"column:quarantined_at_unix_micro"`
	DeletedAtUnixMicro         *int64       `gorm:"column:deleted_at_unix_micro"`
	QuarantineReason           *string      `gorm:"column:quarantine_reason"`
}

func (projectionRecord) TableName() string { return "knowledge_object_list_projections" }

type selectorRecord struct {
	Dimension  string `gorm:"column:dimension"`
	Ordinal    int64  `gorm:"column:ordinal"`
	MatchKind  string `gorm:"column:match_kind"`
	Value      string `gorm:"column:value"`
	ValueBytes int64  `gorm:"column:value_bytes"`
}

// tenantRecord is reserved for administrator health diagnostics and mutation
// accounting. Caller-facing reads establish coherence through catalogState and
// never use these physical ledgers as an authorization or visibility oracle.
type tenantRecord struct {
	TenantID        string `gorm:"column:tenant_id"`
	CatalogRevision int64  `gorm:"column:catalog_revision"`
	IdentityCount   int64  `gorm:"column:identity_count"`
}

type dependencySealRecord struct {
	TenantID          string `gorm:"column:tenant_id"`
	KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
	ObjectVersion     int64  `gorm:"column:object_version"`
	DependencyCount   int64  `gorm:"column:dependency_count"`
}

func (tenantRecord) TableName() string { return "knowledge_catalog_tenants" }

type dependencyRecord struct {
	TenantID            string `gorm:"column:tenant_id"`
	SourceObjectID      string `gorm:"column:source_object_id"`
	SourceObjectVersion int64  `gorm:"column:source_object_version"`
	Ordinal             int64  `gorm:"column:ordinal"`
	TargetKind          string `gorm:"column:target_kind"`
	TargetObjectID      string `gorm:"column:target_object_id"`
	TargetObjectVersion int64  `gorm:"column:target_object_version"`
	DependencyRole      string `gorm:"column:dependency_role"`
	TargetFound         int64  `gorm:"column:target_found"`
}
