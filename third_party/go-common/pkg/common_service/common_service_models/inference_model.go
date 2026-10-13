package common_service_models

// InferenceModel defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type InferenceModel struct {
	ID              uint64
	ProviderID      string
	ProviderModelID string
	DisplayName     string
	Active          bool
}

// InferenceModelPatch defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type InferenceModelPatch struct {
	ID              uint64
	UpdateMask      []string
	ProviderID      *string
	ProviderModelID *string
	DisplayName     *string
	Active          *bool
}
