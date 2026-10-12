package controlplane

import "github.com/Suhaibinator/go-common/pkg/common_persistence"

// Compile-time proof that the narrow persistence store implements the control-plane store seams the
// registry, resolver, and recorder depend on. This keeps the store and the interfaces from drifting
// without either package importing the other's concrete types at runtime.
var (
	_ RegistryStore = (*common_persistence.ControlPlaneStore)(nil)
	_ AttemptStore  = (*common_persistence.ControlPlaneStore)(nil)
)
