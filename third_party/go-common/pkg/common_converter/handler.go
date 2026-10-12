package common_converter

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const loggerName = "common_converter"

type publicIdentifierTypes struct {
	primary   proto_models.UserIdentifierType
	secondary proto_models.UserIdentifierType
}

// ConverterHandler owns converter package dependencies used in the backend conversion flow between persistence, service, and transport models.
// It is instantiated by service wiring and then used to translate entities across backend layer boundaries.
type ConverterHandler[U common_service_models.UserLike] struct {
	publicBucket      string
	publicBaseURL     func() string
	publicIdentifiers func() publicIdentifierTypes
	// globalPermissions returns the compiled attribute visibility policy for
	// one conversion. It samples the provider once per call; the compiled
	// table is memoized per configuration generation.
	globalPermissions func() config.UserTypeAttributePermissions
	logger            *zap.Logger
}

// NewConverterHandler constructs the converter from one configuration source.
// Bucket identity is pinned at construction; public origins, identifiers and
// compiled attribute visibility policies remain hot.
func NewConverterHandler[U common_service_models.UserLike](cfg config.ConverterProvider, logger *zap.Logger) *ConverterHandler[U] {
	if logger == nil {
		logger = zap.NewNop()
	}
	startup := cfg()
	identifiers := publicIdentifierTypes{
		primary:   startup.PublicPrimaryUserIdentifier(),
		secondary: startup.PublicSecondaryUserIdentifier(),
	}
	attributePolicies := config.NewGenerationMemo(
		func(view config.ConverterView) (config.UserTypeAttributePermissions, error) {
			return config.CompileUserTypeAttributePermissions(view.GlobalUserAttributePermissions()), nil
		},
		func(err error) {
			logger.Named(loggerName).Warn("attribute permission policy unavailable; keeping last valid policy",
				zap.String(logkeys.Stage, "compile_converter_attribute_permission_policy"),
				zap.String(logkeys.Fallback, "last_valid_policy"),
				zap.Error(err))
		},
	)
	attributePolicies.Get(startup)
	return &ConverterHandler[U]{
		publicBucket:  startup.MinioPublicBucket(),
		publicBaseURL: func() string { return cfg().MinioPublicBaseURL() },
		publicIdentifiers: func() publicIdentifierTypes {
			view := cfg()
			if view == nil {
				appLogger.LogInvariantViolation(
					appLogger.For(context.Background(), logger, loggerName),
					"converter_public_identifier_policy_present",
					errors.New("public identifier configuration provider returned nil"),
					zap.String(logkeys.Stage, "resolve_public_identifier_types"),
					zap.String(logkeys.Expected, "non-nil public identifier policy"),
					zap.String(logkeys.Actual, "nil public identifier policy"),
					zap.String(logkeys.Fallback, "startup_converter_config"),
				)
				return identifiers
			}
			return publicIdentifierTypes{primary: view.PublicPrimaryUserIdentifier(), secondary: view.PublicSecondaryUserIdentifier()}
		},
		globalPermissions: attributePolicies.Provider(cfg),
		logger:            logger,
	}
}

func (h *ConverterHandler[U]) publicIdentifierTypes() publicIdentifierTypes {
	return h.publicIdentifiers()
}
