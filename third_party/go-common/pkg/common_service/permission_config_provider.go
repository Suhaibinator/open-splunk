package common_service

import (
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/config"
	"go.uber.org/zap"
)

// newCachingPermissionConfigProvider projects the live service provider into
// one immutable permission policy per configuration generation through
// config.GenerationMemo. The provider is sampled exactly once while resolving
// each permission decision, and that one view supplies both permission and
// organization thresholds.
//
// A config.RootView identifies its immutable generation, so an unchanged
// generation reuses its policy; any other view is projected per decision. A
// nil or zero RootView, a projection error, or a panicking view rejects the
// whole candidate, and the last valid policy remains active until a new
// generation is published.
func newCachingPermissionConfigProvider(
	cfg config.ServiceProvider,
	startupView config.ServiceView,
	startupPolicy *permission.PermissionConfig,
	logger *zap.Logger,
) permission.ConfigProvider {
	policies := config.NewGenerationMemo(projectPermissionPolicy, func(err error) {
		logPermissionProjectionFailure(logger, err)
	})
	policies.Seed(startupView, startupPolicy)
	return policies.Provider(cfg)
}

// projectPermissionPolicy projects one sampled view into a permission policy;
// any failure rejects the whole candidate generation.
func projectPermissionPolicy(view config.ServiceView) (*permission.PermissionConfig, error) {
	return permission.NewPermissionConfigFromViews(view, view)
}

func logPermissionProjectionFailure(logger *zap.Logger, err error) {
	if logger != nil {
		logger.Named(loggerName).Warn("permission policy projection failed; keeping last valid policy", zap.Error(err))
	}
}
