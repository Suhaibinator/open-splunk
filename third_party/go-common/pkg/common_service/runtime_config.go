package common_service

import (
	phoneauth "github.com/Suhaibinator/go-common/pkg/common_service/auth/phone"
	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	"github.com/Suhaibinator/go-common/pkg/config"
)

var (
	_ phoneauth.RuntimeConfigView = (config.SmsView)(nil)
	_ botpkg.RuntimeConfigView    = (config.ServiceView)(nil)
)

// runtimeView samples the service policy once at an operation boundary.
func (h *CommonServiceHandler[U]) runtimeView() config.ServiceView {
	if h.runtimeConfig == nil {
		return nil
	}
	return h.runtimeConfig()
}
