package collector

import (
	"errors"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// decodeNative is isolated from the established NDJSON decoding path.
func (d *Decoder) decodeNative(event *opensplunk.LogEvent, raw []byte) (*opensplunk.LogEvent, error) {
	var err error
	switch d.cfg.Format {
	case "docker-json-file":
		err = d.decodeDocker(event, raw)
	case "nginx-combined", "apache-common", "apache-combined":
		err = d.decodeAccess(event, raw)
	case "logfmt":
		err = d.decodeLogfmt(event, raw)
	case "log4j2-pattern", "logback-pattern":
		err = d.decodePattern(event, raw)
	default:
		err = errors.New("unsupported native parser")
	}
	if err != nil {
		return nil, err
	}
	if len(d.constants) != 0 {
		event.Fields = d.mergeConstants(event.Fields.GetFields())
	}
	budget := nativeFieldBudget{fields: min(d.cfg.MaxJSONFields, eventfields.MaximumStoredFieldsPerEvent), names: eventfields.MaximumStoredFieldNamesBytes, depth: min(d.cfg.MaxJSONDepth, eventfields.MaximumDynamicPathSegments)}
	if err := budget.object(event.Fields, 1, 0); err != nil {
		return nil, err
	}
	return event, nil
}
