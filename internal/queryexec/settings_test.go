package queryexec

import (
	"maps"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func TestNewValidatedExecutorSettingsRejectsMalformedOrUnsafeSettings(t *testing.T) {
	base, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	type testCase struct {
		name   string
		mutate func(clickhousedriver.Settings) clickhousedriver.Settings
	}
	tests := []testCase{
		{name: "nil", mutate: func(clickhousedriver.Settings) clickhousedriver.Settings { return nil }},
		{name: "readonly missing", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			delete(settings, "readonly")
			return settings
		}},
		{name: "readonly unsafe", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings["readonly"] = uint8(1)
			return settings
		}},
		{name: "readonly wrong type", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings["readonly"] = "2"
			return settings
		}},
	}
	for _, name := range requiredPositiveExecutorSettingNames {
		tests = append(tests,
			testCase{name: name + " missing", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
				delete(settings, name)
				return settings
			}},
			testCase{name: name + " zero", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
				settings[name] = uint64(0)
				return settings
			}},
			testCase{name: name + " wrong type", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
				settings[name] = "1"
				return settings
			}},
		)
	}
	for _, name := range requiredThrowExecutorSettingNames {
		tests = append(tests, testCase{name: name + " unsafe", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings[name] = "break"
			return settings
		}})
	}
	for _, name := range requiredTextIndexSettingNames {
		tests = append(tests, testCase{name: name + " disabled", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings[name] = uint8(0)
			return settings
		}})
	}
	tests = append(tests,
		testCase{name: "materialized CTE disabled", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings["enable_materialized_cte"] = uint8(0)
			return settings
		}},
		testCase{name: "short circuit disabled", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings["short_circuit_function_evaluation"] = "disable"
			return settings
		}},
		testCase{name: "async insert enabled", mutate: func(settings clickhousedriver.Settings) clickhousedriver.Settings {
			settings["async_insert"] = uint8(1)
			return settings
		}},
	)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := test.mutate(maps.Clone(base))
			validated, err := newValidatedExecutorSettings(settings)
			if validated != nil || err == nil {
				t.Fatalf("newValidatedExecutorSettings() = (%#v, %v), want nil and error", validated, err)
			}
		})
	}
}

func mustValidatedSettings(
	t testing.TB,
	settings clickhousedriver.Settings,
) *validatedExecutorSettings {
	t.Helper()
	validated, err := newValidatedExecutorSettings(settings)
	if err != nil {
		t.Fatalf("validate executor settings: %v", err)
	}
	return validated
}
