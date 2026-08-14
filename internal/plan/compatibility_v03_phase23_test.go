package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestV03MissingMultivalueFieldExtendsClosedOutputSchema(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"makemv missing", "mvexpand missing"} {
		command := command
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			parsed, err := spl.Parse(`index=gradethis | table event_id | ` + command)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if want := []string{"event_id", "missing"}; !slices.Equal(logical.OutputFields, want) {
				t.Fatalf("OutputFields = %v, want %v", logical.OutputFields, want)
			}
		})
	}
}
