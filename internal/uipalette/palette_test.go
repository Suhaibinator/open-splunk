package uipalette

import (
	"strings"
	"testing"
)

func TestDefaultIsClassicAndValid(t *testing.T) {
	t.Parallel()
	if got := Default(); got != Classic {
		t.Fatalf("Default() = %q, want %q", got, Classic)
	}
	if err := Validate(Default()); err != nil {
		t.Fatalf("Validate(Default()) = %v", err)
	}
}

func TestAllListsEveryPaletteInPresentationOrder(t *testing.T) {
	t.Parallel()
	want := []Palette{Classic, Ocean, Ember, Graphite, Glass, Terminal}
	got := All()
	if len(got) != len(want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("All()[%d] = %q, want %q", index, got[index], want[index])
		}
	}
	got[0] = "mutated"
	if All()[0] != Classic {
		t.Fatal("All() returned its backing slice")
	}
}

func TestValidateAcceptsEverySupportedPalette(t *testing.T) {
	t.Parallel()
	for _, palette := range All() {
		if err := Validate(palette); err != nil {
			t.Fatalf("Validate(%q) = %v", palette, err)
		}
	}
}

func TestValidateRejectsUnknownEmptyAndMiscasedPalettes(t *testing.T) {
	t.Parallel()
	for _, palette := range []Palette{"", "Classic", "sepia", " classic", "classic "} {
		err := Validate(palette)
		if err == nil {
			t.Fatalf("Validate(%q) succeeded", palette)
		}
		if !strings.Contains(err.Error(), "ui palette") {
			t.Fatalf("Validate(%q) error = %v, want a ui palette error", palette, err)
		}
	}
}
