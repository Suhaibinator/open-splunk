// Package uipalette defines the instance-wide UI palette an administrator
// selects for every browser session, including the sign-in page.
package uipalette

import "fmt"

// Palette names one shipped aesthetic. Values are the exact lower-case
// identifiers persisted in server_appearance_settings and carried by the
// UiPalette proto enum; the client maps them to data-palette on <html>.
type Palette string

const (
	// Classic is the original Splunk-style light/dark pair.
	Classic Palette = "classic"
	// Ocean is a cool blue accent with slate-blue chrome bars.
	Ocean Palette = "ocean"
	// Ember is warm neutrals with an orange-red accent.
	Ember Palette = "ember"
	// Graphite is near-monochrome, high contrast, minimal colour.
	Graphite Palette = "graphite"
	// Glass is translucent raised surfaces with softer radii and shadows.
	Glass Palette = "glass"
	// Terminal is a monospace UI with near-square corners and flat surfaces.
	Terminal Palette = "terminal"
)

var all = []Palette{Classic, Ocean, Ember, Graphite, Glass, Terminal}

// Default is the palette an instance shows before an administrator chooses.
func Default() Palette { return Classic }

// All lists every supported palette in presentation order.
func All() []Palette {
	result := make([]Palette, len(all))
	copy(result, all)
	return result
}

// Validate accepts exactly the supported identifiers; matching is
// case-sensitive and the empty string is rejected.
func Validate(palette Palette) error {
	for _, candidate := range all {
		if palette == candidate {
			return nil
		}
	}
	return fmt.Errorf("ui palette: %q is not a supported palette", string(palette))
}
