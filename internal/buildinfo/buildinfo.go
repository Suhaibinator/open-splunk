// Package buildinfo owns the immutable identity shared by development binaries,
// embedded assets, and runtime protocol surfaces.
package buildinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/sha256hex"
)

const (
	DevelopmentRevision        = "development"
	AssetManifestFormatVersion = uint32(1)
)

var (
	// sourceRevision is set only by the supported build's -ldflags.
	// Development builds deliberately retain the explicit sentinel.
	sourceRevision = DevelopmentRevision

	sourceRevisionPattern   = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	revisionBuildIDReplacer = strings.NewReplacer(
		"a", "g",
		"b", "h",
		"c", "j",
		"d", "k",
		"e", "m",
		"f", "n",
	)
)

// Identity names one exact source tree without claiming a product version.
type Identity struct {
	SourceRevision string
}

// Parse validates an identity without normalizing it. Build inputs must already
// be canonical so the same bytes cross language and process boundaries.
func Parse(revision string) (Identity, error) {
	if revision != DevelopmentRevision && !sourceRevisionPattern.MatchString(revision) {
		return Identity{}, fmt.Errorf(
			"source revision %q must be %s or a full lowercase Git hash",
			revision,
			DevelopmentRevision,
		)
	}
	return Identity{SourceRevision: revision}, nil
}

// Current returns the identity compiled into the calling binary.
func Current() (Identity, error) {
	identity, err := Parse(sourceRevision)
	if err != nil {
		return Identity{}, fmt.Errorf("load compiled build identity: %w", err)
	}
	return identity, nil
}

// UIBuildID derives a collision-resistant Next.js build ID from the complete
// application identity. Hex letters are translated to an alphabet that cannot
// contain "ad", which Next itself avoids because ad blockers can reject
// matching static paths.
func (identity Identity) UIBuildID() (string, error) {
	validated, err := Parse(identity.SourceRevision)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk-ui-build\x00"))
	_, _ = hash.Write([]byte(validated.SourceRevision))
	return "r" + revisionBuildIDReplacer.Replace(hex.EncodeToString(hash.Sum(nil))), nil
}

// Publishable reports whether the identity is suitable for an immutable
// development artifact.
func (identity Identity) Publishable() bool {
	_, err := Parse(identity.SourceRevision)
	return err == nil && identity.SourceRevision != DevelopmentRevision
}

// WriteIdentity emits the machine-readable identity shared by development
// binaries built from the same source tree.
func WriteIdentity(output io.Writer, identity Identity) error {
	if output == nil {
		return errors.New("build identity output is required")
	}
	validated, err := Parse(identity.SourceRevision)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		output,
		"source_revision=%s\n",
		validated.SourceRevision,
	)
	return err
}

// ValidSHA256 reports whether value is one canonical lowercase SHA-256 hex
// digest.
func ValidSHA256(value string) bool {
	return sha256hex.Valid(value)
}

// Equal reports exact cross-component identity equality.
func (identity Identity) Equal(other Identity) bool {
	return identity.SourceRevision == other.SourceRevision
}

// ValidatePublishable refuses the development sentinel.
func (identity Identity) ValidatePublishable() error {
	if _, err := Parse(identity.SourceRevision); err != nil {
		return err
	}
	if !identity.Publishable() {
		return errors.New("source revision development is not valid for an immutable artifact")
	}
	return nil
}
