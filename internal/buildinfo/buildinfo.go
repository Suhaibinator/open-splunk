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
	productVersionPattern   = regexp.MustCompile(`^0\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$`)
	productVersion          string
	revisionBuildIDReplacer = strings.NewReplacer(
		"a", "g",
		"b", "h",
		"c", "j",
		"d", "k",
		"e", "m",
		"f", "n",
	)
)

// Identity names one exact source tree and its optional v0 product release.
type Identity struct {
	SourceRevision string
	ProductVersion string
}

// Parse validates an identity without normalizing it. Build inputs must already
// be canonical so the same bytes cross language and process boundaries.
func Parse(revision string) (Identity, error) {
	return ParseRelease(revision, "")
}

// ParseRelease validates the source identity and optional canonical v0
// product version embedded only by release builds.
func ParseRelease(revision, version string) (Identity, error) {
	if revision != DevelopmentRevision && !sourceRevisionPattern.MatchString(revision) {
		return Identity{}, fmt.Errorf(
			"source revision %q must be %s or a full lowercase Git hash",
			revision,
			DevelopmentRevision,
		)
	}
	if version != "" && !productVersionPattern.MatchString(version) {
		return Identity{}, fmt.Errorf(
			"product version %q must be empty or canonical 0.MINOR.PATCH",
			version,
		)
	}
	if revision == DevelopmentRevision && version != "" {
		return Identity{}, errors.New("development builds must not carry a product version")
	}
	return Identity{SourceRevision: revision, ProductVersion: version}, nil
}

// Current returns the identity compiled into the calling binary.
func Current() (Identity, error) {
	identity, err := ParseRelease(sourceRevision, productVersion)
	if err != nil {
		return Identity{}, fmt.Errorf("load compiled build identity: %w", err)
	}
	return identity, nil
}

// UIBuildID derives a collision-resistant Next.js build ID from the exact
// source identity. Product releases of identical source intentionally retain
// identical UI bytes. Hex letters are translated to an alphabet that cannot
// contain "ad", which Next itself avoids because ad blockers can reject
// matching static paths.
func (identity Identity) UIBuildID() (string, error) {
	validated, err := ParseRelease(identity.SourceRevision, identity.ProductVersion)
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
	_, err := ParseRelease(identity.SourceRevision, identity.ProductVersion)
	return err == nil && identity.SourceRevision != DevelopmentRevision
}

// WriteIdentity emits the machine-readable identity shared by binaries built
// from the same source tree.
func WriteIdentity(output io.Writer, identity Identity) error {
	if output == nil {
		return errors.New("build identity output is required")
	}
	validated, err := ParseRelease(identity.SourceRevision, identity.ProductVersion)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		output,
		"source_revision=%s\n",
		validated.SourceRevision,
	)
	if err != nil || validated.ProductVersion == "" {
		return err
	}
	_, err = fmt.Fprintf(output, "product_version=%s\n", validated.ProductVersion)
	return err
}

// ValidSHA256 reports whether value is one canonical lowercase SHA-256 hex
// digest.
func ValidSHA256(value string) bool {
	return sha256hex.Valid(value)
}

// Equal reports exact cross-component identity equality.
func (identity Identity) Equal(other Identity) bool {
	return identity.SourceRevision == other.SourceRevision &&
		identity.ProductVersion == other.ProductVersion
}

// ValidatePublishable refuses the development sentinel.
func (identity Identity) ValidatePublishable() error {
	if _, err := ParseRelease(identity.SourceRevision, identity.ProductVersion); err != nil {
		return err
	}
	if !identity.Publishable() {
		return errors.New("source revision development is not valid for an immutable artifact")
	}
	return nil
}
