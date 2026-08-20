// Package buildinfo owns the immutable identity shared by release binaries,
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
	DefaultApplicationVersion  = "0.4.0"
	AssetManifestFormatVersion = uint32(1)
)

var (
	// applicationVersion and sourceRevision are set only by the supported
	// release build's -ldflags. Development builds deliberately retain the
	// explicit sentinel so they cannot be mistaken for release artifacts.
	applicationVersion = DefaultApplicationVersion
	sourceRevision     = DevelopmentRevision

	applicationVersionPattern = regexp.MustCompile(
		`^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`,
	)
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

// Identity binds a semantic application version to one exact source tree.
type Identity struct {
	ApplicationVersion string
	SourceRevision     string
}

// Parse validates an identity without normalizing it. Build inputs must already
// be canonical so the same bytes cross language and process boundaries.
func Parse(version, revision string) (Identity, error) {
	if len(version) == 0 || len(version) > 64 || !applicationVersionPattern.MatchString(version) {
		return Identity{}, fmt.Errorf("application version %q is invalid", version)
	}
	if revision != DevelopmentRevision && !sourceRevisionPattern.MatchString(revision) {
		return Identity{}, fmt.Errorf(
			"source revision %q must be %s or a full lowercase Git hash",
			revision,
			DevelopmentRevision,
		)
	}
	return Identity{ApplicationVersion: version, SourceRevision: revision}, nil
}

// Current returns the identity compiled into the calling binary.
func Current() (Identity, error) {
	identity, err := Parse(applicationVersion, sourceRevision)
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
	validated, err := Parse(identity.ApplicationVersion, identity.SourceRevision)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk-ui-build-v1\x00"))
	_, _ = hash.Write([]byte(validated.ApplicationVersion))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(validated.SourceRevision))
	return "r" + revisionBuildIDReplacer.Replace(hex.EncodeToString(hash.Sum(nil))), nil
}

// Release reports whether the identity is suitable for a published artifact.
func (identity Identity) Release() bool {
	_, err := Parse(identity.ApplicationVersion, identity.SourceRevision)
	return err == nil && identity.SourceRevision != DevelopmentRevision
}

// DisplayVersion is the unambiguous value used on legacy protocol fields that
// cannot yet carry structured build metadata.
func (identity Identity) DisplayVersion() string {
	return identity.ApplicationVersion + " (" + identity.SourceRevision + ")"
}

// WriteIdentity emits the stable machine-readable identity prefix shared by
// every supported release binary.
func WriteIdentity(output io.Writer, identity Identity) error {
	if output == nil {
		return errors.New("build identity output is required")
	}
	validated, err := Parse(identity.ApplicationVersion, identity.SourceRevision)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		output,
		"application_version=%s\nsource_revision=%s\n",
		validated.ApplicationVersion,
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
	return identity.ApplicationVersion == other.ApplicationVersion &&
		identity.SourceRevision == other.SourceRevision
}

// ValidateRelease refuses the development sentinel.
func (identity Identity) ValidateRelease() error {
	if _, err := Parse(identity.ApplicationVersion, identity.SourceRevision); err != nil {
		return err
	}
	if !identity.Release() {
		return errors.New("source revision development is not valid for a release")
	}
	return nil
}
