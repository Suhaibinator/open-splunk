// Package imagemedia adapts validated images to Genkit media parts.
package imagemedia

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/mediaimage"
	"github.com/firebase/genkit/go/ai"
)

// GenkitImagePart encodes a prepared image once as a data URL. It does not pass
// through a protobuf representation. The caller's name is preserved as filename
// metadata; keep application page identities alongside the prepared image.
func GenkitImagePart(name string, image *mediaimage.PreparedImage) (*ai.Part, error) {
	dataURL, err := mediaimage.EncodeDataURL(image)
	if err != nil {
		return nil, fmt.Errorf("Genkit image: %w", err)
	}
	part := ai.NewMediaPart(image.ContentType, dataURL)
	if name != "" {
		part.Metadata = map[string]any{"filename": name}
	}
	return part, nil
}
