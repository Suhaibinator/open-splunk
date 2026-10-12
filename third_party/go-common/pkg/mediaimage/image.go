// Package mediaimage prepares bounded, static raster images for model input.
// It performs no I/O, authorization, provider routing, or generative edits.
package mediaimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math"
	"mime"
	"strings"

	"golang.org/x/image/webp"
)

var (
	ErrInvalidOptions   = errors.New("invalid image preparation options")
	ErrInvalidCrop      = errors.New("invalid normalized image crop")
	ErrInvalidImage     = errors.New("invalid image")
	ErrUnsupportedImage = errors.New("unsupported or animated image")
	ErrLimitExceeded    = errors.New("image limit exceeded")
)

// NormalizedCrop is a rectangle on the original image, with a top-left origin.
// X increases rightward and Y downward. Right and bottom must not exceed one.
type NormalizedCrop struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// Validate rejects empty, nonfinite, or out-of-image normalized rectangles.
func (c NormalizedCrop) Validate() error {
	for _, v := range []float64{c.X, c.Y, c.Width, c.Height} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w: coordinates must be finite", ErrInvalidCrop)
		}
	}
	if c.X < 0 || c.Y < 0 || c.Width <= 0 || c.Height <= 0 || c.X+c.Width > 1 || c.Y+c.Height > 1 {
		return fmt.Errorf("%w: require a nonempty rectangle inside [0,1]", ErrInvalidCrop)
	}
	return nil
}

// PrepareOptions supplies encoded-byte and decoded-pixel limits, not a total
// process-memory budget. All three limits must be positive. ExpectedContentType, when nonempty, must match the decoded format.
type PrepareOptions struct {
	Crop                *NormalizedCrop
	ExpectedContentType string
	MaxInputBytes       int64
	MaxOutputBytes      int64
	MaxPixels           int64
}

// PreparedImage owns its bytes. Treat it and its Data as immutable after Prepare.
// SourceSHA256 always identifies the original input, not the cropped PNG.
// PixelBounds are original-image coordinates [left,top,right,bottom], with
// exclusive right/bottom edges. Data is deliberately excluded from JSON.
type PreparedImage struct {
	Data           []byte `json:"-"`
	ContentType    string `json:"content_type"`
	OriginalWidth  int    `json:"original_width"`
	OriginalHeight int    `json:"original_height"`
	PixelBounds    [4]int `json:"pixel_bounds"`
	SourceSHA256   string `json:"source_sha256"`
}

// Validate checks the structural contract for adapters. It does not decode again
// or confer trust on caller-constructed values; use Prepare for untrusted bytes.
func (p *PreparedImage) Validate() error {
	if p == nil || len(p.Data) == 0 || p.OriginalWidth <= 0 || p.OriginalHeight <= 0 {
		return ErrInvalidImage
	}
	switch p.ContentType {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
	default:
		return ErrUnsupportedImage
	}
	b := p.PixelBounds
	if b[0] < 0 || b[1] < 0 || b[2] <= b[0] || b[3] <= b[1] || b[2] > p.OriginalWidth || b[3] > p.OriginalHeight {
		return ErrInvalidImage
	}
	return nil
}

// Prepare validates a static PNG, JPEG, GIF, or WebP, then optionally crops it.
// Decoders are selected explicitly so other packages cannot widen the allowlist.
// Full images retain their original encoding; crops are lossless PNG pixels.
func Prepare(data []byte, o PrepareOptions) (*PreparedImage, error) {
	if o.MaxInputBytes <= 0 || o.MaxOutputBytes <= 0 || o.MaxPixels <= 0 {
		return nil, ErrInvalidOptions
	}
	if int64(len(data)) > o.MaxInputBytes {
		return nil, fmt.Errorf("%w: input bytes", ErrLimitExceeded)
	}
	if len(data) == 0 {
		return nil, ErrInvalidImage
	}
	if o.Crop != nil {
		if err := o.Crop.Validate(); err != nil {
			return nil, err
		}
	}
	var config func(*bytes.Reader) (image.Config, error)
	var decode func(*bytes.Reader) (image.Image, error)
	var contentType string
	switch {
	case bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")):
		contentType = "image/png"
		if err := inspectPNG(data); err != nil {
			return nil, err
		}
		config = func(r *bytes.Reader) (image.Config, error) { return png.DecodeConfig(r) }
		decode = func(r *bytes.Reader) (image.Image, error) { return png.Decode(r) }
	case len(data) >= 2 && data[0] == 0xff && data[1] == 0xd8:
		contentType = "image/jpeg"
		config = func(r *bytes.Reader) (image.Config, error) { return jpeg.DecodeConfig(r) }
		decode = func(r *bytes.Reader) (image.Image, error) { return jpeg.Decode(r) }
	case bytes.HasPrefix(data, []byte("GIF87a")) || bytes.HasPrefix(data, []byte("GIF89a")):
		contentType = "image/gif"
		if err := inspectGIF(data); err != nil {
			return nil, err
		}
		config = func(r *bytes.Reader) (image.Config, error) { return gif.DecodeConfig(r) }
		decode = func(r *bytes.Reader) (image.Image, error) { return decodeStaticGIF(r) }
	case len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP":
		contentType = "image/webp"
		if err := inspectWebP(data); err != nil {
			return nil, err
		}
		config = func(r *bytes.Reader) (image.Config, error) { return webp.DecodeConfig(r) }
		decode = func(r *bytes.Reader) (image.Image, error) { return webp.Decode(r) }
	default:
		return nil, ErrUnsupportedImage
	}
	if o.ExpectedContentType != "" {
		expected, _, err := mime.ParseMediaType(strings.TrimSpace(o.ExpectedContentType))
		if err != nil || strings.ToLower(expected) != contentType {
			return nil, fmt.Errorf("%w: content type does not match %s", ErrInvalidImage, contentType)
		}
	}
	cfg, err := config(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: header: %w", ErrInvalidImage, err)
	}
	if cfg.Width <= 0 || cfg.Height <= 0 {
		return nil, ErrInvalidImage
	}
	if int64(cfg.Width) > o.MaxPixels/int64(cfg.Height) {
		return nil, fmt.Errorf("%w: pixels", ErrLimitExceeded)
	}
	if o.Crop == nil && int64(len(data)) > o.MaxOutputBytes {
		return nil, fmt.Errorf("%w: output bytes", ErrLimitExceeded)
	}
	source, err := decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: decode: %w", ErrInvalidImage, err)
	}
	if source.Bounds().Dx() != cfg.Width || source.Bounds().Dy() != cfg.Height {
		return nil, fmt.Errorf("%w: inconsistent dimensions", ErrInvalidImage)
	}
	bounds := image.Rect(0, 0, cfg.Width, cfg.Height)
	if c := o.Crop; c != nil {
		bounds = image.Rect(int(math.Floor(c.X*float64(cfg.Width))), int(math.Floor(c.Y*float64(cfg.Height))), int(math.Ceil((c.X+c.Width)*float64(cfg.Width))), int(math.Ceil((c.Y+c.Height)*float64(cfg.Height))))
		if bounds.Empty() || !bounds.In(image.Rect(0, 0, cfg.Width, cfg.Height)) {
			return nil, ErrInvalidCrop
		}
	}
	out := &PreparedImage{ContentType: contentType, OriginalWidth: cfg.Width, OriginalHeight: cfg.Height, PixelBounds: [4]int{bounds.Min.X, bounds.Min.Y, bounds.Max.X, bounds.Max.Y}, SourceSHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	if o.Crop == nil {
		out.Data = bytes.Clone(data)
		return out, nil
	}
	// Preserve the native samples for 16-bit PNGs, including unpremultiplied
	// alpha. Drawing them into NRGBA would silently discard the low eight bits.
	// png.Encode handles the nonzero origin of these subimages directly.
	var clipped image.Image
	sourceBounds := bounds.Add(source.Bounds().Min)
	switch src := source.(type) {
	case *image.NRGBA64:
		clipped = src.SubImage(sourceBounds)
	case *image.RGBA64:
		clipped = src.SubImage(sourceBounds)
	case *image.Gray16:
		clipped = src.SubImage(sourceBounds)
	default:
		canvas := image.NewNRGBA(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
		draw.Draw(canvas, canvas.Bounds(), source, sourceBounds.Min, draw.Src)
		clipped = canvas
	}
	writer := &boundedBuffer{max: o.MaxOutputBytes}
	if err := png.Encode(writer, clipped); err != nil {
		return nil, fmt.Errorf("encode crop: %w", err)
	}
	out.Data = writer.Bytes()
	out.ContentType = "image/png"
	return out, nil
}

type boundedBuffer struct {
	bytes.Buffer
	max int64
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > w.max-int64(w.Len()) {
		return 0, fmt.Errorf("%w: encoded output bytes", ErrLimitExceeded)
	}
	return w.Buffer.Write(p)
}

// EncodeDataURL encodes prepared bytes once, using their canonical content type.
func EncodeDataURL(p *PreparedImage) (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	var output strings.Builder
	output.Grow(len("data:;base64,") + len(p.ContentType) + base64.StdEncoding.EncodedLen(len(p.Data)))
	output.WriteString("data:")
	output.WriteString(p.ContentType)
	output.WriteString(";base64,")
	encoder := base64.NewEncoder(base64.StdEncoding, &output)
	if _, err := encoder.Write(p.Data); err != nil {
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return output.String(), nil
}

// decodeStaticGIF is called only after inspectGIF proves there is exactly one
// frame. DecodeAll supplies the global background metadata; it cannot allocate
// an unbounded sequence of frames here. Crop coordinates refer to the logical
// canvas, not the (possibly offset) frame. A transparent frame uses a transparent
// canvas; otherwise unpainted pixels use the global background color.
func decodeStaticGIF(r *bytes.Reader) (image.Image, error) {
	g, err := gif.DecodeAll(r)
	if err != nil {
		return nil, err
	}
	if len(g.Image) != 1 {
		return nil, ErrUnsupportedImage
	}
	frame := g.Image[0]
	bounds := image.Rect(0, 0, g.Config.Width, g.Config.Height)
	if !frame.Bounds().In(bounds) {
		return nil, ErrInvalidImage
	}
	if frame.Bounds() == bounds {
		return frame, nil
	}
	canvas := image.NewNRGBA(bounds)
	transparent := false
	for _, c := range frame.Palette {
		_, _, _, a := c.RGBA()
		if a == 0 {
			transparent = true
			break
		}
	}
	if !transparent {
		if palette, ok := g.Config.ColorModel.(color.Palette); ok && int(g.BackgroundIndex) < len(palette) {
			draw.Draw(canvas, bounds, image.NewUniform(palette[g.BackgroundIndex]), image.Point{}, draw.Src)
		}
	}
	draw.Draw(canvas, frame.Bounds(), frame, frame.Bounds().Min, draw.Over)
	return canvas, nil
}
