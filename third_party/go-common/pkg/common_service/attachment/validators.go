package attachment

import (
	"bufio"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/config"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"slices"
	"strings"

	"github.com/h2non/filetype"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/webp"
)

// StreamingAttachmentValidator defines the interface for memory-efficient attachment validation.
// All validators implement this interface to enable streaming validation without loading
// entire files into memory, providing enterprise-grade support for large file processing.
type StreamingAttachmentValidator interface {
	// Validate performs validation on an attachment using streaming access.
	// The reader provides sequential access to the file content without loading it entirely.
	// Validators should read only the minimal data needed for validation.
	// Returns true if validation passes, along with optional details.
	Validate(ctx context.Context, reader io.ReadSeeker, size int64, contentType string) (bool, string)

	// Name returns a human-readable name for this validator, used in error messages and logging.
	Name() string
}

// SecurityConstraints defines security policies for file validation.
// These constraints are checked during streaming validation to prevent
// malicious file uploads while maintaining performance.
type SecurityConstraints struct {
	// DangerousTypes are file types that should never be allowed
	DangerousTypes []string

	// AllowedTextTypes are text-based types that are considered safe
	AllowedTextTypes []string

	// MaxFileSize is the maximum allowed file size in bytes
	MaxFileSize int64

	// RequireValidMimeType ensures content matches declared MIME type
	RequireValidMimeType bool
}

// DefaultSecurityConstraints returns the default security configuration
func DefaultSecurityConstraints() SecurityConstraints {
	return SecurityConstraints{
		DangerousTypes: []string{
			"application/x-executable",
			"application/x-msdownload",
			"application/x-msdos-program",
			"application/x-sh",
			"application/x-shellscript",
			"text/x-sh",
			"text/x-shellscript",
			"application/javascript",
			"text/javascript",
			"application/x-javascript",
			"text/html",     // HTML can contain scripts
			"image/svg+xml", // Active XML requires complete sanitization, not prefix scanning.
			"application/xhtml+xml",
		},
		AllowedTextTypes: []string{
			"text/plain",
			"text/csv",
			"text/markdown",
			"application/json",
			"application/xml",
			"text/xml",
		},
		MaxFileSize:          100 * 1024 * 1024, // 100MB default
		RequireValidMimeType: true,
	}
}

// StreamingSizeValidator validates file size without loading content.
// This validator uses only the size parameter and performs no I/O operations.
type StreamingSizeValidator struct {
	MaxSize int64
}

// NewStreamingSizeValidator constructs a StreamingSizeValidator used in upload validation pipelines to enforce a maximum byte size before deeper checks run.
//
// Parameters:
//   - maxSize: Maximum allowed file size in bytes.
//
// Returns:
//   - *StreamingSizeValidator: Validator configured with the provided size limit.
func NewStreamingSizeValidator(maxSize int64) *StreamingSizeValidator {
	return &StreamingSizeValidator{MaxSize: maxSize}
}

// Name returns the validator identifier used by attachment validation flows for logging and diagnostics.
//
// Returns:
//   - string: Stable validator name.
func (v *StreamingSizeValidator) Name() string {
	return "SizeValidator"
}

// Validate checks whether the uploaded content size is within the configured limit as an early guard in the streaming validation sequence.
//
// Parameters:
//   - ctx: Request context propagated through validation interfaces.
//   - reader: File reader for interface consistency; not consumed by this validator.
//   - size: Declared size of the uploaded content in bytes.
//   - contentType: Declared MIME type of the upload; not used by this validator.
//
// Returns:
//   - bool: `true` when the size is allowed, `false` when it exceeds the configured limit.
//   - string: Empty string on success, or a rejection message describing the size violation.
func (v *StreamingSizeValidator) Validate(ctx context.Context, reader io.ReadSeeker, size int64, contentType string) (bool, string) {
	if size > v.MaxSize {
		return false, fmt.Sprintf("file too large: %d bytes exceeds limit of %d bytes", size, v.MaxSize)
	}
	return true, ""
}

// objectTypeValidators returns the library-owned MIME boundary for config's
// AllowedMimeTypes and ForbiddenMimeTypes, followed by its registered and the
// caller's validators, which can only add restrictions. Object types should not
// register a StreamingFileTypeValidator for the same policy: each one rereads
// the object, and XML declarations are parsed in full.
func objectTypeValidators(config *ObjectTypeConfig, maxXMLTokenBytes int64, extra []StreamingAttachmentValidator) []StreamingAttachmentValidator {
	mimeValidator := NewStreamingFileTypeValidator(nil)
	mimeValidator.MaxXMLTokenBytes = maxXMLTokenBytes
	validators := []StreamingAttachmentValidator{mimeValidator}
	if config != nil {
		mimeValidator.AllowedTypes = config.AllowedMimeTypes
		mimeValidator.ForbiddenTypes = config.ForbiddenMimeTypes
		validators = append(validators, config.StreamingValidators...)
	}
	return append(validators, extra...)
}

// StreamingFileTypeValidator validates file type by reading file headers.
// This validator reads only the first 8KB of the file to detect the actual
// file type and compare it against declared MIME type and allowed types.
type StreamingFileTypeValidator struct {
	AllowedTypes          []string
	ForbiddenTypes        []string
	SecurityConstraints   SecurityConstraints
	CheckMimeTypeSpoofing bool
	// MaxXMLTokenBytes caps one token while declared XML is parsed. The
	// constructor starts it at the config default; the lifecycle manager sets
	// the validated attachment_xml_max_token_bytes value.
	MaxXMLTokenBytes int64
}

// NewStreamingFileTypeValidator constructs a StreamingFileTypeValidator for header-based MIME validation in the attachment security pipeline.
//
// Parameters:
//   - allowedTypes: Allowed MIME types; an empty list means all types are allowed unless blocked by security constraints.
//
// Returns:
//   - *StreamingFileTypeValidator: Validator initialized with defaults and the provided allow-list.
func NewStreamingFileTypeValidator(allowedTypes []string) *StreamingFileTypeValidator {
	return &StreamingFileTypeValidator{
		AllowedTypes:          allowedTypes,
		SecurityConstraints:   DefaultSecurityConstraints(),
		CheckMimeTypeSpoofing: true,
		MaxXMLTokenBytes:      config.DefaultAttachmentXMLMaxTokenBytes,
	}
}

// Name returns the validator identifier used by attachment validation flows for logging and diagnostics.
//
// Returns:
//   - string: Stable validator name.
func (v *StreamingFileTypeValidator) Name() string {
	return "FileTypeValidator"
}

// Validate inspects the uploaded file header to detect actual type, reject dangerous formats, and enforce declared and allowed MIME rules in the streaming validation path.
//
// Parameters:
//   - ctx: Request context propagated through validation interfaces.
//   - reader: Read-seeker over uploaded content used to read header bytes for type detection.
//   - size: Declared size of the uploaded content in bytes.
//   - contentType: Declared MIME type from the upload request.
//
// Returns:
//   - bool: `true` when type checks pass, `false` when the upload fails MIME or security rules.
//   - string: Empty string on success, or a rejection message explaining the failed check.
func (v *StreamingFileTypeValidator) Validate(ctx context.Context, reader io.ReadSeeker, size int64, contentType string) (bool, string) {
	declaredType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false, "invalid declared MIME type"
	}
	contentType = declaredType
	if size < 0 {
		return false, "invalid file size"
	}
	// Seek to beginning and read only the header bytes needed for file type detection (8KB max)
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return false, fmt.Sprintf("failed to seek to start: %v", err)
	}

	headerSize := min(size, int64(8192))

	header := make([]byte, headerSize)
	n, err := io.ReadFull(reader, header)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return false, fmt.Sprintf("failed to read file header: %v", err)
	}

	// Detect actual file type
	actualType, err := filetype.Match(header[:n])
	if err != nil {
		return false, fmt.Sprintf("failed to detect file type: %v", err)
	}
	actualMimeType := actualType.MIME.Value
	unrecognizedImage := actualMimeType == "" && strings.HasPrefix(contentType, "image/")

	// If no type detected, use the declared type
	if actualMimeType == "" {
		actualMimeType = contentType
	}

	// Check for dangerous file types
	for _, dangerous := range v.SecurityConstraints.DangerousTypes {
		if actualMimeType == dangerous || contentType == dangerous {
			return false, fmt.Sprintf("dangerous file type detected: %s", dangerous)
		}
	}
	if unrecognizedImage && v.CheckMimeTypeSpoofing {
		return false, fmt.Sprintf("MIME type spoofing detected: declared %s but no image signature was detected", contentType)
	}
	for _, forbidden := range v.ForbiddenTypes {
		if mimeTypeMatches(actualMimeType, forbidden) || mimeTypeMatches(contentType, forbidden) {
			return false, fmt.Sprintf("forbidden file type detected: %s", forbidden)
		}
	}

	// Check MIME type spoofing if enabled
	if v.CheckMimeTypeSpoofing && actualMimeType != "" && actualMimeType != contentType {
		// Allow some common safe mismatches
		if !v.isAcceptableMismatch(actualMimeType, contentType) {
			return false, fmt.Sprintf("MIME type spoofing detected: declared %s but actual type is %s", contentType, actualMimeType)
		}
	}

	// Check if type is in allowed list
	if len(v.AllowedTypes) > 0 {
		typeToCheck := actualMimeType
		if typeToCheck == "" {
			typeToCheck = contentType
		}

		if !v.isTypeAllowed(typeToCheck) || !v.isTypeAllowed(contentType) {
			return false, fmt.Sprintf("file type %s not allowed", typeToCheck)
		}
	}
	// SVG can also be served as XML, and browsers render every +xml type (RSS,
	// Atom, vnd.*+xml) as XML. Parse these completely, rather than treating a
	// clean prefix as evidence that the document is inert. ParseMediaType has
	// already lowercased contentType and stripped its parameters.
	if contentType == "application/xml" || contentType == "text/xml" || strings.HasSuffix(contentType, "+xml") {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return false, fmt.Sprintf("failed to seek XML content: %v", err)
		}
		// Each token may read at most tokenBytes past where it starts, bounding
		// the decoder's token buffer. An exhausted budget rejects even a clean
		// EOF, since the unread remainder was never inspected. The decoder also
		// keeps every open element's name and namespace bindings until it
		// closes, so the open start tags together share the same cap.
		tokenBytes := v.MaxXMLTokenBytes
		// The decoder reads ahead through this buffer, so every allowance covers it.
		budget := &io.LimitedReader{R: contextObjectReader{ctx: ctx, reader: reader}}
		buffered := bufio.NewReader(budget)
		readAhead := int64(buffered.Size())
		budget.N = readAhead + tokenBytes
		decoder := xml.NewDecoder(buffered)
		var openTags []int64
		var openBytes int64
		for allowed := budget.N; ; {
			start := decoder.InputOffset()
			token, err := decoder.Token()
			span := decoder.InputOffset() - start
			if budget.N == 0 || span > tokenBytes {
				return false, fmt.Sprintf("XML token exceeds %d bytes", tokenBytes)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				// Decoder errors quote the upload; report only bounded metadata.
				if syntaxErr, ok := errors.AsType[*xml.SyntaxError](err); ok {
					return false, fmt.Sprintf("invalid XML content at line %d", syntaxErr.Line)
				}
				return false, "invalid XML content"
			}
			switch token := token.(type) {
			case xml.StartElement:
				if token.Name.Space == "http://www.w3.org/2000/svg" || token.Name.Space == "http://www.w3.org/1999/xhtml" || strings.EqualFold(token.Name.Local, "svg") || strings.EqualFold(token.Name.Local, "html") {
					return false, "dangerous active XML format is unsupported"
				}
				if openBytes += span; openBytes > tokenBytes {
					return false, fmt.Sprintf("open XML elements exceed %d bytes", tokenBytes)
				}
				openTags = append(openTags, span)
			case xml.EndElement:
				// The decoder rejects unmatched end tags, so one is always open.
				openBytes -= openTags[len(openTags)-1]
				openTags = openTags[:len(openTags)-1]
			case xml.ProcInst:
				if strings.EqualFold(token.Target, "xml-stylesheet") {
					return false, "dangerous XML stylesheet is unsupported"
				}
			}
			next := decoder.InputOffset() + readAhead + tokenBytes
			budget.N += next - allowed
			allowed = next
		}
	}

	return true, ""
}

// isAcceptableMismatch checks whether a detected MIME type mismatch is considered safe within file-type validation.
//
// Parameters:
//   - actual: MIME type detected from file contents.
//   - declared: MIME type declared by the upload request.
//
// Returns:
//   - bool: `true` when the mismatch is allowed.
func (v *StreamingFileTypeValidator) isAcceptableMismatch(actual, declared string) bool {
	// Some common acceptable mismatches
	acceptableMismatches := map[string][]string{
		"text/plain":    {"application/octet-stream"},    // Plain text often misdetected
		"image/svg+xml": {"text/xml", "application/xml"}, // SVG can be detected as XML
	}

	if allowedDeclarations, exists := acceptableMismatches[actual]; exists {
		if slices.Contains(allowedDeclarations, declared) {
			return true
		}
	}

	return false
}

// isTypeAllowed checks whether a normalized MIME type appears in the validator allow-list.
//
// Parameters:
//   - mimeType: MIME type to test.
//
// Returns:
//   - bool: `true` when the MIME type is explicitly allowed.
func (v *StreamingFileTypeValidator) isTypeAllowed(mimeType string) bool {
	normalizedType := strings.ToLower(strings.TrimSpace(mimeType))

	for _, allowed := range v.AllowedTypes {
		if mimeTypeMatches(normalizedType, allowed) {
			return true
		}
	}

	return false
}

func mimeTypeMatches(actual, policy string) bool {
	return strings.EqualFold(strings.TrimSpace(actual), strings.TrimSpace(policy))
}

// StreamingImageDimensionValidator validates image dimensions by reading only the image headers.
// This validator uses Go's standard library image.DecodeConfig for efficient header-only parsing.
// Supported formats: JPEG, PNG, GIF, WebP, BMP (via golang.org/x/image extensions).
// Provides format verification to prevent MIME type spoofing attacks.
type StreamingImageDimensionValidator struct {
	MaxWidth  int
	MaxHeight int
	MinWidth  int
	MinHeight int
}

// NewStreamingImageDimensionValidator constructs a StreamingImageDimensionValidator for image-specific dimension checks in attachment processing.
//
// Parameters:
//   - maxWidth: Maximum allowed image width in pixels.
//   - maxHeight: Maximum allowed image height in pixels.
//   - minWidth: Minimum allowed image width in pixels.
//   - minHeight: Minimum allowed image height in pixels.
//
// Returns:
//   - *StreamingImageDimensionValidator: Validator configured with the provided dimension limits.
func NewStreamingImageDimensionValidator(maxWidth, maxHeight, minWidth, minHeight int) *StreamingImageDimensionValidator {
	return &StreamingImageDimensionValidator{
		MaxWidth:  maxWidth,
		MaxHeight: maxHeight,
		MinWidth:  minWidth,
		MinHeight: minHeight,
	}
}

// Name returns the validator identifier used by attachment validation flows for logging and diagnostics.
//
// Returns:
//   - string: Stable validator name.
func (v *StreamingImageDimensionValidator) Name() string {
	return "ImageDimensionValidator"
}

// Validate reads image metadata and enforces configured width and height bounds as part of the streaming image validation phase.
//
// Parameters:
//   - ctx: Request context propagated through validation interfaces.
//   - reader: Read-seeker over uploaded content used to decode image headers.
//   - size: Declared size of the uploaded content in bytes.
//   - contentType: Declared MIME type used for format consistency checks.
//
// Returns:
//   - bool: `true` when image dimensions are within configured bounds, `false` otherwise.
//   - string: Empty string on success, or a rejection message describing the violated bound.
func (v *StreamingImageDimensionValidator) Validate(ctx context.Context, reader io.ReadSeeker, size int64, contentType string) (bool, string) {
	// Extract dimensions based on content type
	width, height, err := v.extractImageDimensions(reader, contentType)
	if err != nil {
		return false, fmt.Sprintf("failed to read image dimensions: %v", err)
	}

	// Validate dimensions
	if width > v.MaxWidth || height > v.MaxHeight {
		return false, fmt.Sprintf("image dimensions %dx%d exceed maximum %dx%d", width, height, v.MaxWidth, v.MaxHeight)
	}

	if width < v.MinWidth || height < v.MinHeight {
		return false, fmt.Sprintf("image dimensions %dx%d below minimum %dx%d", width, height, v.MinWidth, v.MinHeight)
	}

	return true, ""
}

// extractImageDimensions reads minimal header data to extract image dimensions and verify format consistency for image validation.
//
// Parameters:
//   - reader: Read-seeker positioned over uploaded content.
//   - contentType: Declared MIME type used for expected format validation.
//
// Returns:
//   - int: Decoded image width in pixels.
//   - int: Decoded image height in pixels.
//   - error: Seek, decode, or format-mismatch error.
func (v *StreamingImageDimensionValidator) extractImageDimensions(reader io.ReadSeeker, contentType string) (int, int, error) {
	// Seek to beginning before dimension extraction
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	// Use Go's standard library image.DecodeConfig which only reads headers
	config, format, err := image.DecodeConfig(reader)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode image config: %w", err)
	}

	// Verify format matches declared content type for security
	expectedFormat := v.getFormatFromContentType(contentType)
	if expectedFormat == "" {
		return 0, 0, fmt.Errorf("unsupported content type: %s", contentType)
	}
	if format != expectedFormat {
		return 0, 0, fmt.Errorf("format mismatch: declared %s but detected %s", contentType, format)
	}

	return config.Width, config.Height, nil
}

// getFormatFromContentType maps MIME types to Go's image format strings
func (v *StreamingImageDimensionValidator) getFormatFromContentType(contentType string) string {
	switch strings.ToLower(contentType) {
	case "image/jpeg", "image/jpg":
		return "jpeg"
	case "image/png":
		return "png"
	case "image/gif":
		return "gif"
	case "image/webp":
		return "webp"
	case "image/bmp":
		return "bmp"
	default:
		// Return empty string to allow Go's format detection to handle unknown formats
		// The format verification will catch mismatches if needed
		return ""
	}
}

// StreamingSecurityValidator performs security checks on file content.
// This validator reads specific portions of files to detect security threats
// like embedded scripts, malicious headers, or other dangerous content.
type StreamingSecurityValidator struct {
	SecurityConstraints SecurityConstraints
}

// NewStreamingSecurityValidator constructs a StreamingSecurityValidator used to scan uploaded content headers for high-risk patterns and signatures.
//
// Returns:
//   - *StreamingSecurityValidator: Validator initialized with default security constraints.
func NewStreamingSecurityValidator() *StreamingSecurityValidator {
	return &StreamingSecurityValidator{
		SecurityConstraints: DefaultSecurityConstraints(),
	}
}

// Name returns the validator identifier used by attachment validation flows for logging and diagnostics.
//
// Returns:
//   - string: Stable validator name.
func (v *StreamingSecurityValidator) Name() string {
	return "SecurityValidator"
}

// Validate performs lightweight content scanning in the streaming pipeline to detect suspicious script patterns and executable signatures before attachment persistence.
//
// Parameters:
//   - ctx: Request context propagated through validation interfaces.
//   - reader: Read-seeker over uploaded content used to read initial bytes for inspection.
//   - size: Declared size of the uploaded content in bytes.
//   - contentType: Declared MIME type of the upload; currently informational for this validator.
//
// Returns:
//   - bool: `true` when no dangerous indicators are detected, `false` when content appears unsafe.
//   - string: Empty string on success, or a rejection message identifying the detected risk.
func (v *StreamingSecurityValidator) Validate(ctx context.Context, reader io.ReadSeeker, size int64, contentType string) (bool, string) {
	// Seek to beginning and read first few KB to check for malicious patterns
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return false, fmt.Sprintf("failed to seek to start: %v", err)
	}

	headerSize := min(size, int64(8192))

	header := make([]byte, headerSize)
	n, err := reader.Read(header)
	if err != nil && err != io.EOF {
		return false, fmt.Sprintf("failed to read file for security check: %v", err)
	}

	content := string(header[:n])
	contentLower := strings.ToLower(content)

	// Check for dangerous patterns
	dangerousPatterns := []string{
		"<script",
		"javascript:",
		"vbscript:",
		"onload=",
		"onerror=",
		"onclick=",
		"onfocus=",
		"onmouseover=",
		"eval(",
		"expression(",
	}

	for _, pattern := range dangerousPatterns {
		if strings.Contains(contentLower, pattern) {
			return false, fmt.Sprintf("dangerous pattern detected: %s", pattern)
		}
	}

	// Check for executable signatures
	if v.hasExecutableSignature(header[:n]) {
		return false, "executable file signature detected"
	}

	return true, ""
}

// hasExecutableSignature checks uploaded bytes for common executable signatures during security validation.
//
// Parameters:
//   - data: Leading bytes from uploaded content.
//
// Returns:
//   - bool: `true` when a known executable signature is detected.
func (v *StreamingSecurityValidator) hasExecutableSignature(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	// Check for common executable signatures
	signatures := [][]byte{
		{0x4D, 0x5A},             // PE executable (MZ)
		{0x7F, 0x45, 0x4C, 0x46}, // ELF executable
		{0xFE, 0xED, 0xFA, 0xCE}, // Mach-O executable (32-bit)
		{0xFE, 0xED, 0xFA, 0xCF}, // Mach-O executable (64-bit)
		{0xCA, 0xFE, 0xBA, 0xBE}, // Mach-O fat binary
	}

	for _, sig := range signatures {
		if len(data) >= len(sig) {
			match := true
			for i, b := range sig {
				if data[i] != b {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}

	return false
}
