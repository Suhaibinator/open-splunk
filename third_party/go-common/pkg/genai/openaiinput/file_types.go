// Package openaiinput contains the OpenAI Responses input constraints shared by the standalone
// Genkit adapter and the durable agent-context reconstruction path.
package openaiinput

import (
	"fmt"
	"mime"
	"path/filepath"
	"strings"
)

const (
	// MaxFileInputBytes is the Responses limit for each individual file and for the combined files
	// in one request. Each individual file must be strictly smaller; a combined request may equal
	// it.
	MaxFileInputBytes uint64 = 50 * 1024 * 1024
	// MaxImageInputPayloadBytes is the documented aggregate image-input payload limit for one
	// request. It is independent of the smaller input_file budget above.
	MaxImageInputPayloadBytes uint64 = 512 * 1024 * 1024
	// MaxImageInputsPerRequest is the documented maximum number of image inputs in one request.
	MaxImageInputsPerRequest uint64 = 1500
)

var acceptedImageMIMETypes = map[string]struct{}{
	"image/gif":  {},
	"image/jpeg": {},
	"image/png":  {},
	"image/webp": {},
}

var acceptedFileExtensions = map[string]struct{}{
	// PDF.
	".pdf": {},
	// Excel and spreadsheet formats.
	".xla": {}, ".xlb": {}, ".xlc": {}, ".xlm": {}, ".xls": {}, ".xlsx": {}, ".xlt": {}, ".xlw": {},
	".csv": {}, ".tsv": {}, ".iif": {},
	// Rich documents.
	".doc": {}, ".docx": {}, ".dot": {}, ".odt": {}, ".rtf": {},
	// Presentations.
	".pot": {}, ".ppa": {}, ".pps": {}, ".ppt": {}, ".pptx": {}, ".pwz": {}, ".wiz": {},
	// Text and code formats from the Responses file-input table.
	".asm": {}, ".bat": {}, ".c": {}, ".cc": {}, ".conf": {}, ".cpp": {}, ".css": {}, ".cxx": {},
	".def": {}, ".dic": {}, ".eml": {}, ".h": {}, ".hh": {}, ".htm": {}, ".html": {}, ".ics": {},
	".ifb": {}, ".in": {}, ".js": {}, ".json": {}, ".ksh": {}, ".list": {}, ".log": {}, ".markdown": {},
	".md": {}, ".mht": {}, ".mhtml": {}, ".mime": {}, ".mjs": {}, ".nws": {}, ".pl": {}, ".py": {},
	".rst": {}, ".s": {}, ".sql": {}, ".srt": {}, ".text": {}, ".txt": {}, ".vcf": {}, ".vtt": {},
	".xml": {},
}

var acceptedFileMIMETypes = map[string]struct{}{
	// PDF, spreadsheets, rich documents, and presentations.
	"application/pdf": {},
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {},
	"application/vnd.ms-excel": {},
	"text/csv":                 {},
	"application/csv":          {},
	"text/tsv":                 {},
	"text/x-iif":               {},
	"application/x-iif":        {},
	"application/vnd.google-apps.spreadsheet":                                 {},
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document": {},
	"application/msword": {},
	"application/rtf":    {},
	"text/rtf":           {},
	"application/vnd.oasis.opendocument.text":                                   {},
	"application/vnd.apple.pages":                                               {},
	"application/vnd.google-apps.document":                                      {},
	"application/vnd.apple.iwork":                                               {},
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": {},
	"application/vnd.ms-powerpoint":                                             {},
	"application/vnd.apple.keynote":                                             {},
	"application/vnd.google-apps.presentation":                                  {},
	// Text and code.
	"application/javascript":         {},
	"application/typescript":         {},
	"text/xml":                       {},
	"text/x-shellscript":             {},
	"text/x-rst":                     {},
	"text/x-makefile":                {},
	"text/x-lisp":                    {},
	"text/x-asm":                     {},
	"text/vbscript":                  {},
	"text/css":                       {},
	"message/rfc822":                 {},
	"application/x-sql":              {},
	"application/x-scala":            {},
	"application/x-rust":             {},
	"application/x-powershell":       {},
	"text/x-diff":                    {},
	"text/x-patch":                   {},
	"application/x-patch":            {},
	"text/plain":                     {},
	"text/markdown":                  {},
	"text/x-java":                    {},
	"text/x-script.python":           {},
	"text/x-python":                  {},
	"text/x-c":                       {},
	"text/x-c++":                     {},
	"text/x-golang":                  {},
	"text/html":                      {},
	"text/x-php":                     {},
	"application/x-php":              {},
	"application/x-httpd-php":        {},
	"application/x-httpd-php-source": {},
	"text/x-ruby":                    {},
	"text/x-sh":                      {},
	"text/x-bash":                    {},
	"application/x-bash":             {},
	"text/x-zsh":                     {},
	"text/x-tex":                     {},
	"text/x-csharp":                  {},
	"application/json":               {},
	"text/x-typescript":              {},
	"text/javascript":                {},
	"text/x-go":                      {},
	"text/x-rust":                    {},
	"text/x-scala":                   {},
	"text/x-kotlin":                  {},
	"text/x-swift":                   {},
	"text/x-lua":                     {},
	"text/x-r":                       {},
	"text/x-julia":                   {},
	"text/x-perl":                    {},
	"text/x-objectivec":              {},
	"text/x-objectivec++":            {},
	"text/x-erlang":                  {},
	"text/x-elixir":                  {},
	"text/x-haskell":                 {},
	"text/x-clojure":                 {},
	"text/x-groovy":                  {},
	"text/x-dart":                    {},
	"text/x-awk":                     {},
	"application/x-awk":              {},
	"text/jsx":                       {},
	"text/tsx":                       {},
	"text/x-handlebars":              {},
	"text/x-mustache":                {},
	"text/x-ejs":                     {},
	"text/x-jinja2":                  {},
	"text/x-liquid":                  {},
	"text/x-erb":                     {},
	"text/x-twig":                    {},
	"text/x-pug":                     {},
	"text/x-jade":                    {},
	"text/x-tmpl":                    {},
	"text/x-cmake":                   {},
	"text/x-dockerfile":              {},
	"text/x-gradle":                  {},
	"text/x-ini":                     {},
	"text/x-properties":              {},
	"text/x-protobuf":                {},
	"application/x-protobuf":         {},
	"text/x-sql":                     {},
	"text/x-sass":                    {},
	"text/x-scss":                    {},
	"text/x-less":                    {},
	"text/x-hcl":                     {},
	"text/x-terraform":               {},
	"application/x-terraform":        {},
	"text/x-toml":                    {},
	"application/x-toml":             {},
	"application/graphql":            {},
	"application/x-graphql":          {},
	"text/x-graphql":                 {},
	"application/x-ndjson":           {},
	"application/json5":              {},
	"application/x-json5":            {},
	"text/x-yaml":                    {},
	"application/toml":               {},
	"application/x-yaml":             {},
	"application/yaml":               {},
	"text/x-astro":                   {},
	"text/srt":                       {},
	"application/x-subrip":           {},
	"text/x-subrip":                  {},
	"text/vtt":                       {},
	"text/x-vcard":                   {},
	"text/calendar":                  {},
}

var audioVideoExtensions = map[string]struct{}{
	".aac": {}, ".aif": {}, ".aiff": {}, ".avi": {}, ".flac": {}, ".m4a": {}, ".m4v": {},
	".mkv": {}, ".mov": {}, ".mp3": {}, ".mp4": {}, ".mpeg": {}, ".mpg": {}, ".oga": {},
	".ogg": {}, ".opus": {}, ".wav": {}, ".webm": {}, ".wma": {}, ".wmv": {},
}

// ValidateFileType checks a declared MIME type and filename against the current Responses
// accepted-file table. A supported declared MIME type is authoritative even when a nonstandard
// extension is used; an absent/generic MIME type requires a listed extension. Explicitly
// unsupported MIME types never become acceptable merely because the filename looks supported.
func ValidateFileType(contentType, filename string) error {
	canonical := ""
	if strings.TrimSpace(contentType) != "" {
		var err error
		canonical, _, err = mime.ParseMediaType(strings.TrimSpace(contentType))
		if err != nil || canonical == "" {
			return fmt.Errorf("malformed part content type %q", contentType)
		}
		canonical = strings.ToLower(canonical)
	}
	if strings.HasPrefix(canonical, "audio/") || strings.HasPrefix(canonical, "video/") {
		return fmt.Errorf("Responses input_file does not support audio or video content type %q", canonical)
	}
	if strings.HasPrefix(canonical, "image/") {
		return fmt.Errorf("image content type %q must be sent as input_image, not input_file", canonical)
	}
	if _, ok := acceptedFileMIMETypes[canonical]; ok {
		return nil
	}

	extension := strings.ToLower(filepath.Ext(strings.TrimSpace(filename)))
	if canonical == "" || canonical == "application/octet-stream" || canonical == "binary/octet-stream" {
		if extension == "" && canonical == "" && strings.TrimSpace(filename) == "" {
			// A file ID carries its authoritative type in OpenAI's file metadata.
			return nil
		}
		if _, ok := acceptedFileExtensions[extension]; ok {
			return nil
		}
		if _, media := audioVideoExtensions[extension]; media {
			return fmt.Errorf("Responses input_file does not support audio or video file %q", filename)
		}
		return fmt.Errorf("Responses input_file does not support file %q", filename)
	}
	return fmt.Errorf("Responses input_file does not support content type %q", canonical)
}

// ValidateImageType checks a declared MIME type against the current Responses image-input table.
// The API additionally requires GIFs to be non-animated; callers cannot establish that property
// from a MIME declaration alone, so OpenAI remains the authoritative content validator.
func ValidateImageType(contentType string) error {
	canonical, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || canonical == "" {
		return fmt.Errorf("malformed image content type %q", contentType)
	}
	canonical = strings.ToLower(canonical)
	if _, ok := acceptedImageMIMETypes[canonical]; !ok {
		return fmt.Errorf("Responses input_image does not support content type %q", canonical)
	}
	return nil
}

// DefaultExtension returns a listed extension suitable for raw file_data with this MIME type.
// Generic code MIME types intentionally fall back to .txt, which is accepted without pretending
// to infer a language-specific filename that is absent from the official extension column.
func DefaultExtension(contentType string) string {
	canonical, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		return ""
	}
	canonical = strings.ToLower(canonical)
	if canonical == "application/vnd.apple.iwork" {
		// The shared MIME type does not identify whether the bytes are Pages or Keynote, so a safe
		// filename cannot be inferred. Callers can still provide the real filename explicitly.
		return ""
	}
	if extension := map[string]string{
		"application/pdf": ".pdf",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
		"application/vnd.ms-excel": ".xls",
		"text/csv":                 ".csv",
		"application/csv":          ".csv",
		"text/tsv":                 ".tsv",
		"text/x-iif":               ".iif",
		"application/x-iif":        ".iif",
		"application/vnd.google-apps.spreadsheet":                                 ".xlsx",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
		"application/msword": ".doc",
		"application/rtf":    ".rtf",
		"text/rtf":           ".rtf",
		"application/vnd.oasis.opendocument.text":                                   ".odt",
		"application/vnd.apple.pages":                                               ".pages",
		"application/vnd.google-apps.document":                                      ".docx",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
		"application/vnd.ms-powerpoint":                                             ".ppt",
		"application/vnd.apple.keynote":                                             ".key",
		"application/vnd.google-apps.presentation":                                  ".pptx",
		"application/json":                                                          ".json",
		"application/javascript":                                                    ".js",
		"text/javascript":                                                           ".js",
		"text/css":                                                                  ".css",
		"text/html":                                                                 ".html",
		"text/markdown":                                                             ".md",
		"text/xml":                                                                  ".xml",
		"message/rfc822":                                                            ".eml",
		"text/calendar":                                                             ".ics",
		"text/vtt":                                                                  ".vtt",
		"text/x-vcard":                                                              ".vcf",
		"text/srt":                                                                  ".srt",
		"application/x-subrip":                                                      ".srt",
		"text/x-subrip":                                                             ".srt",
		"application/x-sql":                                                         ".sql",
		"text/x-sql":                                                                ".sql",
	}[canonical]; extension != "" {
		return extension
	}
	if _, ok := acceptedFileMIMETypes[canonical]; ok {
		return ".txt"
	}
	return ""
}
