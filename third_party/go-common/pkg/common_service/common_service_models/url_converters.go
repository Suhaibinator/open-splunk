// Package common_service_models provides service-layer models and interfaces
// that define the core business entities used throughout the service layer.
// These models act as an abstraction over persistence models and protocol models.
package common_service_models

import (
	"fmt"
	"net/url"
	"strings"
)

// RewritePresignedURL executes its part of the service logic in the common_service_models backend flow.
func RewritePresignedURL(urlStr string, publicBaseURL string) (string, error) {
	if urlStr == "" {
		return "", fmt.Errorf("presigned URL cannot be empty")
	}

	if publicBaseURL == "" {
		// This might be intentional (no CDN configured), so return original
		return urlStr, nil
	}

	// Parse the presigned URL
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse presigned URL: %w", err)
	}

	// Parse the public base URL
	publicURL, err := url.Parse(publicBaseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse public base URL: %w", err)
	}

	// Validate that public URL has required components
	if publicURL.Scheme == "" || publicURL.Host == "" {
		return "", fmt.Errorf("public base URL must have scheme and host")
	}

	// Replace the scheme and host
	parsedURL.Scheme = publicURL.Scheme
	parsedURL.Host = publicURL.Host

	// Handle path rewriting intelligently
	if publicURL.Path != "" && publicURL.Path != "/" {
		publicPath := strings.TrimRight(publicURL.Path, "/")
		originalPath := parsedURL.Path

		// MinIO presigned URLs have the format: /bucket-name/path/to/object
		// Extract the bucket name from the original URL path
		pathParts := strings.SplitN(strings.TrimPrefix(originalPath, "/"), "/", 2)
		if len(pathParts) > 0 {
			originalBucket := pathParts[0]
			remainingPath := ""
			if len(pathParts) > 1 {
				remainingPath = "/" + pathParts[1]
			}

			// Check if the public path ends with the same bucket name
			// This handles cases like publicBaseURL = "http://cdn.com/mybucket"
			if strings.HasSuffix(publicPath, "/"+originalBucket) || publicPath == "/"+originalBucket {
				// Public path already includes the bucket, just append the remaining path
				parsedURL.Path = publicPath + remainingPath
			} else {
				// Public path doesn't include the bucket, keep the original full path
				parsedURL.Path = publicPath + originalPath
			}
		} else {
			// Couldn't parse bucket from path, just prepend the public path
			parsedURL.Path = publicPath + originalPath
		}
	}

	return parsedURL.String(), nil
}

// ComposeAssetURL executes its part of the service logic in the common_service_models backend flow.
func ComposeAssetURL(objectPath string, publicBaseURL string) string {
	if objectPath == "" || publicBaseURL == "" {
		return objectPath
	}

	// Ensure the base URL doesn't end with a slash
	baseURL := strings.TrimRight(publicBaseURL, "/")

	// URL encode the object path to handle special characters, spaces, etc.
	// We need to encode each path segment separately to preserve the directory structure
	pathSegments := strings.Split(strings.Trim(objectPath, "/"), "/")
	encodedSegments := make([]string, len(pathSegments))
	for i, segment := range pathSegments {
		encodedSegments[i] = url.PathEscape(segment)
	}

	// Join the encoded segments with forward slashes
	encodedPath := "/" + strings.Join(encodedSegments, "/")

	return baseURL + encodedPath
}

// ComposePublicAttachmentURL composes the stable, anonymously fetchable URL of an
// attachment stored with public visibility.
//
// The composed URL always carries the public bucket as its first path segment:
//
//	<publicBaseURL>/<publicBucket>/<objectName>
//
// publicBaseURL (MinioPublicBaseURL) is therefore the public origin fronting the
// MinIO server ROOT — the server itself, or a CDN in front of it — never a
// bucket-scoped origin. RewritePresignedURL only swaps scheme and host and keeps
// the private bucket segment MinIO puts in a presigned path, so a single origin
// serves both presigned (private bucket) and public (public bucket) URLs. Keeping
// the bucket in the path also makes an attachment's visibility deducible from its
// URL.
//
// Parameters:
//   - objectName: Verified storage object name of the attachment (FinalObjectName).
//   - publicBaseURL: Public origin of the MinIO server root or its CDN.
//   - publicBucket: Name of the anonymously readable bucket (MinioPublicBucket).
//
// Returns:
//   - result1: Composed public URL, or an empty string when any input is empty —
//     the object has no public identity or public storage is not configured, and
//     callers treat an empty string as "no URL".
func ComposePublicAttachmentURL(objectName string, publicBaseURL string, publicBucket string) string {
	if objectName == "" || publicBaseURL == "" || publicBucket == "" {
		return ""
	}

	// The bucket is just the first path segment, so the shared path-encoding rules
	// apply to it unchanged.
	return ComposeAssetURL(strings.Trim(publicBucket, "/")+"/"+strings.Trim(objectName, "/"), publicBaseURL)
}
