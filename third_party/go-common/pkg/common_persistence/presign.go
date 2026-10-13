package common_persistence

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// SigV4 presigned-query constants shared with the offline edge verifier: the
// verifier only accepts AWS4-HMAC-SHA256 signatures scoped to the s3 service
// with SignedHeaders=host and an UNSIGNED-PAYLOAD content hash.
const (
	presignAlgorithm       = "AWS4-HMAC-SHA256"
	presignService         = "s3"
	presignRequestSuffix   = "aws4_request"
	presignUnsignedPayload = "UNSIGNED-PAYLOAD"
	presignISO8601Format   = "20060102T150405Z"
	presignDateFormat      = "20060102"

	// presignMaxExpiry is the SigV4 ceiling on X-Amz-Expires (7 days); MinIO and
	// minio-go reject anything larger, so the offline presigner does too.
	presignMaxExpiry = 7 * 24 * time.Hour
)

// validatePresignObjectName rejects object names whose canonical URI would not
// survive the trip through real clients and proxies verbatim: browsers resolve
// dot-segments before sending and NGINX merges empty segments, so a signed
// "a/../b" or "a//b" arrives as a different raw URI and the signature never
// verifies — while a non-normalizing client could deliver the raw traversal
// with a genuine signature, shifting all path interpretation onto the origin.
// Invalid UTF-8 is rejected because rune-wise encoding would collapse distinct
// byte sequences onto U+FFFD, breaking name -> canonical-URI injectivity.
func validatePresignObjectName(objectName string) error {
	if objectName == "" {
		return fmt.Errorf("presign host-scoped download URL: object name cannot be empty")
	}
	if !utf8.ValidString(objectName) {
		return fmt.Errorf("presign host-scoped download URL: object name is not valid UTF-8")
	}
	if strings.HasPrefix(objectName, "/") {
		return fmt.Errorf("presign host-scoped download URL: object name cannot start with a slash")
	}
	for segment := range strings.SplitSeq(objectName, "/") {
		switch segment {
		case "":
			return fmt.Errorf("presign host-scoped download URL: object name %q contains an empty path segment", objectName)
		case ".", "..":
			return fmt.Errorf("presign host-scoped download URL: object name %q contains a dot path segment", objectName)
		}
	}
	return nil
}

// PresignHostScopedDownloadURL signs a path-style GET for /<private bucket>/<objectName>
// against publicOrigin, entirely offline, following the SigV4 presigned-query rules the
// edge verifier reconstructs (SignedHeaders=host only, UNSIGNED-PAYLOAD). Unlike
// GetPresignedURL, the signature covers the PUBLIC host rather than the internal MinIO
// endpoint, so no post-hoc host rewrite is needed and the verifier's canonical request
// matches. The output is byte-identical to what minio-go's PresignedGetObject would emit
// for the same host, credentials, region, expiry, and signing time.
//
// publicOrigin is "host[:port]" or "scheme://host[:port]" (scheme defaults to https);
// origins carrying a path, query, fragment, or userinfo are rejected, the host is
// lowercased before signing (clients send lowercase hosts on the wire), and non-ASCII
// hosts are rejected because browsers would punycode them into a different canonical
// host. Default ports (:80 for http, :443 for https) are stripped from the signed
// host, matching browser and SDK behavior.
//
// signingTime determines X-Amz-Date, so callers that quantize it obtain byte-identical
// URLs within the quantum window. Expiry counts from signingTime: quantize DOWNWARD
// only (an up-rounded, future X-Amz-Date trips verifier clock-skew checks), and keep
// expiry comfortably above the quantum — the effective remaining validity of a URL
// minted late in a window of size Q is expiry-Q.
//
// accessKey/secretKey select the signing credential. When accessKey is non-empty both
// must be set and the URL is signed with that dedicated pair (verifiable only by an
// edge holding the same secret; MinIO does not know it). When accessKey is empty the
// MinIO credential is used, producing URLs MinIO itself also verifies.
func (h *CommonPersistenceHandler[U]) PresignHostScopedDownloadURL(ctx context.Context, objectName string, publicOrigin string, region string, expiry time.Duration, signingTime time.Time, accessKey string, secretKey config.Secret) (string, error) {
	dedicatedCredentials := accessKey != ""
	withContext := func(err error) error {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "presign_host_scoped_download_url"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.String(appLogger.KeyPublicOrigin, publicOrigin),
			zap.String(appLogger.KeyRegion, region),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, expiry),
			zap.Bool(appLogger.KeyDedicatedCredentials, dedicatedCredentials))
	}
	if err := validatePresignObjectName(objectName); err != nil {
		return "", withContext(err)
	}
	if region == "" {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: region cannot be empty"))
	}
	if expiry <= 0 {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: expiry must be positive, got: %v", expiry))
	}
	if expiry < time.Second {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: expiry must be at least one second, got: %v", expiry))
	}
	if expiry > presignMaxExpiry {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: expiry cannot exceed the SigV4 maximum of 7 days, got: %v", expiry))
	}
	if signingTime.IsZero() {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: signing time cannot be zero"))
	}
	signingSecret := secretKey.StringValue()
	if accessKey != "" && signingSecret == "" {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: dedicated presign access key is set but its secret is empty"))
	}
	if accessKey == "" {
		if signingSecret != "" {
			return "", withContext(fmt.Errorf("presign host-scoped download URL: dedicated presign secret is set but its access key is empty"))
		}
		accessKey = h.minioAccessKey
		signingSecret = h.minioSecretKey.StringValue()
	}
	if accessKey == "" || signingSecret == "" {
		return "", withContext(fmt.Errorf("presign host-scoped download URL: no signing credentials are configured"))
	}

	scheme, host, err := parsePresignOrigin(publicOrigin)
	if err != nil {
		return "", withContext(err)
	}

	t := signingTime.UTC()
	amzDate := t.Format(presignISO8601Format)
	scope := strings.Join([]string{t.Format(presignDateFormat), region, presignService, presignRequestSuffix}, "/")
	credential := accessKey + "/" + scope

	// Canonical query: the five X-Amz-* parameters sorted by name with AWS-style
	// escaping. url.Values.Encode sorts keys and query-escapes values; replacing
	// "+" with "%20" converts Go's form encoding into SigV4's %20-for-space rule.
	query := url.Values{}
	query.Set("X-Amz-Algorithm", presignAlgorithm)
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Date", amzDate)
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expiry/time.Second), 10))
	query.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := strings.ReplaceAll(query.Encode(), "+", "%20")

	canonicalURI := awsEncodePath("/" + h.minioBucket + "/" + objectName)

	canonicalRequest := strings.Join([]string{
		http.MethodGet,
		canonicalURI,
		canonicalQuery,
		"host:" + host + "\n",
		"host",
		presignUnsignedPayload,
	}, "\n")

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		presignAlgorithm,
		amzDate,
		scope,
		hex.EncodeToString(canonicalRequestHash[:]),
	}, "\n")

	// Standard SigV4 key derivation: AWS4+secret -> date -> region -> service -> request.
	dateKey := hmacSHA256([]byte("AWS4"+signingSecret), t.Format(presignDateFormat))
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, presignService)
	signingKey := hmacSHA256(serviceKey, presignRequestSuffix)
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	return scheme + "://" + host + canonicalURI + "?" + canonicalQuery + "&X-Amz-Signature=" + signature, nil
}

// parsePresignOrigin resolves a configured public origin into the scheme and
// canonical host the presign signature covers. The scheme defaults to https
// when absent, and default ports are stripped so the signed host matches what
// browsers and SDKs put on the wire.
func parsePresignOrigin(publicOrigin string) (scheme string, host string, err error) {
	if publicOrigin == "" {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin cannot be empty")
	}
	raw := publicOrigin
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", "", fmt.Errorf("presign host-scoped download URL: invalid public origin %q: %w", publicOrigin, parseErr)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must use http or https, got scheme %q", publicOrigin, parsed.Scheme)
	}
	if parsed.User != nil {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must not carry userinfo", publicOrigin)
	}
	if parsed.Opaque != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must not carry a path", publicOrigin)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must not carry a query", publicOrigin)
	}
	if parsed.Fragment != "" {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must not carry a fragment", publicOrigin)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q has no host", publicOrigin)
	}

	// Canonicalize before signing: clients lowercase the URL host on the wire
	// (and NGINX's $host is lowercased), so a mixed-case signed host would never
	// verify. Non-ASCII hosts are rejected rather than punycoded — a browser
	// would send the xn-- form, which is a different canonical host than the
	// configured literal.
	host = strings.ToLower(parsed.Host)
	for _, r := range host {
		if r > 127 {
			return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q must be an ASCII host (configure the punycode form for internationalized domains)", publicOrigin)
		}
	}
	// Strip default ports so the signed host matches what clients send: browsers
	// and SDKs drop :80/:443, and a signature over "host:443" would never verify.
	// The port is compared numerically so zero-padded forms like ":0443" are
	// normalized too.
	if hostOnly, port, splitErr := net.SplitHostPort(host); splitErr == nil {
		portNumber, portErr := strconv.Atoi(port)
		if portErr != nil {
			return "", "", fmt.Errorf("presign host-scoped download URL: public origin %q has an invalid port %q", publicOrigin, port)
		}
		if (parsed.Scheme == "http" && portNumber == 80) || (parsed.Scheme == "https" && portNumber == 443) {
			host = hostOnly
			if ip := net.ParseIP(hostOnly); ip != nil && ip.To4() == nil {
				host = "[" + hostOnly + "]"
			}
		}
	}
	return parsed.Scheme, host, nil
}

// awsEncodePath percent-encodes a URL path per the SigV4 canonical-URI rules:
// every byte outside the unreserved set [A-Za-z0-9-._~] is encoded as uppercase
// %XX (space becomes %20, never "+"), "/" separators are preserved, and the
// input is encoded exactly once.
func awsEncodePath(pathName string) string {
	var encoded strings.Builder
	for _, r := range pathName {
		if 'A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9' ||
			r == '-' || r == '_' || r == '.' || r == '~' || r == '/' {
			encoded.WriteRune(r)
			continue
		}
		runeBytes := make([]byte, utf8.RuneLen(r))
		utf8.EncodeRune(runeBytes, r)
		for _, b := range runeBytes {
			encoded.WriteString("%")
			encoded.WriteString(strings.ToUpper(hex.EncodeToString([]byte{b})))
		}
	}
	return encoded.String()
}

// hmacSHA256 computes HMAC-SHA256 of data under key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
