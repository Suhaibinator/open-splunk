package common_sanitizer

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
)

// Email validation constants define RFC 5321 compliant length limits.
const (
	// MaxEmailLocalLength is the maximum allowed length for the local part
	// of an email address (the portion before the @ symbol) as defined by RFC 5321.
	MaxEmailLocalLength = 64

	// MaxEmailDomainLength is the maximum allowed length for the domain part
	// of an email address (the portion after the @ symbol) as defined by RFC 5321.
	MaxEmailDomainLength = 253
)

// Precompiled regex for better performance
var (
	// RFC 5322 simplified regex for common use cases
	// This regex validates:
	// - Local part: alphanumeric + special chars (.!#$%&'*+/=?^_`{|}~-)
	// - Domain: standard domain format with subdomains
	emailRegex = regexp.MustCompile(`^[a-zA-Z0-9.!#$%&'*+/=?^_` + "`" + `{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$`)

	// Common disposable email domains (can be extended via config)
	defaultDisposableEmailDomains = map[string]bool{
		// — Core list you already had
		"10minutemail.com":  true,
		"guerrillamail.com": true,
		"mailinator.com":    true,
		"tempmail.com":      true,
		"throwaway.email":   true,

		// — GuerrillaMail & friends
		"grr.la":                 true,
		"guerrillamail.biz":      true,
		"guerrillamail.de":       true,
		"guerrillamail.info":     true,
		"guerrillamail.net":      true,
		"guerrillamail.org":      true,
		"guerrillamailblock.com": true,
		"pokemail.net":           true,
		"sharklasers.com":        true,

		// — YOPmail network
		"yopmail.com":   true,
		"yopmail.fr":    true,
		"yopmail.gq":    true,
		"yopmail.net":   true,
		"yopmail.pp.ua": true,

		// — Maildrop variants
		"maildrop.cc": true,
		"maildrop.cf": true,
		"maildrop.ga": true,
		"maildrop.gq": true,
		"maildrop.ml": true,

		// — 10MinuteMail rotating
		"mvrht.com":    true,
		"r8r4p0cb.com": true,

		// — FakeInbox & similar
		"fakeinbox.com":  true,
		"getairmail.com": true,

		// — Spamgourmet / MintEmail
		"cd.mintemail.com": true,
		"mintemail.com":    true,
		"spamgourmet.com":  true,

		// — General multi-service throw-aways
		"20mail.eu":          true,
		"30wave.com":         true,
		"6paq.com":           true,
		"adrianou.gq":        true,
		"arugitu.gq":         true,
		"bst-72.com":         true,
		"clrmail.com":        true,
		"crotslep.tk":        true,
		"dr69.site":          true,
		"drivetagdev.com":    true,
		"eyepaste.com":       true,
		"fulvie.com":         true,
		"maildx.com":         true,
		"mailed.ro":          true,
		"maileimer.de":       true,
		"mailexpire.com":     true,
		"mailfa.tk":          true,
		"mailforspam.com":    true,
		"mailfreeonline.com": true,
		"mailguard.me":       true,
		"mailin8r.com":       true,
		"mailinater.com":     true,
		"mailinator.net":     true,
		"mailinator.org":     true,
		"mailinator2.com":    true,
		"mailincubator.com":  true,
		"mailismagic.com":    true,
		"mailme.lv":          true,
		"mailme24.com":       true,
		"mailmetrash.com":    true,
		"mailmoat.com":       true,
		"mailms.com":         true,
		"mailnesia.com":      true,
		"mailnull.com":       true,
		"mailorg.org":        true,
		"mailpick.biz":       true,
		"mailrock.biz":       true,
		"mailscrap.com":      true,
		"mailshell.com":      true,
		"mailsiphon.com":     true,
		"mailtemp.info":      true,
		"mailtome.de":        true,
		"mailtothis.com":     true,
		"mailtrash.net":      true,
		"nezzart.com":        true,
		"securehost.com.es":  true,
		"spam4.me":           true,
		"tverya.com":         true,
		"ucupkoh.ml":         true,
		"u.qvap.ru":          true,
		"vssms.com":          true,
		"zain.site":          true,
		"zainmax.net":        true,
	}
)

// EmailValidator defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - EmailValidator is read or populated by backend components in this package to move requests through this stage.
type EmailValidator struct {
	config               *EmailValidationConfig
	compiledRegex        *regexp.Regexp
	disposableDomainsMap map[string]bool
	blockedDomainsMap    map[string]bool
	allowedDomainsMap    map[string]bool
}

// NewEmailValidator constructs an email validator and panics on invalid configuration.
func NewEmailValidator(config *EmailValidationConfig) *EmailValidator {
	validator, err := newEmailValidator(config)
	if err != nil {
		panic(fmt.Sprintf("EmailValidator initialization failed: %v", err))
	}
	return validator
}

func newEmailValidator(config *EmailValidationConfig) (*EmailValidator, error) {
	validator := &EmailValidator{config: config}

	if err := validator.initialize(); err != nil {
		return nil, err
	}
	return validator, nil
}

// initialize performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) initialize() error {
	if v.config == nil {
		return fmt.Errorf("email validation config is nil")
	}

	// Compile custom regex if provided, otherwise use default unless strict RFC mode is enabled.
	if v.config.CustomRegexPattern != "" {
		regex, err := regexp.Compile(v.config.CustomRegexPattern)
		if err != nil {
			return fmt.Errorf("invalid custom regex pattern: %w", err)
		}
		v.compiledRegex = regex
	} else if v.config.UseStrictRFC5322 {
		v.compiledRegex = nil
	} else {
		v.compiledRegex = emailRegex
	}

	// Build disposable domains map
	v.disposableDomainsMap = make(map[string]bool)
	if len(v.config.DisposableDomains) > 0 {
		for _, domain := range v.config.DisposableDomains {
			v.disposableDomainsMap[strings.ToLower(domain)] = true
		}
	} else {
		// Use default disposable domains
		for domain := range defaultDisposableEmailDomains {
			v.disposableDomainsMap[domain] = true
		}
	}

	// Convert arrays to maps for efficient lookup
	v.blockedDomainsMap = make(map[string]bool)
	for _, domain := range v.config.BlockedDomains {
		v.blockedDomainsMap[strings.ToLower(domain)] = true
	}

	v.allowedDomainsMap = make(map[string]bool)
	for _, domain := range v.config.AllowedDomains {
		v.allowedDomainsMap[strings.ToLower(domain)] = true
	}

	return nil
}

// ValidateEmail performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) ValidateEmail(email string) (string, error) {
	// Normalize: trim spaces and lowercase
	email = strings.TrimSpace(strings.ToLower(email))

	// Basic length check
	if len(email) == 0 {
		return "", ErrEmailEmpty
	}

	// Use configured max length
	maxLength := v.config.MaxEmailLength
	if len(email) > maxLength {
		return "", configuredMaximumLengthError(ErrEmailTooLong, maxLength)
	}

	// Use standard library for initial parsing
	addr, err := mail.ParseAddress(email)
	if err != nil {
		// Try to provide more specific error based on what's wrong
		if !strings.Contains(email, "@") {
			return "", ErrEmailMissingAtSign
		}
		return "", ErrInvalidEmailFormat
	}

	// Extract the actual email address
	email = addr.Address

	// Regex validation for additional security (skipped in strict RFC mode unless custom regex is set)
	if v.compiledRegex != nil && !v.compiledRegex.MatchString(email) {
		return "", ErrEmailInvalidCharacters
	}

	// Split and validate parts
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		if len(parts) < 2 {
			return "", ErrEmailMissingAtSign
		}
		// Multiple @ signs
		return "", ErrInvalidEmailFormat
	}

	local, domain := parts[0], parts[1]

	// Check for missing parts
	if len(local) == 0 {
		return "", ErrEmailMissingLocalPart
	}
	if len(domain) == 0 {
		return "", ErrEmailMissingDomain
	}

	// Validate local part
	if err := v.validateLocalPart(local); err != nil {
		return "", err
	}

	// Validate domain part
	if err := v.validateDomainPart(domain); err != nil {
		return "", err
	}

	// Check against blocked/allowed lists
	if err := v.checkDomainRestrictions(domain); err != nil {
		return "", err
	}

	return email, nil
}

// validateLocalPart performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) validateLocalPart(local string) error {
	if len(local) == 0 {
		return ErrEmailMissingLocalPart
	}
	maxLocalLength := v.config.MaxLocalPartLength
	if len(local) > maxLocalLength {
		return configuredMaximumLengthError(ErrEmailLocalPartTooLong, maxLocalLength)
	}

	// Check for invalid patterns
	if strings.HasPrefix(local, ".") {
		return ErrEmailStartsWithDot
	}
	if strings.HasSuffix(local, ".") {
		return ErrEmailEndsWithDot
	}
	if strings.Contains(local, "..") {
		return ErrEmailConsecutiveDots
	}

	// Check for SQL injection attempts if enabled
	checkSQLInjection := v.config.BlockSQLPatterns
	if checkSQLInjection {
		suspiciousPatterns := []string{"'", "\"", "--", "/*", "*/", ";", "\\"}
		for _, pattern := range suspiciousPatterns {
			if strings.Contains(local, pattern) {
				return ErrEmailInvalidCharacters
			}
		}
	}

	return nil
}

// validateDomainPart performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) validateDomainPart(domain string) error {
	if len(domain) == 0 {
		return ErrEmailMissingDomain
	}
	maxDomainLength := v.config.MaxDomainLength
	if len(domain) > maxDomainLength {
		return configuredMaximumLengthError(ErrEmailDomainTooLong, maxDomainLength)
	}

	// Allow domain literals when strict RFC 5322 mode is enabled.
	if v.config.UseStrictRFC5322 && strings.HasPrefix(domain, "[") && strings.HasSuffix(domain, "]") {
		return nil
	}

	// Must contain at least one dot when required
	if v.config.RequireDotInDomain && !strings.Contains(domain, ".") {
		return ErrEmailInvalidDomain
	}

	// Check each domain label
	labels := strings.SplitSeq(domain, ".")
	for label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return ErrEmailInvalidDomain
		}
		// Labels must start and end with alphanumeric
		if !isAlphanumeric(label[0]) || !isAlphanumeric(label[len(label)-1]) {
			return ErrEmailInvalidDomain
		}
	}

	return nil
}

// checkDomainRestrictions performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) checkDomainRestrictions(domain string) error {
	// Check if domain is in blocked list
	if v.blockedDomainsMap[domain] {
		return ErrEmailDomainBlocked
	}

	// Check if we have an allowed list and domain is not in it
	if len(v.allowedDomainsMap) > 0 && !v.allowedDomainsMap[domain] {
		return ErrEmailDomainNotAllowed
	}

	// Check for disposable emails if not allowed
	checkDisposable := !v.config.AllowDisposableEmails
	if checkDisposable && v.isDisposableEmail(domain) {
		return ErrEmailDisposableNotAllowed
	}

	return nil
}

// isDisposableEmail performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (v *EmailValidator) isDisposableEmail(domain string) bool {
	if v.disposableDomainsMap != nil {
		return v.disposableDomainsMap[domain]
	}
	return defaultDisposableEmailDomains[domain]
}

// isAlphanumeric checks if a rune is alphanumeric

// isAlphanumeric performs its package-specific work in the request sanitization and validation flow before service handlers run.
func isAlphanumeric(r byte) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}
