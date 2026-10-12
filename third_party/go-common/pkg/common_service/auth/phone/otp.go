package phone

// OTPCodeLength is the standard length for phone verification OTP codes
const OTPCodeLength = 6

// IsValidOTPFormat executes this step in the phone backend flow.
func IsValidOTPFormat(code string) bool {
	if len(code) != OTPCodeLength {
		return false
	}

	// Check if all characters are digits
	for _, char := range code {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}
