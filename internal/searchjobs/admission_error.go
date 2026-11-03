package searchjobs

// AdmissionError carries private diagnostic context for a rejected search.
// Error and Unwrap expose only the public error. Cause is classification input:
// it can contain SQL, credentials, or endpoints and must never be serialized or
// logged verbatim. In particular, an internal deadline must not turn a storage
// failure into a caller cancellation through errors.Is.
type AdmissionError struct {
	Stage string
	Err   error
	Cause error
}

func (err *AdmissionError) Error() string { return err.Err.Error() }
func (err *AdmissionError) Unwrap() error { return err.Err }
