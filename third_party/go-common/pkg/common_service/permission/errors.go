package permission

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Permission-related errors for consistent error handling throughout the system.
// These errors provide specific context for permission failures and can be
// used by API handlers to return appropriate HTTP status codes.
var (
	// ErrInsufficientPermissions indicates that a user does not have the required
	// permission level to perform an operation.
	ErrInsufficientPermissions = errors.New("insufficient permissions for this operation")

	// ErrUnknownOperation indicates that a permission operation is not recognized
	// by the system. This is typically a configuration or programming error.
	ErrUnknownOperation = errors.New("unknown permission operation")

	// ErrInvalidUserType indicates that a user type is not valid or recognized
	// by the permission system.
	ErrInvalidUserType = errors.New("invalid user type")

	// ErrPermissionConfigurationError indicates an error in the permission
	// configuration, such as invalid thresholds or missing required operations.
	ErrPermissionConfigurationError = errors.New("permission configuration error")

	// ErrNonAdminComparison indicates an attempt to compare admin tiers for
	// users who are not admins.
	ErrNonAdminComparison = errors.New("cannot compare admin tiers for non-admin users")
)

// PermissionError carries state used by the permission backend flow.
type PermissionError struct {
	// OperationName is the string representation of the operation that failed
	OperationName string

	// UserType is the user type that was checked
	UserType proto_models.UserType

	// RequiredUserType is the minimum user type required for the operation
	RequiredUserType proto_models.UserType

	// Message provides additional context about the error
	Message string

	// Underlying error, if any
	Err error
}

// Error executes this step in the permission backend flow.
func (e *PermissionError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("permission error: %s (operation: %s, user_type: %s, required: %s)",
			e.Message, e.OperationName, e.UserType, e.RequiredUserType)
	}

	return fmt.Sprintf("permission denied for operation %s: user type %s insufficient, requires %s",
		e.OperationName, e.UserType, e.RequiredUserType)
}

// Unwrap executes this step in the permission backend flow.
func (e *PermissionError) Unwrap() error {
	return e.Err
}

// PermissionOperation is the constraint shared by the site-scoped and
// organization-scoped operation enums. Both name themselves through String(),
// which is all the error constructors need from an operation.
type PermissionOperation interface {
	SitePermissionOperation | OrganizationPermissionOperation

	String() string
}

// NewPermissionError builds a denial for a site- or organization-scoped
// operation, naming the operation by its enum string.
func NewPermissionError[O PermissionOperation](operation O, userType, requiredUserType proto_models.UserType, message string) *PermissionError {
	return NewPermissionErrorFromString(operation.String(), userType, requiredUserType, message, nil)
}

// NewPermissionErrorWithCause is NewPermissionError with the underlying failure
// attached, so callers can errors.Is/As through to it.
func NewPermissionErrorWithCause[O PermissionOperation](operation O, userType, requiredUserType proto_models.UserType, message string, err error) *PermissionError {
	return NewPermissionErrorFromString(operation.String(), userType, requiredUserType, message, err)
}

// NewPermissionErrorFromString executes this step in the permission backend flow.
func NewPermissionErrorFromString(operationName string, userType, requiredUserType proto_models.UserType, message string, err error) *PermissionError {
	return &PermissionError{
		OperationName:    operationName,
		UserType:         userType,
		RequiredUserType: requiredUserType,
		Message:          message,
		Err:              err,
	}
}

// IsPermissionError executes this step in the permission backend flow.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for direct permission errors
	if _, ok := errors.AsType[*PermissionError](err); ok {
		return true
	}

	// Check for known permission error types
	return errors.Is(err, ErrInsufficientPermissions) ||
		errors.Is(err, ErrUnknownOperation) ||
		errors.Is(err, ErrInvalidUserType) ||
		errors.Is(err, ErrPermissionConfigurationError) ||
		errors.Is(err, ErrNonAdminComparison)
}

// GetPermissionError executes this step in the permission backend flow.
func GetPermissionError(err error) *PermissionError {
	if permErr, ok := errors.AsType[*PermissionError](err); ok {
		return permErr
	}
	return nil
}
