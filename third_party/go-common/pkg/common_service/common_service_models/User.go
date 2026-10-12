// Package common_service_models provides service-layer models and interfaces
// that define the core business entities used throughout the service layer.
// These models act as an abstraction over persistence models and protocol models.
package common_service_models

import (
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
)

// UserIdType defines the type used for user IDs throughout the service layer.
// Using a type alias allows for easy changes to the underlying type if needed.
type UserIdType = uint64

// User defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type User struct {
	// ID is the unique identifier for the user.
	ID UserIdType `json:"id"`

	// Username is the user's unique username.
	Username string `json:"username"`

	// AvatarUrl contains the object path to the user's profile picture.
	// This is the FinalObjectName from the attachment, not a full URL.
	AvatarUrl string `json:"avatar_url"`

	// Email is the user's email address (stored in lowercase).
	Email string `json:"email"`

	// PhoneNumber is the user's phone number in E.164 international format.
	PhoneNumber *phonenumbers.PhoneNumber `json:"phone_number"`

	// FirstName is the user's first name.
	FirstName string `json:"first_name"`

	// LastName is the user's last name.
	LastName string `json:"last_name"`

	// UserType indicates the type/role of the user (admin, regular, etc.).
	UserType proto_models.UserType `json:"user_type"`

	// Banned indicates whether the user account is banned.
	Banned bool `json:"banned"`

	// Bot indicates whether this is a bot/automated account.
	Bot bool `json:"is_bot"`
}

// UserLike defines the minimum set of methods required for user objects.
// This interface allows for different user implementations while maintaining
// a consistent API. The generic type system uses this interface to work with
// various user types across different layers.
type UserLike interface {
	// GetID returns the user's unique identifier.
	GetID() UserIdType

	// GetEmail returns the user's email address.
	GetEmail() string

	// GetPhoneNumber returns the user's phone number in E.164 format.
	GetPhoneNumber() *phonenumbers.PhoneNumber

	// GetFirstName returns the user's first name.
	GetFirstName() string

	// GetLastName returns the user's last name.
	GetLastName() string

	// GetUsername returns the user's username.
	GetUsername() string

	// GetAvatarUrl returns the object path to the user's avatar.
	GetAvatarUrl() string

	// GetUserType returns the user's type/role.
	GetUserType() proto_models.UserType

	// IsBanned returns whether the user is banned.
	IsBanned() bool

	// IsBot returns whether the user is a bot/automated account.
	IsBot() bool
}

// Compile-time check to ensure User implements UserLike
var _ UserLike = &User{}

// GetID returns the requested value for User in the common_service_models backend flow.
func (u User) GetID() UserIdType {
	return u.ID
}

// GetEmail returns the requested value for User in the common_service_models backend flow.
func (u User) GetEmail() string {
	return u.Email
}

// GetPhoneNumber returns the requested value for User in the common_service_models backend flow.
func (u User) GetPhoneNumber() *phonenumbers.PhoneNumber {
	return u.PhoneNumber
}

// GetFirstName returns the requested value for User in the common_service_models backend flow.
func (u User) GetFirstName() string {
	return u.FirstName
}

// GetLastName returns the requested value for User in the common_service_models backend flow.
func (u User) GetLastName() string {
	return u.LastName
}

// GetUsername returns the requested value for User in the common_service_models backend flow.
func (u User) GetUsername() string {
	return u.Username
}

// GetAvatarUrl returns the requested value for User in the common_service_models backend flow.
func (u User) GetAvatarUrl() string {
	return u.AvatarUrl
}

// GetUserType returns the requested value for User in the common_service_models backend flow.
func (u User) GetUserType() proto_models.UserType {
	return u.UserType
}

// IsBanned evaluates whether the requested condition is true for User in the common_service_models backend flow.
func (u User) IsBanned() bool {
	return u.Banned
}

// IsBot evaluates whether the requested condition is true for User in the common_service_models backend flow.
func (u User) IsBot() bool {
	return u.Bot
}
