// Package notificationkey owns the activity-scope grammar and native notification
// key formats. It has no transport or persistence dependencies. Activity scopes
// are exact-match visibility hints, not realtime topics or authorization grants.
package notificationkey

import (
	"errors"
	"strconv"
	"strings"
)

const MaxActivityScopeLength = 128

var ErrInvalidID = errors.New("invalid notification key ID")

// ChatActivityScope identifies the visible channel timeline (parentMessageID=0)
// or thread. Both unread-chat and approval notifications use this scope.
func ChatActivityScope(channelID, parentMessageID uint64) (string, error) {
	if channelID == 0 {
		return "", ErrInvalidID
	}
	return chatKey(channelID, parentMessageID), nil
}

// UnreadChatGroupKey groups reusable activity across all threads in a channel. Its spelling
// matches the activity scope, but grouping is an independent backend concern.
func UnreadChatGroupKey(channelID uint64) (string, error) {
	if channelID == 0 {
		return "", ErrInvalidID
	}
	return chatKey(channelID, 0), nil
}

func chatKey(channelID, parentMessageID uint64) string {
	return "chat." + strconv.FormatUint(channelID, 10) + "." + strconv.FormatUint(parentMessageID, 10)
}

// ApprovalGroupKey groups one tool call's approval notification. It is not the
// activity scope: seeing the corresponding chat content suppresses interruptions.
func ApprovalGroupKey(messageID uint64) (string, error) {
	if messageID == 0 {
		return "", ErrInvalidID
	}
	return "approval." + strconv.FormatUint(messageID, 10), nil
}

// ValidActivityScope validates a nonempty scope without normalizing it. Names
// use lowercase ASCII letters, digits, underscores and hyphens, beginning with
// a letter. Numeric segments use canonical unsigned decimal uint64 values.
// The native chat namespace additionally requires channel and parent IDs.
// An empty entry scope means no tracking; it is not a valid activity command item.
func ValidActivityScope(scope string) bool {
	if scope == "" || len(scope) > MaxActivityScopeLength {
		return false
	}
	segments := strings.Split(scope, ".")
	if !validName(segments[0]) {
		return false
	}
	for _, segment := range segments[1:] {
		if !validName(segment) && !validID(segment, true) {
			return false
		}
	}
	if segments[0] == "chat" {
		return len(segments) == 3 && validID(segments[1], false) && validID(segments[2], true)
	}
	return true
}

func validName(segment string) bool {
	if segment == "" || segment[0] < 'a' || segment[0] > 'z' {
		return false
	}
	for i := 1; i < len(segment); i++ {
		c := segment[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validID(segment string, allowZero bool) bool {
	id, err := strconv.ParseUint(segment, 10, 64)
	return err == nil && (allowZero || id != 0) && strconv.FormatUint(id, 10) == segment
}
