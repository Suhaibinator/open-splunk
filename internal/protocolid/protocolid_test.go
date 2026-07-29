package protocolid

import (
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"a",
		"A0._:-",
		strings.Repeat("a", int(MaximumBytes)),
	} {
		if !Valid(value) {
			t.Errorf("Valid(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"",
		"-leading",
		"contains/slash",
		"contains space",
		"nonascii-\u00e9",
		string([]byte{'a', 0xff}),
		strings.Repeat("a", int(MaximumBytes)+1),
	} {
		if Valid(value) {
			t.Errorf("Valid(%q) = true, want false", value)
		}
	}
}

func TestValidWithMaximumTightensButDoesNotWidenHardLimit(t *testing.T) {
	t.Parallel()
	if !ValidWithMaximum("abcd", 4) {
		t.Fatal("ValidWithMaximum rejected value at configured limit")
	}
	if ValidWithMaximum("abcde", 4) {
		t.Fatal("ValidWithMaximum accepted value above configured limit")
	}
	if ValidWithMaximum("a", 0) {
		t.Fatal("ValidWithMaximum accepted nonempty value with zero limit")
	}
	if ValidWithMaximum(
		strings.Repeat("a", int(MaximumBytes)+1),
		MaximumBytes+1,
	) {
		t.Fatal("ValidWithMaximum widened the protocol hard limit")
	}
}
