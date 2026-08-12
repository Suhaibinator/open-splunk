package knowledgecatalog

import (
	"errors"
	"testing"
)

func FuzzDecodeCursorRejectsMalformedOrReboundInput(f *testing.F) {
	fingerprint := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	valid, err := encodeCursor(testCursorKey, listCursor{
		Fingerprint:     fingerprint,
		CatalogRevision: 1,
		CatalogState:    fingerprint,
		PrimaryString:   "name",
		ObjectID:        "ko-cursor",
	})
	if err != nil {
		f.Fatalf("encode seed cursor: %v", err)
	}
	f.Add(valid, fingerprint)
	f.Add("", fingerprint)
	f.Add("not-a-cursor", fingerprint)
	f.Add(valid, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	f.Fuzz(func(t *testing.T, token, requestedFingerprint string) {
		if len(token) > maximumCursorBytes*2 || len(requestedFingerprint) > maximumCursorBytes*2 {
			t.Skip()
		}
		cursor, err := decodeCursor(testCursorKey, token, requestedFingerprint, SortByName)
		if err != nil {
			if !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("decodeCursor() error = %v, want ErrInvalidCursor", err)
			}
			return
		}
		if cursor.Fingerprint != requestedFingerprint || !validCursor(cursor) ||
			cursor.PrimaryString == "" || cursor.PrimaryInteger != nil || cursor.SecondaryString != "" {
			t.Fatalf("decodeCursor() accepted invalid cursor %#v", cursor)
		}
	})
}
