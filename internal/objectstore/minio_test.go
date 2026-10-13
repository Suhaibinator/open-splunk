package objectstore

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
)

func TestUnknownStreamCapRejectsOverflowBeforeSuccessfulPublication(t *testing.T) {
	for _, test := range []struct {
		data     string
		want     string
		tooLarge bool
	}{
		{data: "exact", want: "exact"},
		{data: "extra!", tooLarge: true},
	} {
		reader := &cappedReader{reader: bytes.NewBufferString(test.data), remaining: 5}
		received, err := io.ReadAll(reader)
		if test.tooLarge {
			if !errors.Is(err, common_persistence.ErrObjectStreamTooLarge) || len(received) != 0 {
				t.Fatalf("overflow = %q/%v", received, err)
			}
		} else if err != nil || string(received) != test.want {
			t.Fatalf("exact stream = %q/%v", received, err)
		}
	}
}
func TestKnownStreamUsesCurrentOffsetAndStopsAtDeclaredSize(t *testing.T) {
	source := bytes.NewReader([]byte("prefix-payload-suffix"))
	if _, err := source.Seek(7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	reader := boundedReader(source, 7)
	received, err := io.ReadAll(reader)
	if err != nil || string(received) != "payload" {
		t.Fatalf("bounded reader = %q/%v", received, err)
	}
	if position, err := source.Seek(0, io.SeekCurrent); err != nil || position != 7 {
		t.Fatalf("ReaderAt offset was rewound: %d/%v", position, err)
	}
}
