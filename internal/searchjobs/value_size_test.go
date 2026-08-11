package searchjobs_test

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestValueProtoSizeLowerBoundNeverExceedsConvertedSize(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("+0001.2300")
	if err != nil {
		t.Fatal(err)
	}
	object, err := searchjobs.ObjectValue(
		searchjobs.ObjectField{Name: "empty", Value: searchjobs.ListValue()},
		searchjobs.ObjectField{Name: "decimal", Value: decimal},
	)
	if err != nil {
		t.Fatal(err)
	}
	values := []searchjobs.Value{
		searchjobs.NullValue(),
		searchjobs.StringValue("hello"),
		searchjobs.SignedValue(-42),
		searchjobs.UnsignedValue(42),
		searchjobs.DoubleValue(3.5),
		searchjobs.BoolValue(false),
		searchjobs.BytesValue([]byte{0, 1, 2}),
		searchjobs.TimeValue(time.Unix(0, 0)),
		searchjobs.DurationValue(0),
		decimal,
		searchjobs.ListValue(searchjobs.NullValue(), searchjobs.ListValue()),
		object,
	}
	for _, value := range values {
		lower, exceeded, err := value.ProtoSizeLowerBound(math.MaxUint64)
		if err != nil {
			t.Fatalf("ProtoSizeLowerBound(%v): %v", value.Kind(), err)
		}
		if exceeded {
			t.Fatalf("ProtoSizeLowerBound(%v) unexpectedly saturated", value.Kind())
		}
		converted, err := searchjobproto.Value(context.Background(), value)
		if err != nil {
			t.Fatalf("Value(%v): %v", value.Kind(), err)
		}
		if actual := uint64(proto.Size(converted)); lower > actual {
			t.Fatalf("ProtoSizeLowerBound(%v) = %d, converted size = %d", value.Kind(), lower, actual)
		}
	}
}

func TestValueProtoSizeLowerBoundSaturatesWithoutAllocating(t *testing.T) {
	value := searchjobs.StringValue(strings.Repeat("x", 512))
	if got := testing.AllocsPerRun(1_000, func() {
		size, exceeded, err := value.ProtoSizeLowerBound(64)
		if err != nil || size != 65 || !exceeded {
			panic("unexpected preflight result")
		}
	}); got != 0 {
		t.Fatalf("ProtoSizeLowerBound allocations = %v, want 0", got)
	}

	size, exceeded, err := value.ProtoSizeLowerBound(math.MaxUint64)
	if err != nil || exceeded || size != 514 {
		t.Fatalf("unbounded ProtoSizeLowerBound = (%d, %v, %v), want (514, false, nil)", size, exceeded, err)
	}
}

func TestValueProtoSizeLowerBoundCountsDeepEmptyContainers(t *testing.T) {
	value := searchjobs.ListValue()
	for range 32 {
		value = searchjobs.ListValue(value)
	}
	const want = uint64(130)
	size, exceeded, err := value.ProtoSizeLowerBound(want)
	if err != nil || exceeded || size != want {
		t.Fatalf("exact deep-empty lower bound = (%d, %v, %v), want (%d, false, nil)", size, exceeded, err, want)
	}
	size, exceeded, err = value.ProtoSizeLowerBound(want - 1)
	if err != nil || !exceeded || size != want {
		t.Fatalf("saturated deep-empty lower bound = (%d, %v, %v), want (%d, true, nil)", size, exceeded, err, want)
	}
}
