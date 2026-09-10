package queryexec

import (
	"context"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type resultValueDecoder struct {
	ctx   context.Context
	nodes uint64
}

func (decoder *resultValueDecoder) check() error {
	if decoder == nil {
		return nil
	}
	if decoder.nodes&255 == 0 {
		if err := decoder.ctx.Err(); err != nil {
			return err
		}
	}
	decoder.nodes++
	return nil
}
func (decoder *resultValueDecoder) phase() error {
	if decoder == nil {
		return nil
	}
	return decoder.ctx.Err()
}
func convertValueContext(ctx context.Context, raw any) (searchjobs.Value, error) {
	decoder := resultValueDecoder{ctx: ctx}
	if err := decoder.phase(); err != nil {
		return searchjobs.Value{}, err
	}
	value, err := decoder.convertValue(raw)
	if canceled := decoder.phase(); canceled != nil {
		return searchjobs.Value{}, canceled
	}
	return value, err
}
func (decoder *resultValueDecoder) list(length int, item func(int) (searchjobs.Value, error)) (searchjobs.Value, error) {
	if decoder == nil {
		return searchjobs.ListValueFromItems(length, item)
	}
	return searchjobs.ListValueFromItemsContext(decoder.ctx, length, item)
}
func (decoder *resultValueDecoder) object(length int, item func(int) (searchjobs.ObjectField, error)) (searchjobs.Value, error) {
	if decoder == nil {
		return searchjobs.ObjectValueFromFields(length, item)
	}
	return searchjobs.ObjectValueFromFieldsContext(decoder.ctx, length, item)
}

// Context-aware heap sorting bounds cancellation latency without allocating a
// second key array or weakening the comparator's ordering after cancellation.
func sortDecodedValues[T any](decoder *resultValueDecoder, values []T, compare func(T, T) int) error {
	if decoder == nil {
		slices.SortFunc(values, compare)
		return nil
	}
	if err := decoder.phase(); err != nil {
		return err
	}
	sift := func(root, end int) error {
		for root < end/2 {
			if err := decoder.check(); err != nil {
				return err
			}
			child := root*2 + 1
			if child+1 < end && compare(values[child], values[child+1]) < 0 {
				child++
			}
			if compare(values[root], values[child]) >= 0 {
				return nil
			}
			values[root], values[child] = values[child], values[root]
			root = child
		}
		return nil
	}
	for root := len(values) / 2; root > 0; root-- {
		if err := sift(root-1, len(values)); err != nil {
			return err
		}
	}
	for end := len(values) - 1; end > 0; end-- {
		if err := decoder.check(); err != nil {
			return err
		}
		values[0], values[end] = values[end], values[0]
		if err := sift(0, end); err != nil {
			return err
		}
	}
	return decoder.phase()
}
