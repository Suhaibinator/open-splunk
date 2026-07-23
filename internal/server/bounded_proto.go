package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"google.golang.org/protobuf/proto"
)

// boundedProtoResponse transfers ownership of one shared serialization permit
// from a typed handler through protobuf marshaling and the response write.
type boundedProtoResponse[Message proto.Message] struct {
	message Message
	ctx     context.Context
	release func()
}

type boundedProtoCodecOptions struct {
	stateError   string
	messageError string
	contextError func(context.Context) error
	maximumBytes int
	sizeError    string
}

// boundedProtoCodec delegates request decoding to the ordinary protobuf codec
// while centralizing the permit, cancellation, marshal, and write lifecycle
// shared by potentially large API responses.
type boundedProtoCodec[Request any, Message proto.Message] struct {
	inner   codec.Codec[Request, Message]
	options boundedProtoCodecOptions
}

func newBoundedProtoCodec[Request any, Message proto.Message](
	inner codec.Codec[Request, Message],
	options boundedProtoCodecOptions,
) *boundedProtoCodec[Request, Message] {
	return &boundedProtoCodec[Request, Message]{inner: inner, options: options}
}

func (bounded *boundedProtoCodec[Request, Message]) NewRequest() Request {
	return bounded.inner.NewRequest()
}

func (bounded *boundedProtoCodec[Request, Message]) Decode(request *http.Request) (Request, error) {
	return bounded.inner.Decode(request)
}

func (bounded *boundedProtoCodec[Request, Message]) DecodeBytes(data []byte) (Request, error) {
	return bounded.inner.DecodeBytes(data)
}

func (bounded *boundedProtoCodec[Request, Message]) Encode(response http.ResponseWriter, result *boundedProtoResponse[Message]) error {
	if result == nil || result.release == nil {
		return errors.New(bounded.options.stateError)
	}
	defer result.release()
	if isNilDependency(result.message) {
		return errors.New(bounded.options.messageError)
	}
	if err := bounded.contextError(result.ctx); err != nil {
		return err
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	if bounded.options.maximumBytes > 0 && len(payload) > bounded.options.maximumBytes {
		message := bounded.options.sizeError
		if message == "" {
			message = "protobuf response exceeds its byte limit"
		}
		return errors.New(message)
	}
	if err := bounded.contextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}

func (bounded *boundedProtoCodec[Request, Message]) contextError(ctx context.Context) error {
	if bounded.options.contextError != nil {
		return bounded.options.contextError(ctx)
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}
