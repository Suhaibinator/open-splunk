// Package hecadapter converts the bounded HEC protocol domain into canonical
// Open Splunk ingestion requests. It owns no HTTP, credential parsing, or
// persistence behavior.
package hecadapter

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/hec"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const semanticDigestDomain = "open-splunk-hec-semantics-v1\x00"

// RequestContext contains the trusted values captured once at the HTTP
// authentication boundary. RequestID is a random, canonical protocol ID and
// Channel is already validated by the protocol package.
type RequestContext struct {
	TenantID       string
	Authentication auth.Authentication
	RequestID      string
	ReceivedAt     time.Time
	Channel        hec.Channel
}

// JSON converts a complete, already bounded envelope sequence. Conversion is
// request-atomic: the first envelope failure returns no admission request.
func JSON(context RequestContext, envelopes []hec.Envelope) (ingest.AdmissionRequest, error) {
	if len(envelopes) == 0 {
		return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorNoData, nil)
	}
	if err := validateContext(context); err != nil {
		return ingest.AdmissionRequest{}, err
	}
	events := make([]*opensplunkv1.LogEvent, 0, len(envelopes))
	for _, envelope := range envelopes {
		event, err := convertEnvelope(context, envelope)
		if err != nil {
			return ingest.AdmissionRequest{}, err
		}
		events = append(events, event)
	}
	return admissionRequest(context, events)
}

// Raw converts the complete output of the fixed HEC raw breaker using one
// validated request-level metadata and time snapshot.
func Raw(
	context RequestContext,
	query hec.RawQuery,
	lines [][]byte,
) (ingest.AdmissionRequest, error) {
	if len(lines) == 0 {
		return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorNoData, nil)
	}
	if err := validateContext(context); err != nil {
		return ingest.AdmissionRequest{}, err
	}
	metadata, _, err := resolveMetadata(context.Authentication, query.Metadata, 0)
	if err != nil {
		return ingest.AdmissionRequest{}, err
	}
	eventTime := context.ReceivedAt
	timeSource := opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK
	if query.Time.Present {
		nanoseconds, parseErr := hec.ParseEpochNanoseconds(query.Time.Value)
		if parseErr != nil {
			return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorInvalidDataFormat, parseErr)
		}
		eventTime = time.Unix(0, nanoseconds).UTC()
		timeSource = opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED
	}
	events := make([]*opensplunkv1.LogEvent, 0, len(lines))
	for ordinal, line := range lines {
		if len(line) == 0 || strings.IndexByte(string(line), 0) >= 0 {
			return ingest.AdmissionRequest{}, hec.NewEventError(
				hec.ErrorInvalidDataFormat,
				ordinal,
				errors.New("HEC raw event is empty or contains NUL"),
			)
		}
		message := string(line)
		events = append(events, canonicalEvent(
			context,
			ordinal,
			metadata,
			eventTime,
			timeSource,
			append([]byte(nil), line...),
			&message,
			nil,
		))
	}
	return admissionRequest(context, events)
}

func validateContext(context RequestContext) error {
	if context.TenantID == "" || context.RequestID == "" || context.ReceivedAt.IsZero() ||
		context.Authentication.TokenID == "" || context.Authentication.TokenVersion == 0 {
		return hec.NewProtocolError(hec.ErrorInternal, errors.New("HEC adapter context is incomplete"))
	}
	if context.Authentication.Purpose != auth.IngestionTokenPurposeHEC ||
		context.Authentication.BoundCollectorID != "" ||
		len(context.Authentication.AuthorizedIndexes) == 0 {
		return hec.NewProtocolError(hec.ErrorInvalidToken, errors.New("HEC authentication snapshot is unusable"))
	}
	if context.Authentication.HECProfile.IndexerAcknowledgment && context.Channel == "" {
		return hec.NewProtocolError(hec.ErrorChannelMissing, nil)
	}
	return nil
}

func convertEnvelope(context RequestContext, envelope hec.Envelope) (*opensplunkv1.LogEvent, error) {
	metadata, err := hec.DecodeEnvelopeMetadata(envelope)
	if err != nil {
		return nil, err
	}
	resolved, _, err := resolveMetadata(context.Authentication, metadata, envelope.Number)
	if err != nil {
		return nil, err
	}
	eventTime, explicit, err := hec.ParseEnvelopeTime(envelope, context.ReceivedAt)
	if err != nil {
		return nil, err
	}
	raw, err := envelope.RawEvent()
	if err != nil {
		return nil, err
	}
	fields, err := typedFields(envelope)
	if err != nil {
		return nil, err
	}
	var message *string
	if envelope.Event.Value.Kind == hec.JSONString {
		value := envelope.Event.Value.StringValue
		message = &value
	}
	timeSource := opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK
	if explicit {
		timeSource = opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED
	}
	return canonicalEvent(
		context,
		envelope.Number,
		resolved,
		eventTime,
		timeSource,
		raw,
		message,
		fields,
	), nil
}

func resolveMetadata(
	authentication auth.Authentication,
	event hec.MetadataValues,
	eventNumber int,
) (hec.MetadataValues, auth.AuthorizedIndexPolicy, error) {
	token := hec.MetadataValues{}
	profile := authentication.HECProfile
	if profile.DefaultIndexName != "" {
		token.Index = hec.OptionalString{Present: true, Value: profile.DefaultIndexName}
	}
	if profile.DefaultHost != "" {
		token.Host = hec.OptionalString{Present: true, Value: profile.DefaultHost}
	}
	if profile.DefaultSource != "" {
		token.Source = hec.OptionalString{Present: true, Value: profile.DefaultSource}
	}
	if profile.DefaultSourcetype != "" {
		token.Sourcetype = hec.OptionalString{Present: true, Value: profile.DefaultSourcetype}
	}
	selectedIndex := hec.ResolveMetadata(event, token, hec.MetadataValues{}, hec.MetadataValues{}).Index
	if !selectedIndex.Present || selectedIndex.Value == "" {
		return hec.MetadataValues{}, auth.AuthorizedIndexPolicy{}, hec.NewEventError(
			hec.ErrorIncorrectIndex,
			eventNumber,
			errors.New("HEC index has no resolved value"),
		)
	}
	var policy auth.AuthorizedIndexPolicy
	found := false
	for _, candidate := range authentication.AuthorizedIndexes {
		if candidate.Name == selectedIndex.Value {
			policy, found = candidate, true
			break
		}
	}
	if !found {
		return hec.MetadataValues{}, auth.AuthorizedIndexPolicy{}, hec.NewEventError(
			hec.ErrorIncorrectIndex,
			eventNumber,
			errors.New("HEC index is not authorized"),
		)
	}
	index := hec.MetadataValues{}
	if policy.DefaultSourcetype != "" {
		index.Sourcetype = hec.OptionalString{Present: true, Value: policy.DefaultSourcetype}
	}
	resolved := hec.ResolveMetadata(event, token, index, hec.DefaultMetadataFallbacks())
	if !resolved.Host.Present || !resolved.Source.Present || !resolved.Sourcetype.Present ||
		!resolved.Index.Present {
		return hec.MetadataValues{}, auth.AuthorizedIndexPolicy{}, hec.NewProtocolError(
			hec.ErrorInternal,
			errors.New("HEC metadata resolution is incomplete"),
		)
	}
	return resolved, policy, nil
}

func canonicalEvent(
	context RequestContext,
	ordinal int,
	metadata hec.MetadataValues,
	eventTime time.Time,
	timeSource opensplunkv1.EventTimeSource,
	raw []byte,
	message *string,
	fields *opensplunkv1.TypedObject,
) *opensplunkv1.LogEvent {
	return &opensplunkv1.LogEvent{
		EventId:         context.RequestID + "-" + strconv.Itoa(ordinal),
		IndexName:       metadata.Index.Value,
		EventTime:       timestamppb.New(eventTime),
		CollectedAt:     timestamppb.New(context.ReceivedAt),
		EventTimeSource: timeSource,
		Host:            metadata.Host.Value,
		Source:          metadata.Source.Value,
		Sourcetype:      metadata.Sourcetype.Value,
		Message:         message,
		Raw:             raw,
		RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Fields:          fields,
	}
}

func typedFields(envelope hec.Envelope) (*opensplunkv1.TypedObject, error) {
	if !envelope.Fields.Present {
		return nil, nil
	}
	if err := hec.ValidateEnvelopeFields(envelope); err != nil {
		return nil, err
	}
	fields := make([]*opensplunkv1.TypedObjectField, 0, len(envelope.Fields.Value.ObjectValue))
	for _, field := range envelope.Fields.Value.ObjectValue {
		value, err := typedValue(field.Value)
		if err != nil {
			return nil, hec.NewEventError(hec.ErrorIndexedFields, envelope.Number, err)
		}
		fields = append(fields, &opensplunkv1.TypedObjectField{Name: field.Name, Value: value})
	}
	return &opensplunkv1.TypedObject{Fields: fields}, nil
}

func typedValue(value hec.JSONValue) (*opensplunkv1.TypedValue, error) {
	switch value.Kind {
	case hec.JSONNull:
		return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_NullValue{
			NullValue: opensplunkv1.NullValue_NULL_VALUE_NULL,
		}}, nil
	case hec.JSONString:
		return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: value.StringValue}}, nil
	case hec.JSONBoolean:
		return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_BoolValue{BoolValue: value.BooleanValue}}, nil
	case hec.JSONNumber:
		classified, err := hec.ClassifyFieldNumber(value.NumberValue)
		if err != nil {
			return nil, err
		}
		switch classified.Kind {
		case hec.FieldNumberSint64:
			return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: classified.Sint64}}, nil
		case hec.FieldNumberUint64:
			return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Uint64Value{Uint64Value: classified.Uint64}}, nil
		case hec.FieldNumberDecimal:
			return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DecimalValue{
				DecimalValue: &opensplunkv1.DecimalValue{Value: classified.Decimal},
			}}, nil
		default:
			return nil, errors.New("HEC field number classification is invalid")
		}
	case hec.JSONArray:
		values := make([]*opensplunkv1.TypedValue, 0, len(value.ArrayValue))
		for _, item := range value.ArrayValue {
			converted, err := typedValue(item)
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{
			ListValue: &opensplunkv1.TypedValueList{Values: values},
		}}, nil
	default:
		return nil, errors.New("HEC field value is unsupported")
	}
}

func admissionRequest(
	context RequestContext,
	events []*opensplunkv1.LogEvent,
) (ingest.AdmissionRequest, error) {
	if len(events) == 0 || len(events) > math.MaxUint32 {
		return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorInternal, errors.New("HEC event count is outside durable bounds"))
	}
	admissionEvents := make([]ingest.AdmissionEvent, len(events))
	for index, event := range events {
		size := proto.Size(event)
		if size <= 0 {
			return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorInternal, errors.New("HEC normalized event has invalid size"))
		}
		admissionEvents[index] = ingest.AdmissionEvent{Event: event, UncompressedBytes: uint64(size)}
	}
	digest, err := semanticDigest(context.Channel, context.Authentication.HECProfile.IndexerAcknowledgment, events)
	if err != nil {
		return ingest.AdmissionRequest{}, hec.NewProtocolError(hec.ErrorInternal, err)
	}
	channel := ""
	if context.Authentication.HECProfile.IndexerAcknowledgment {
		channel = string(context.Channel)
	}
	return ingest.AdmissionRequest{
		Authorization: ingest.Authorization{
			SubjectID:            context.Authentication.TokenID,
			TenantID:             context.TenantID,
			TokenRateLimits:      context.Authentication.TokenRateLimits,
			AuthorizedIndexes:    append([]ingest.IndexPolicy(nil), context.Authentication.AuthorizedIndexes...),
			AllowedHostRegexes:   append([]string(nil), context.Authentication.AllowedHostRegexes...),
			AllowedSourceRegexes: append([]string(nil), context.Authentication.AllowedSourceRegexes...),
		},
		Source:  ingest.HECSource(context.Authentication.TokenID),
		BatchID: context.RequestID,
		// BatchSequence is a native-collector wire field. HEC's durable
		// per-source sequence is allocated atomically by visibility staging and
		// retained in hec_requests; the immutable outbox is keyed by BatchID.
		BatchSequence:     1,
		SourceBatchSHA256: digest,
		ReceivedAt:        context.ReceivedAt,
		QuotaEvaluatedAt:  context.ReceivedAt,
		Events:            admissionEvents,
		HECAdmission: &ingest.HECStageAdmission{
			TokenID:               context.Authentication.TokenID,
			TokenVersion:          context.Authentication.TokenVersion,
			RequestID:             context.RequestID,
			AcknowledgmentEnabled: context.Authentication.HECProfile.IndexerAcknowledgment,
			Channel:               channel,
			CreatedAt:             context.ReceivedAt,
		},
	}, nil
}

func semanticDigest(channel hec.Channel, acknowledgment bool, events []*opensplunkv1.LogEvent) ([sha256.Size]byte, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte(semanticDigestDomain))
	if acknowledgment {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	writeDigestPart(hash, []byte(channel))
	marshal := proto.MarshalOptions{Deterministic: true}
	for _, event := range events {
		encoded, err := marshal.Marshal(event)
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("marshal HEC semantic event: %w", err)
		}
		writeDigestPart(hash, encoded)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestPart(destination digestWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write(value)
}
