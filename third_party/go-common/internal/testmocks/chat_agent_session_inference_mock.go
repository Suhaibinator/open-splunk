package testmocks

import (
	"context"
	"errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (m *MockPersistenceHandler) DrainRetargetedChatAgentSessions(ctx context.Context, sessionIDs []uint64) ([]persistence.ChatAgentSessionResume, error) {
	if !m.hasExpectation("DrainRetargetedChatAgentSessions") {
		return nil, errors.New("mock retargeted session drain requires an explicit expectation")
	}
	result := m.Called(ctx, sessionIDs)
	if result.Get(0) == nil {
		return nil, result.Error(1)
	}
	return result.Get(0).([]persistence.ChatAgentSessionResume), result.Error(1)
}

func (m *MockPersistenceHandler) InitializeChatAgentSessionInferencePin(ctx context.Context, sessionID uint64, pin cpm.ChatAgentSessionInferencePin) (*cpm.ChatAgentSession, error) {
	if !m.hasExpectation("InitializeChatAgentSessionInferencePin") {
		return nil, errors.New("mock inference pin initialization requires an explicit expectation")
	}
	result := m.Called(ctx, sessionID, pin)
	if result.Get(0) == nil {
		return nil, result.Error(1)
	}
	return result.Get(0).(*cpm.ChatAgentSession), result.Error(1)
}

func (m *MockPersistenceHandler) BlockChatAgentSessionInference(ctx context.Context, sessionID, expectedPinVersion uint64, reason proto_models.InferenceBlockReason) error {
	if !m.hasExpectation("BlockChatAgentSessionInference") {
		return errors.New("mock inference block requires an explicit expectation")
	}
	return m.Called(ctx, sessionID, expectedPinVersion, reason).Error(0)
}

func (m *MockPersistenceHandler) GetChatAgentSessionsByIDs(ctx context.Context, organizationID int64, sessionIDs []uint64) ([]cpm.ChatAgentSession, error) {
	if !m.hasExpectation("GetChatAgentSessionsByIDs") {
		return nil, errors.New("mock inference session batch requires an explicit expectation")
	}
	result := m.Called(ctx, organizationID, sessionIDs)
	if result.Get(0) == nil {
		return nil, result.Error(1)
	}
	return result.Get(0).([]cpm.ChatAgentSession), result.Error(1)
}

func (m *MockPersistenceHandler) BatchRetargetChatAgentSessionInferencePins(ctx context.Context, organizationID int64, sessionIDs []uint64, expectedPinVersions map[uint64]uint64, pin cpm.ChatAgentSessionInferencePin, actorUserID uint64) ([]cpm.ChatAgentSession, error) {
	if !m.hasExpectation("BatchRetargetChatAgentSessionInferencePins") {
		return nil, errors.New("mock inference batch retarget requires an explicit expectation")
	}
	result := m.Called(ctx, organizationID, sessionIDs, expectedPinVersions, pin, actorUserID)
	if result.Get(0) == nil {
		return nil, result.Error(1)
	}
	return result.Get(0).([]cpm.ChatAgentSession), result.Error(1)
}
