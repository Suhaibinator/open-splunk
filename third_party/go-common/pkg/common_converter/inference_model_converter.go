package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// PersistenceInferenceModelToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceInferenceModelToService(
	model *common_persistence_models.InferenceModel,
) *common_service_models.InferenceModel {
	if model == nil {
		return nil
	}
	return &common_service_models.InferenceModel{
		ID:              model.ID,
		ProviderID:      model.ProviderID,
		ProviderModelID: model.ProviderModelID,
		DisplayName:     model.DisplayName,
		Active:          model.Active,
	}
}

// ServiceInferenceModelToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceInferenceModelToProto(
	model *common_service_models.InferenceModel,
) *proto_models.InferenceModel {
	if model == nil {
		return nil
	}
	return &proto_models.InferenceModel{
		Id:              model.ID,
		ProviderId:      model.ProviderID,
		ProviderModelId: model.ProviderModelID,
		DisplayName:     model.DisplayName,
		Active:          model.Active,
	}
}

// ServiceInferenceModelToAdminProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceInferenceModelToAdminProto(
	model *common_service_models.InferenceModel,
) *proto_models.AdminInferenceModel {
	if model == nil {
		return nil
	}
	return &proto_models.AdminInferenceModel{
		InferenceModel: h.ServiceInferenceModelToProto(model),
	}
}
