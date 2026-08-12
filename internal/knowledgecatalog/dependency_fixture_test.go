package knowledgecatalog

import opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"

const dependencyFixtureInputField = "dependency_input"

func dependencyExtractionDefinition(
	appID, name string,
	scope SharingScope,
	description *string,
	hostPattern, outputField string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
				Json: &opensplunkv1.JsonFieldExtractionDefinition{
					Path:        "payload.value",
					OutputField: outputField,
				},
			},
		},
	}
	return definition
}

func dependencyAliasDefinition(
	appID, name string,
	scope SharingScope,
	description *string,
	hostPattern, sourceField, destinationField string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField:      sourceField,
			DestinationField: destinationField,
		},
	}
	return definition
}

func dependencyCalculatedDefinition(
	appID, name string,
	scope SharingScope,
	description *string,
	hostPattern, expression, destinationField string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
		CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: destinationField,
			Expression:       expression,
		},
	}
	return definition
}
