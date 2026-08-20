package knowledgecatalog

import opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"

const dependencyFixtureInputField = "dependency_input"

func dependencyExtractionDefinition(
	appID, name string,
	scope SharingScope,
	description *string,
	hostPattern, outputField string,
) *opensplunk.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunk.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunk.FieldExtractionDefinition_Json{
				Json: &opensplunk.JsonFieldExtractionDefinition{
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
) *opensplunk.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunk.FieldAliasDefinition{
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
) *opensplunk.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, description, hostPattern)
	definition.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{
		CalculatedField: &opensplunk.CalculatedFieldDefinition{
			DestinationField: destinationField,
			Expression:       expression,
		},
	}
	return definition
}
