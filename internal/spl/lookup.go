package spl

// MaximumLookupDefinitionNameBytes bounds the exact unquoted knowledge-object
// name carried by one lookup command. The catalog applies its own canonical
// name validation when resolving this authored reference.
const MaximumLookupDefinitionNameBytes = 255

const (
	// MaximumLookupKeys bounds one exact composite lookup key.
	MaximumLookupKeys = 4
	// MaximumLookupOutputs bounds the enrichment work and public fields written
	// by one lookup stage.
	MaximumLookupOutputs = 16
)

// LookupOutputMode distinguishes replacement from preserve-existing output
// semantics. Requiring an explicit mode keeps authored intent visible at the
// parser-to-planner boundary.
type LookupOutputMode uint8

const (
	LookupOutputModeInvalid LookupOutputMode = iota
	LookupOutputModeOverwrite
	LookupOutputModePreserveExisting
)

// LookupKeyMapping maps one exact lookup-table key column to one event field.
// Mappings retain source order because composite-key component order is part
// of lookup identity.
type LookupKeyMapping struct {
	LookupField      string
	LookupFieldRange Range
	EventField       string
	EventFieldRange  Range
	Range            Range
}

// LookupOutputMapping maps one exact lookup-table output column to one event
// field. EventField defaults to LookupField when AS is omitted.
type LookupOutputMapping struct {
	LookupField      string
	LookupFieldRange Range
	EventField       string
	EventFieldRange  Range
	Range            Range
}

// LookupCommand enriches each input row from one visible lookup definition.
// DefinitionName is an unresolved authored object name, never an asset path,
// tenant identifier, object identifier, version, or runtime row container.
type LookupCommand struct {
	DefinitionName  string
	DefinitionRange Range
	Keys            []LookupKeyMapping
	Outputs         []LookupOutputMapping
	OutputMode      LookupOutputMode
	OutputModeRange Range
	Range           Range
}

func (*LookupCommand) command()             {}
func (*LookupCommand) Name() string         { return "lookup" }
func (c *LookupCommand) SourceRange() Range { return c.Range }
