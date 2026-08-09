package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const updateProtobufRouteFixturesEnvironment = "UPDATE_PROTOBUF_ROUTE_FIXTURES"

type protobufHTTPRouteContractFixture struct {
	Version           int                               `json:"version"`
	FutureFieldNumber int32                             `json:"futureFieldNumber"`
	Routes            []protobufHTTPRouteContractRecord `json:"routes"`
}

type protobufHTTPRouteContractRecord struct {
	Path               string `json:"path"`
	RequestType        string `json:"requestType"`
	ResponseType       string `json:"responseType"`
	RequestKnownWire   string `json:"requestKnownWire,omitempty"`
	RequestFutureWire  string `json:"requestFutureWire,omitempty"`
	ResponseKnownWire  string `json:"responseKnownWire,omitempty"`
	ResponseFutureWire string `json:"responseFutureWire,omitempty"`
}

type protobufHTTPRouteSignature struct {
	RequestType  string
	ResponseType string
}

func TestEveryProtobufHTTPRouteHasCrossRuntimeForwardCompatibility(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "testdata", "protobuf-http-route-contracts.json")
	encoded, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read route fixture: %v", err)
	}
	var fixture protobufHTTPRouteContractFixture
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatalf("decode route fixture: %v", err)
	}
	if fixture.Version != 1 || len(fixture.Routes) != 57 {
		t.Fatalf("route fixture version/count = %d/%d, want 1/57", fixture.Version, len(fixture.Routes))
	}
	assertProtobufRouteFixtureInventory(t, fixture.Routes)
	futureFieldNumber := protowire.Number(fixture.FutureFieldNumber)
	if futureFieldNumber <= 0 || futureFieldNumber > protowire.MaxValidNumber {
		t.Fatalf("future field number = %d", futureFieldNumber)
	}

	update := os.Getenv(updateProtobufRouteFixturesEnvironment) == "1"
	seenPaths := make(map[string]struct{}, len(fixture.Routes))
	for index := range fixture.Routes {
		route := &fixture.Routes[index]
		if _, duplicate := seenPaths[route.Path]; duplicate {
			t.Fatalf("duplicate route fixture %q", route.Path)
		}
		seenPaths[route.Path] = struct{}{}

		t.Run(route.Path, func(t *testing.T) {
			requestKnown, requestFuture := protobufRouteFixtureWire(
				t,
				route.RequestType,
				route.Path+":request",
				futureFieldNumber,
			)
			responseKnown, responseFuture := protobufRouteFixtureWire(
				t,
				route.ResponseType,
				route.Path+":response",
				futureFieldNumber,
			)
			if update {
				route.RequestKnownWire = base64.StdEncoding.EncodeToString(requestKnown)
				route.RequestFutureWire = base64.StdEncoding.EncodeToString(requestFuture)
				route.ResponseKnownWire = base64.StdEncoding.EncodeToString(responseKnown)
				route.ResponseFutureWire = base64.StdEncoding.EncodeToString(responseFuture)
				return
			}

			assertProtobufRouteFixtureBytes(t, "request known", route.RequestKnownWire, requestKnown)
			assertProtobufRouteFixtureBytes(t, "request future", route.RequestFutureWire, requestFuture)
			assertProtobufRouteFixtureBytes(t, "response known", route.ResponseKnownWire, responseKnown)
			assertProtobufRouteFixtureBytes(t, "response future", route.ResponseFutureWire, responseFuture)
			assertGoRequestAcceptsFutureWire(t, route.RequestType, requestKnown, requestFuture)
			assertGoResponseAcceptsFutureWire(t, route.ResponseType, responseKnown, responseFuture)
		})
	}

	if update {
		updated, err := json.MarshalIndent(&fixture, "", "  ")
		if err != nil {
			t.Fatalf("encode updated route fixture: %v", err)
		}
		updated = append(updated, '\n')
		if err := os.WriteFile(fixturePath, updated, 0o600); err != nil {
			t.Fatalf("write updated route fixture: %v", err)
		}
	}
}

func TestProtobufRouteInventoryFailsClosed(t *testing.T) {
	t.Parallel()

	const source = `package server
import (
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)
type ambiguousResponse = pair[*opensplunkv1.GetAppRequest, *opensplunkv1.GetAppResponse]
type nestedAmbiguousResponse = outer[ambiguousResponse, *opensplunkv1.GetSystemBootstrapResponse]
type aliasedRoute = protobufRouteDefinition
type lookalikeRoute struct { definition router.RouteDefinition }
var direct = newForwardCompatibleProtoRoute[*opensplunkv1.GetAppRequest, *opensplunkv1.GetAppResponse](router.RouteConfig[*opensplunkv1.GetAppRequest, *opensplunkv1.GetAppResponse]{})
var indirect = newForwardCompatibleProtoRoute[*opensplunkv1.GetAppRequest, *opensplunkv1.GetAppResponse]
var converted = (protobufRouteDefinition)(lookalikeRoute{definition: router.RouteConfigBase{}})
func bypasses() {
	var wrapped protobufRouteDefinition
	wrapped.definition = helperRoute()
	_ = unwrapProtobufRoutes(nil)
}
func mutatesRoutes(subrouter router.SubRouterConfig) { subrouter.Routes = nil }
`
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "inventory_guard.go", source, 0)
	if err != nil {
		t.Fatalf("parse inventory guard fixture: %v", err)
	}
	direct := protobufDirectRouteRegistrationPositions(file)
	if _, found := indirectProtobufRouteConstructorPosition(
		file,
		direct,
	); !found {
		t.Fatal("indirect protobuf route constructor was not rejected")
	}
	if _, found := protobufRouteDefinitionWritePosition(file); !found {
		t.Fatal("protobuf route definition field write was not rejected")
	}
	if _, found := protobufRouteDefinitionAliasPosition(file); !found {
		t.Fatal("protobuf route definition alias was not rejected")
	}
	if _, found := protobufRouteDefinitionConversionPosition(file); !found {
		t.Fatal("protobuf route definition conversion was not rejected")
	}
	if _, found := protobufSubRouterRoutesSelectorPosition(file); !found {
		t.Fatal("protobuf subrouter Routes mutation was not rejected")
	}
	if _, found := invalidProtobufRouteUnwrapPosition([]*ast.File{file}); !found {
		t.Fatal("indirect protobuf route unwrap was not rejected")
	}
	typeExpressions := make(map[string]ast.Expr)
	collectProtobufRouteDeclarations(file, typeExpressions, make(map[string]ast.Expr))
	if resolved, ok := protobufRouteMessageType(
		ast.NewIdent("nestedAmbiguousResponse"),
		typeExpressions,
		nil,
	); ok {
		t.Fatalf("ambiguous response resolved as %q", resolved)
	}
}

func assertProtobufRouteFixtureInventory(
	t *testing.T,
	fixture []protobufHTTPRouteContractRecord,
) {
	t.Helper()
	registered := registeredProtobufHTTPRoutes(t)
	if len(registered) != len(fixture) {
		t.Fatalf("registered/fixture protobuf route count = %d/%d", len(registered), len(fixture))
	}
	seen := make(map[string]struct{}, len(fixture))
	for _, route := range fixture {
		if _, duplicate := seen[route.Path]; duplicate {
			t.Fatalf("duplicate route fixture %q", route.Path)
		}
		seen[route.Path] = struct{}{}
		got, exists := registered[route.Path]
		if !exists {
			t.Fatalf("fixture route %q is not registered", route.Path)
		}
		want := protobufHTTPRouteSignature{
			RequestType:  route.RequestType,
			ResponseType: route.ResponseType,
		}
		if got != want {
			t.Fatalf("registered route %q types = %+v, want %+v", route.Path, got, want)
		}
	}
	for path := range registered {
		if _, exists := seen[path]; !exists {
			t.Fatalf("registered protobuf route %q is missing from the fixture", path)
		}
	}
}

func registeredProtobufHTTPRoutes(t *testing.T) map[string]protobufHTTPRouteSignature {
	t.Helper()
	fileSet := token.NewFileSet()
	serverFiles, err := parseServerProductionFiles(fileSet)
	if err != nil {
		t.Fatalf("parse server package: %v", err)
	}

	typeExpressions := make(map[string]ast.Expr)
	stringConstants := make(map[string]ast.Expr)
	for _, file := range serverFiles {
		collectProtobufRouteDeclarations(file, typeExpressions, stringConstants)
	}
	assertProtobufRouteConstructionBoundary(t, fileSet, serverFiles)

	registered := make(map[string]protobufHTTPRouteSignature)
	for _, file := range serverFiles {
		directRegistrations := protobufDirectRouteRegistrationPositions(file)
		assertNoIndirectProtobufRouteConstructors(
			t,
			fileSet,
			file,
			directRegistrations,
		)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			typeArguments, genericRoute := protobufTrackedRouteTypeArguments(call.Fun)
			if !genericRoute {
				return true
			}
			position := fileSet.Position(call.Pos())
			if len(typeArguments) < 2 || len(call.Args) != 1 {
				t.Fatalf("invalid protobuf route registration at %s", position)
			}
			config, ok := call.Args[0].(*ast.CompositeLit)
			if !ok {
				t.Fatalf("non-literal protobuf route config at %s", position)
			}
			pathExpression := protobufRouteConfigField(config, "Path")
			path, ok := protobufRouteString(pathExpression, stringConstants, nil)
			if !ok {
				t.Fatalf("unresolved protobuf route path at %s", position)
			}
			sanitizer := protobufRouteConfigField(config, "Sanitizer")
			if protobufRouteFunctionName(sanitizer) != "forwardCompatibleProtoSanitizer" {
				t.Fatalf("protobuf route %q lacks the forward-compatible sanitizer at %s", path, position)
			}
			requestType, ok := protobufRouteMessageType(typeArguments[0], typeExpressions, nil)
			if !ok {
				t.Fatalf("unresolved protobuf request type at %s", position)
			}
			responseType, ok := protobufRouteMessageType(typeArguments[1], typeExpressions, nil)
			if !ok {
				t.Fatalf("unresolved protobuf response type at %s", position)
			}
			fullPath := apiV1PathPrefix + path
			if _, duplicate := registered[fullPath]; duplicate {
				t.Fatalf("duplicate registered protobuf route %q", fullPath)
			}
			registered[fullPath] = protobufHTTPRouteSignature{
				RequestType:  requestType,
				ResponseType: responseType,
			}
			return true
		})
	}
	return registered
}

func parseServerProductionFiles(fileSet *token.FileSet) ([]*ast.File, error) {
	entries, err := os.ReadDir(".")
	if err != nil {
		return nil, err
	}
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		matchesBuild, matchErr := build.Default.MatchFile(".", name)
		if matchErr != nil {
			return nil, fmt.Errorf("match build constraints for %s: %w", name, matchErr)
		}
		if !matchesBuild {
			continue
		}
		file, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		if parseErr != nil {
			return nil, fmt.Errorf("parse %s: %w", name, parseErr)
		}
		if file.Name.Name == "server" {
			files = append(files, file)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("parsed server package is missing")
	}
	return files, nil
}

func collectProtobufRouteDeclarations(
	file *ast.File,
	typeExpressions map[string]ast.Expr,
	stringConstants map[string]ast.Expr,
) {
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, specification := range general.Specs {
			switch typed := specification.(type) {
			case *ast.TypeSpec:
				typeExpressions[typed.Name.Name] = typed.Type
			case *ast.ValueSpec:
				if general.Tok != token.CONST {
					continue
				}
				for index, name := range typed.Names {
					if index < len(typed.Values) {
						stringConstants[name.Name] = typed.Values[index]
					}
				}
			}
		}
	}
}

func protobufRouterImportAliases(file *ast.File) (map[string]struct{}, bool) {
	const routerImportPath = "github.com/Suhaibinator/SRouter/pkg/router"
	aliases := make(map[string]struct{})
	dotImported := false
	for _, specification := range file.Imports {
		importPath, err := strconv.Unquote(specification.Path.Value)
		if err != nil || importPath != routerImportPath {
			continue
		}
		if specification.Name == nil {
			aliases["router"] = struct{}{}
			continue
		}
		switch specification.Name.Name {
		case ".":
			dotImported = true
		case "_":
		default:
			aliases[specification.Name.Name] = struct{}{}
		}
	}
	return aliases, dotImported
}

func assertProtobufRouteConstructionBoundary(
	t *testing.T,
	fileSet *token.FileSet,
	files []*ast.File,
) {
	t.Helper()
	for _, file := range files {
		if position, found := protobufRouteDefinitionWritePosition(file); found {
			t.Fatalf(
				"protobuf route definition is mutated outside its constructor at %s",
				fileSet.Position(position),
			)
		}
		if position, found := protobufRouteDefinitionAliasPosition(file); found {
			t.Fatalf(
				"protobuf route definition is aliased outside its constructor at %s",
				fileSet.Position(position),
			)
		}
		if position, found := protobufRouteDefinitionConversionPosition(file); found {
			t.Fatalf(
				"protobuf route definition is populated by conversion at %s",
				fileSet.Position(position),
			)
		}
		if position, found := protobufSubRouterRoutesSelectorPosition(file); found {
			t.Fatalf(
				"protobuf subrouter Routes escapes its inline construction at %s",
				fileSet.Position(position),
			)
		}
	}
	if position, found := invalidProtobufRouteUnwrapPosition(files); found {
		t.Fatalf(
			"protobuf routes are not unwrapped directly into SRouter at %s",
			fileSet.Position(position),
		)
	}
	if directProtobufRouteUnwrapCount(files) != 1 {
		t.Fatalf(
			"protobuf routes must be unwrapped directly into SRouter exactly once",
		)
	}
	allowedSRouter := make(map[token.Pos]struct{})
	allowedWrappers := make(map[token.Pos]struct{})
	for _, file := range files {
		routerAliases, dotImported := protobufRouterImportAliases(file)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Name.Name != "newForwardCompatibleProtoRoute" || function.Body == nil {
				continue
			}
			directSRouter := make(map[token.Pos]struct{})
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, callOK := node.(*ast.CallExpr)
				if !callOK {
					return true
				}
				if _, constructor := srouterGenericRouteTypeArguments(
					call.Fun,
					routerAliases,
					dotImported,
				); constructor {
					directSRouter[call.Fun.Pos()] = struct{}{}
				}
				return true
			})
			ast.Inspect(function.Body, func(node ast.Node) bool {
				expression, expressionOK := node.(ast.Expr)
				if expressionOK {
					if _, constructor := srouterGenericRouteTypeArguments(
						expression,
						routerAliases,
						dotImported,
					); constructor {
						if _, direct := directSRouter[expression.Pos()]; !direct {
							t.Fatalf(
								"SRouter constructor is indirect inside the local boundary at %s",
								fileSet.Position(expression.Pos()),
							)
						}
						allowedSRouter[expression.Pos()] = struct{}{}
					}
				}
				literal, literalOK := node.(*ast.CompositeLit)
				if literalOK && protobufRouteDefinitionLiteral(literal) {
					allowedWrappers[literal.Pos()] = struct{}{}
				}
				return true
			})
		}
	}

	seenSRouter := 0
	seenWrappers := 0
	for _, file := range files {
		routerAliases, dotImported := protobufRouterImportAliases(file)
		ast.Inspect(file, func(node ast.Node) bool {
			expression, expressionOK := node.(ast.Expr)
			if expressionOK {
				if _, constructor := srouterGenericRouteTypeArguments(
					expression,
					routerAliases,
					dotImported,
				); constructor {
					if _, allowed := allowedSRouter[expression.Pos()]; !allowed {
						t.Fatalf(
							"SRouter protobuf constructor bypasses the local boundary at %s",
							fileSet.Position(expression.Pos()),
						)
					}
					seenSRouter++
				}
			}
			literal, literalOK := node.(*ast.CompositeLit)
			if literalOK && protobufRouteDefinitionLiteral(literal) {
				if _, allowed := allowedWrappers[literal.Pos()]; !allowed {
					t.Fatalf(
						"protobuf route wrapper bypasses its constructor at %s",
						fileSet.Position(literal.Pos()),
					)
				}
				seenWrappers++
			}
			return true
		})
	}
	if seenSRouter != 1 || seenWrappers != 1 {
		t.Fatalf(
			"protobuf construction boundary count = SRouter %d/wrapper %d, want 1/1",
			seenSRouter,
			seenWrappers,
		)
	}
}

func protobufRouteDefinitionLiteral(literal *ast.CompositeLit) bool {
	identifier, ok := literal.Type.(*ast.Ident)
	return ok && identifier.Name == "protobufRouteDefinition"
}

func protobufRouteDefinitionWritePosition(file *ast.File) (token.Pos, bool) {
	var write token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		if write.IsValid() {
			return false
		}
		assignment, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, expression := range assignment.Lhs {
			selector, selectorOK := expression.(*ast.SelectorExpr)
			if selectorOK && selector.Sel.Name == "definition" {
				write = selector.Pos()
				return false
			}
		}
		return true
	})
	return write, write.IsValid()
}

func protobufRouteDefinitionAliasPosition(file *ast.File) (token.Pos, bool) {
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpecification, typeOK := specification.(*ast.TypeSpec)
			if !typeOK {
				continue
			}
			underlying, identifierOK := typeSpecification.Type.(*ast.Ident)
			if identifierOK && underlying.Name == "protobufRouteDefinition" {
				return typeSpecification.Pos(), true
			}
		}
	}
	return token.NoPos, false
}

func protobufRouteDefinitionConversionPosition(file *ast.File) (token.Pos, bool) {
	var conversion token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		if conversion.IsValid() {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if protobufRouteFunctionName(call.Fun) == "protobufRouteDefinition" {
			conversion = call.Pos()
			return false
		}
		return true
	})
	return conversion, conversion.IsValid()
}

func protobufSubRouterRoutesSelectorPosition(file *ast.File) (token.Pos, bool) {
	var selectorPosition token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		if selectorPosition.IsValid() {
			return false
		}
		selector, ok := node.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Routes" {
			selectorPosition = selector.Pos()
			return false
		}
		return true
	})
	return selectorPosition, selectorPosition.IsValid()
}

func invalidProtobufRouteUnwrapPosition(files []*ast.File) (token.Pos, bool) {
	allowed, _ := directProtobufRouteUnwrapPositions(files)
	for _, file := range files {
		var invalid token.Pos
		ast.Inspect(file, func(node ast.Node) bool {
			if invalid.IsValid() {
				return false
			}
			identifier, ok := node.(*ast.Ident)
			if !ok || identifier.Name != "unwrapProtobufRoutes" {
				return true
			}
			if _, direct := allowed[identifier.Pos()]; !direct {
				invalid = identifier.Pos()
				return false
			}
			return true
		})
		if invalid.IsValid() {
			return invalid, true
		}
	}
	return token.NoPos, false
}

func directProtobufRouteUnwrapCount(files []*ast.File) int {
	_, count := directProtobufRouteUnwrapPositions(files)
	return count
}

func directProtobufRouteUnwrapPositions(files []*ast.File) (map[token.Pos]struct{}, int) {
	allowed := make(map[token.Pos]struct{})
	count := 0
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Name.Name == "unwrapProtobufRoutes" {
				allowed[function.Name.Pos()] = struct{}{}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			keyed, ok := node.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, keyOK := keyed.Key.(*ast.Ident)
			call, callOK := keyed.Value.(*ast.CallExpr)
			if !keyOK || key.Name != "Routes" || !callOK {
				return true
			}
			function, functionOK := call.Fun.(*ast.Ident)
			if functionOK && function.Name == "unwrapProtobufRoutes" {
				allowed[function.Pos()] = struct{}{}
				count++
			}
			return true
		})
	}
	return allowed, count
}

func srouterGenericRouteTypeArguments(
	expression ast.Expr,
	routerAliases map[string]struct{},
	dotImported bool,
) ([]ast.Expr, bool) {
	var base ast.Expr
	var arguments []ast.Expr
	switch indexed := expression.(type) {
	case *ast.IndexListExpr:
		base, arguments = indexed.X, indexed.Indices
	case *ast.IndexExpr:
		base, arguments = indexed.X, []ast.Expr{indexed.Index}
	default:
		return nil, false
	}
	selector, ok := base.(*ast.SelectorExpr)
	if ok {
		packageName, packageOK := selector.X.(*ast.Ident)
		if !packageOK {
			return nil, false
		}
		_, routerPackage := routerAliases[packageName.Name]
		return arguments, routerPackage && selector.Sel.Name == "NewGenericRouteDefinition"
	}
	identifier, ok := base.(*ast.Ident)
	return arguments, ok && dotImported && identifier.Name == "NewGenericRouteDefinition"
}

func protobufDirectRouteRegistrationPositions(
	file *ast.File,
) map[token.Pos]struct{} {
	positions := make(map[token.Pos]struct{})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if _, genericRoute := protobufTrackedRouteTypeArguments(call.Fun); genericRoute {
			positions[call.Fun.Pos()] = struct{}{}
		}
		return true
	})
	return positions
}

func assertNoIndirectProtobufRouteConstructors(
	t *testing.T,
	fileSet *token.FileSet,
	file *ast.File,
	directRegistrations map[token.Pos]struct{},
) {
	t.Helper()
	position, found := indirectProtobufRouteConstructorPosition(
		file,
		directRegistrations,
	)
	if found {
		t.Fatalf(
			"protobuf route constructor must be called directly at %s",
			fileSet.Position(position),
		)
	}
}

func indirectProtobufRouteConstructorPosition(
	file *ast.File,
	directRegistrations map[token.Pos]struct{},
) (token.Pos, bool) {
	var indirect token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		if indirect.IsValid() {
			return false
		}
		expression, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		if _, genericRoute := protobufTrackedRouteTypeArguments(expression); !genericRoute {
			return true
		}
		if _, direct := directRegistrations[expression.Pos()]; !direct {
			indirect = expression.Pos()
			return false
		}
		return true
	})
	return indirect, indirect.IsValid()
}

func protobufTrackedRouteTypeArguments(expression ast.Expr) ([]ast.Expr, bool) {
	var base ast.Expr
	var arguments []ast.Expr
	switch indexed := expression.(type) {
	case *ast.IndexListExpr:
		base, arguments = indexed.X, indexed.Indices
	case *ast.IndexExpr:
		base, arguments = indexed.X, []ast.Expr{indexed.Index}
	default:
		return nil, false
	}
	identifier, ok := base.(*ast.Ident)
	return arguments, ok && identifier.Name == "newForwardCompatibleProtoRoute"
}

func protobufRouteConfigField(config *ast.CompositeLit, name string) ast.Expr {
	for _, element := range config.Elts {
		keyed, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := keyed.Key.(*ast.Ident)
		if ok && key.Name == name {
			return keyed.Value
		}
	}
	return nil
}

func protobufRouteFunctionName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr:
		return protobufRouteFunctionName(typed.X)
	case *ast.IndexListExpr:
		return protobufRouteFunctionName(typed.X)
	case *ast.ParenExpr:
		return protobufRouteFunctionName(typed.X)
	default:
		return ""
	}
}

func protobufRouteString(
	expression ast.Expr,
	constants map[string]ast.Expr,
	visiting map[string]struct{},
) (string, bool) {
	switch typed := expression.(type) {
	case *ast.BasicLit:
		if typed.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(typed.Value)
		return value, err == nil
	case *ast.Ident:
		if visiting == nil {
			visiting = make(map[string]struct{})
		}
		if _, cycle := visiting[typed.Name]; cycle {
			return "", false
		}
		value, exists := constants[typed.Name]
		if !exists {
			return "", false
		}
		visiting[typed.Name] = struct{}{}
		resolved, ok := protobufRouteString(value, constants, visiting)
		delete(visiting, typed.Name)
		return resolved, ok
	case *ast.BinaryExpr:
		if typed.Op != token.ADD {
			return "", false
		}
		left, leftOK := protobufRouteString(typed.X, constants, visiting)
		right, rightOK := protobufRouteString(typed.Y, constants, visiting)
		return left + right, leftOK && rightOK
	case *ast.ParenExpr:
		return protobufRouteString(typed.X, constants, visiting)
	default:
		return "", false
	}
}

func protobufRouteMessageType(
	expression ast.Expr,
	types map[string]ast.Expr,
	visiting map[string]struct{},
) (string, bool) {
	resolved, candidates := protobufRouteMessageTypeCandidates(expression, types, visiting)
	return resolved, candidates == 1
}

func protobufRouteMessageTypeCandidates(
	expression ast.Expr,
	types map[string]ast.Expr,
	visiting map[string]struct{},
) (string, int) {
	switch typed := expression.(type) {
	case *ast.StarExpr:
		return protobufRouteMessageTypeCandidates(typed.X, types, visiting)
	case *ast.SelectorExpr:
		packageName, ok := typed.X.(*ast.Ident)
		if ok && packageName.Name == "opensplunkv1" {
			return typed.Sel.Name, 1
		}
		return "", 0
	case *ast.Ident:
		if visiting == nil {
			visiting = make(map[string]struct{})
		}
		if _, cycle := visiting[typed.Name]; cycle {
			return "", 0
		}
		underlying, exists := types[typed.Name]
		if !exists {
			return "", 0
		}
		visiting[typed.Name] = struct{}{}
		resolved, candidates := protobufRouteMessageTypeCandidates(underlying, types, visiting)
		delete(visiting, typed.Name)
		return resolved, candidates
	case *ast.IndexExpr:
		return protobufRouteMessageTypeCandidates(typed.Index, types, visiting)
	case *ast.IndexListExpr:
		resolved := ""
		candidates := 0
		for _, argument := range typed.Indices {
			candidate, count := protobufRouteMessageTypeCandidates(argument, types, visiting)
			if count == 0 {
				continue
			}
			candidates += count
			if candidates > 1 {
				return "", candidates
			}
			resolved = candidate
		}
		return resolved, candidates
	case *ast.StructType:
		for _, field := range typed.Fields.List {
			if len(field.Names) == 1 && field.Names[0].Name == "message" {
				return protobufRouteMessageTypeCandidates(field.Type, types, visiting)
			}
		}
		return "", 0
	case *ast.ParenExpr:
		return protobufRouteMessageTypeCandidates(typed.X, types, visiting)
	default:
		return "", 0
	}
}

func protobufRouteFixtureWire(
	t *testing.T,
	typeName string,
	seed string,
	futureFieldNumber protowire.Number,
) ([]byte, []byte) {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName("open_splunk.v1." + typeName),
	)
	if err != nil {
		t.Fatalf("find %s: %v", typeName, err)
	}
	message := messageType.New()
	if message.Descriptor().Fields().ByNumber(futureFieldNumber) != nil {
		t.Fatalf("%s already defines future field %d", typeName, futureFieldNumber)
	}
	if err := populateProtobufRouteFixture(message, seed, 0); err != nil {
		t.Fatalf("populate %s: %v", typeName, err)
	}
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message.Interface())
	if err != nil {
		t.Fatalf("marshal %s: %v", typeName, err)
	}
	if len(known) == 0 {
		t.Fatalf("%s fixture has no non-default known field", typeName)
	}
	future := bytes.Clone(known)
	future = protowire.AppendString(
		protowire.AppendTag(future, futureFieldNumber, protowire.BytesType),
		"future:"+seed,
	)
	return known, future
}

func populateProtobufRouteFixture(
	message protoreflect.Message,
	seed string,
	depth int,
) error {
	if depth >= 32 {
		return errors.New("fixture message nesting exceeds 32 levels")
	}
	fields := message.Descriptor().Fields()
	if fields.Len() == 0 {
		return errors.New("fixture message has no fields")
	}
	field := fields.Get(0)
	switch {
	case field.IsMap():
		return populateProtobufRouteFixtureMap(message.Mutable(field).Map(), field, seed, depth)
	case field.IsList():
		return populateProtobufRouteFixtureList(message.Mutable(field).List(), field, seed, depth)
	case field.Message() != nil:
		return populateProtobufRouteFixture(message.Mutable(field).Message(), seed, depth+1)
	default:
		value, err := protobufRouteFixtureScalar(field, seed)
		if err != nil {
			return err
		}
		message.Set(field, value)
		return nil
	}
}

func populateProtobufRouteFixtureList(
	list protoreflect.List,
	field protoreflect.FieldDescriptor,
	seed string,
	depth int,
) error {
	element := list.NewElement()
	if field.Message() != nil {
		if err := populateProtobufRouteFixture(element.Message(), seed, depth+1); err != nil {
			return err
		}
	} else {
		value, err := protobufRouteFixtureScalar(field, seed)
		if err != nil {
			return err
		}
		element = value
	}
	list.Append(element)
	return nil
}

func populateProtobufRouteFixtureMap(
	protobufMap protoreflect.Map,
	field protoreflect.FieldDescriptor,
	seed string,
	depth int,
) error {
	key, err := protobufRouteFixtureScalar(field.MapKey(), seed+":key")
	if err != nil {
		return err
	}
	valueDescriptor := field.MapValue()
	value := protobufMap.NewValue()
	if valueDescriptor.Message() != nil {
		if err := populateProtobufRouteFixture(value.Message(), seed+":value", depth+1); err != nil {
			return err
		}
	} else {
		value, err = protobufRouteFixtureScalar(valueDescriptor, seed+":value")
		if err != nil {
			return err
		}
	}
	protobufMap.Set(key.MapKey(), value)
	return nil
}

func protobufRouteFixtureScalar(
	field protoreflect.FieldDescriptor,
	seed string,
) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true), nil
	case protoreflect.EnumKind:
		values := field.Enum().Values()
		for index := 0; index < values.Len(); index++ {
			if number := values.Get(index).Number(); number != 0 {
				return protoreflect.ValueOfEnum(number), nil
			}
		}
		return protoreflect.Value{}, errors.New("fixture enum has no nonzero value")
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.25), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.25), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(seed), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(seed)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported fixture field kind %s", field.Kind())
	}
}

func assertProtobufRouteFixtureBytes(
	t *testing.T,
	name string,
	encoded string,
	want []byte,
) {
	t.Helper()
	got, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode %s fixture: %v", name, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s fixture is stale; run %s=1 go test ./internal/server -run TestEveryProtobufHTTPRouteHasCrossRuntimeForwardCompatibility", name, updateProtobufRouteFixturesEnvironment)
	}
}

func assertGoRequestAcceptsFutureWire(
	t *testing.T,
	typeName string,
	wantKnown []byte,
	future []byte,
) {
	t.Helper()
	message := newProtobufRouteFixtureMessage(t, typeName)
	if err := proto.Unmarshal(future, message); err != nil {
		t.Fatalf("unmarshal future request: %v", err)
	}
	if len(message.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("future request field was not decoded as unknown")
	}
	if _, err := forwardCompatibleProtoSanitizer(message); err != nil {
		t.Fatalf("sanitize future request: %v", err)
	}
	assertProtobufRouteKnownWire(t, message, wantKnown)
}

func assertGoResponseAcceptsFutureWire(
	t *testing.T,
	typeName string,
	wantKnown []byte,
	future []byte,
) {
	t.Helper()
	message := newProtobufRouteFixtureMessage(t, typeName)
	if err := proto.Unmarshal(future, message); err != nil {
		t.Fatalf("unmarshal future response: %v", err)
	}
	if len(message.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("future response field was not retained")
	}
	message.ProtoReflect().SetUnknown(nil)
	assertProtobufRouteKnownWire(t, message, wantKnown)
}

func newProtobufRouteFixtureMessage(t *testing.T, typeName string) proto.Message {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName("open_splunk.v1." + typeName),
	)
	if err != nil {
		t.Fatalf("find %s: %v", typeName, err)
	}
	return messageType.New().Interface()
}

func assertProtobufRouteKnownWire(t *testing.T, message proto.Message, want []byte) {
	t.Helper()
	got, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal known fields: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("known fields changed: got %x, want %x", got, want)
	}
}
