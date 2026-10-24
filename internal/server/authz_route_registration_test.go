package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authz"
)

// routeHelperMethods names every route-config helper in route_builder.go and
// the one method each registers. A registrar that builds a route any other
// way fails TestEverySRouterRegistrationIsInTheAuthzTable, so a new helper
// wrapper cannot add a route this inventory does not see.
var routeHelperMethods = map[string]string{
	"postRoute":           http.MethodPost,
	"sizedPostRoute":      http.MethodPost,
	"protoPostRoute":      http.MethodPost,
	"sizedProtoPostRoute": http.MethodPost,
	"rawGetRoute":         http.MethodGet,
}

type sourceRouteInventory struct {
	fset      *token.FileSet
	constants map[string]ast.Expr
	routes    map[string]string
	problems  []string
}

// inventorySourceRoutes reads this package's non-test sources and returns the
// path and method of every route the SRouter registrars can register, whatever
// feature gate guards them. It does not depend on browserAPIRoutes, so it
// checks that hand-maintained list against the registrations themselves.
func inventorySourceRoutes(t *testing.T) *sourceRouteInventory {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	inventory := &sourceRouteInventory{
		fset:      token.NewFileSet(),
		constants: map[string]ast.Expr{},
		routes:    map[string]string{},
	}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(inventory.fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, spec := range general.Specs {
				value := spec.(*ast.ValueSpec)
				for index, name := range value.Names {
					if index < len(value.Values) {
						inventory.constants[name.Name] = value.Values[index]
					}
				}
			}
		}
	}
	if len(files) == 0 {
		t.Fatal("no package sources found")
	}
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if isFunction {
				if _, isHelper := routeHelperMethods[function.Name.Name]; isHelper {
					// Helpers forward their path parameter to one another.
					continue
				}
			}
			ast.Inspect(declaration, inventory.inspect)
		}
	}
	return inventory
}

func (inventory *sourceRouteInventory) problem(node ast.Node, message string) {
	inventory.problems = append(inventory.problems, inventory.fset.Position(node.Pos()).String()+": "+message)
}

func (inventory *sourceRouteInventory) inspect(node ast.Node) bool {
	switch value := node.(type) {
	case *ast.CompositeLit:
		if routeConfigType(value.Type) {
			inventory.problem(value, "route config literal outside the route helpers: "+types.ExprString(value.Type))
		}
	case *ast.CallExpr:
		selector, isSelector := value.Fun.(*ast.SelectorExpr)
		switch {
		case isSelector && selector.Sel.Name == "Route":
			if value.Ellipsis.IsValid() {
				inventory.problem(value, "Route called with a spread route list")
			}
			for _, argument := range value.Args {
				if _, ok := inventory.helperCall(argument); !ok {
					inventory.problem(argument, "route registered without a known helper: "+types.ExprString(argument))
				}
			}
		case isSelector && selector.Sel.Name == "Group":
			for _, argument := range value.Args {
				prefix, ok := inventory.constantString(argument, 0)
				if !ok || prefix != apiPathPrefix && prefix != "/" {
					inventory.problem(argument, "route group prefix is not /api or /: "+types.ExprString(argument))
				}
			}
		}
		if helper, ok := inventory.helperCall(value); ok {
			if len(value.Args) == 0 {
				inventory.problem(value, "route helper called without a path")
				return true
			}
			path, constant := inventory.constantString(value.Args[0], 0)
			if !constant || !strings.HasPrefix(path, "/") || strings.Contains(path, "//") {
				inventory.problem(value, "route path is not a constant rooted path: "+types.ExprString(value.Args[0]))
				return true
			}
			full := apiPathPrefix + path
			if method, seen := inventory.routes[full]; seen && method != routeHelperMethods[helper] {
				inventory.problem(value, "route registered with two methods: "+full)
			}
			inventory.routes[full] = routeHelperMethods[helper]
		}
	}
	return true
}

// helperCall reports which route helper expression calls, looking through
// explicit generic instantiation such as protoPostRoute[Request, Response].
func (inventory *sourceRouteInventory) helperCall(expression ast.Expr) (string, bool) {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	function := call.Fun
	switch instantiated := function.(type) {
	case *ast.IndexExpr:
		function = instantiated.X
	case *ast.IndexListExpr:
		function = instantiated.X
	}
	identifier, ok := function.(*ast.Ident)
	if !ok {
		return "", false
	}
	_, known := routeHelperMethods[identifier.Name]
	return identifier.Name, known
}

func (inventory *sourceRouteInventory) constantString(expression ast.Expr, depth int) (string, bool) {
	if depth > 16 {
		return "", false
	}
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		unquoted, err := strconv.Unquote(value.Value)
		return unquoted, err == nil
	case *ast.Ident:
		definition, ok := inventory.constants[value.Name]
		if !ok {
			return "", false
		}
		return inventory.constantString(definition, depth+1)
	case *ast.ParenExpr:
		return inventory.constantString(value.X, depth+1)
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := inventory.constantString(value.X, depth+1)
		right, rightOK := inventory.constantString(value.Y, depth+1)
		return left + right, leftOK && rightOK
	default:
		return "", false
	}
}

func routeConfigType(expression ast.Expr) bool {
	switch instantiated := expression.(type) {
	case *ast.IndexExpr:
		expression = instantiated.X
	case *ast.IndexListExpr:
		expression = instantiated.X
	}
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return strings.HasPrefix(selector.Sel.Name, "RouteConfig")
}

// TestEverySRouterRegistrationIsInTheAuthzTable inventories the registrars'
// source, independent of browserAPIRoutes and of which dependencies a test
// handler wires, and requires it to match the authz table exactly.
func TestEverySRouterRegistrationIsInTheAuthzTable(t *testing.T) {
	t.Parallel()

	inventory := inventorySourceRoutes(t)
	for _, problem := range inventory.problems {
		t.Error(problem)
	}
	paths := make([]string, 0, len(inventory.routes))
	for path := range inventory.routes {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		requirement, ok := authz.RouteRequirement(path)
		if !ok {
			t.Errorf("SRouter registers %s %s, which the authz table does not name", inventory.routes[path], path)
			continue
		}
		if requirement.Method != inventory.routes[path] {
			t.Errorf("SRouter registers %s %s; the authz table says %s", inventory.routes[path], path, requirement.Method)
		}
	}
	for _, path := range authz.Routes() {
		if _, ok := inventory.routes[path]; !ok {
			t.Errorf("authz table names %s, which no SRouter registrar registers", path)
		}
	}
	if len(inventory.routes) < len(authz.Routes())/2 {
		t.Fatalf("inventory found only %d routes; the source scan is broken", len(inventory.routes))
	}
}
