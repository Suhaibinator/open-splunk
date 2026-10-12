// Package resourcepolicy carries trusted source visibility into native metadata queries.
package resourcepolicy

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const UnresolvedSource = "kvstore_unresolved_source"
const AppDefaultsSource = "kvstore_app_defaults_source"

// A history entry may retain all 1,024 effective catalog indexes in addition
// to authored selectors and declared scope. The union stays explicitly bounded.
const MaximumSourceNames = 2048

// SourceNames records declared scope and authored index selectors without
// compiling or executing SPL. Failed parsing is represented by a reserved
// source which can never be an authorized physical index.
func SourceNames(definition *opensplunk.SearchDefinition, extra ...string) ([]byte, error) {
	names := append([]string{}, extra...)
	if definition != nil {
		names = append(names, definition.GetIndexScope()...)
		if len(definition.GetIndexScope()) == 0 && definition.GetAppId() != "" {
			names = append(names, AppDefaultsSource)
		}
		parsed, err := spl.Parse(definition.GetSpl())
		if err != nil {
			names = append(names, UnresolvedSource)
		} else {
			var visit func(spl.Expr)
			visit = func(expr spl.Expr) {
				switch node := expr.(type) {
				case *spl.BinaryExpr:
					visit(node.Left)
					visit(node.Right)
				case *spl.NotExpr:
					visit(node.Operand)
				case *spl.ComparisonExpr:
					if strings.EqualFold(node.Field, "index") {
						names = append(names, node.Value.Text)
					}
				}
			}
			visit(parsed.Search)
			indexField := func(expr spl.ScalarExpr) bool {
				field, ok := expr.(*spl.ScalarFieldExpr)
				return ok && strings.EqualFold(field.Field, "index")
			}
			literal := func(expr spl.ScalarExpr) {
				if value, ok := expr.(*spl.ScalarLiteralExpr); ok {
					names = append(names, value.Value.Text)
				} else {
					names = append(names, UnresolvedSource)
				}
			}
			var visitWhere func(spl.WhereExpr)
			visitWhere = func(expr spl.WhereExpr) {
				switch node := expr.(type) {
				case *spl.WhereBoolExpr:
					visitWhere(node.Left)
					visitWhere(node.Right)
				case *spl.WhereNotExpr:
					visitWhere(node.Operand)
				case *spl.WhereComparisonExpr:
					if indexField(node.Left) {
						literal(node.Right)
					}
					if indexField(node.Right) {
						literal(node.Left)
					}
				case *spl.WhereMembershipExpr:
					if indexField(node.Value) {
						for _, value := range node.Candidates {
							literal(value)
						}
					}
				}
			}
			for _, command := range parsed.Commands {
				if search, ok := command.(*spl.SearchCommand); ok {
					visit(search.Expression)
				}
				if where, ok := command.(*spl.WhereCommand); ok {
					visitWhere(where.Expression)
				}
			}
		}
	}
	for i, name := range names {
		names[i] = strings.ToLower(strings.TrimSpace(name))
		if !ValidSourceName(names[i]) {
			names[i] = UnresolvedSource
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) > MaximumSourceNames {
		return nil, errors.New("source index scope exceeds its bound")
	}
	return json.Marshal(names)
}
func DashboardSourceNames(definition *opensplunk.DashboardDefinition) ([]byte, error) {
	names := []string{}
	if definition != nil {
		for _, panel := range definition.GetPanels() {
			encoded, err := SourceNames(panel.GetSearch())
			if err != nil {
				return nil, err
			}
			var scope []string
			if err = json.Unmarshal(encoded, &scope); err != nil {
				return nil, err
			}
			names = append(names, scope...)
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) > MaximumSourceNames {
		return nil, errors.New("dashboard source scope exceeds its bound")
	}
	return json.Marshal(names)
}
func MatchesSourceNames(stored, expected []byte) bool {
	var one, two []string
	if json.Unmarshal(stored, &one) != nil || json.Unmarshal(expected, &two) != nil {
		return false
	}
	return slices.Equal(one, two)
}
func ValidSourceName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if name == UnresolvedSource || name == AppDefaultsSource {
		return true
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '*' || c == '?' {
			continue
		}
		return false
	}
	return !strings.Contains(name, "kvstore")
}
