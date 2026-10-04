package httpapi

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const (
	MaxDepth  = 16
	MaxFields = 300
	MaxCost   = 5000
)

type documentLimits struct{}

func (documentLimits) ExtensionName() string                   { return "DocumentLimits" }
func (documentLimits) Validate(graphql.ExecutableSchema) error { return nil }
func (documentLimits) MutateOperationContext(_ context.Context, op *graphql.OperationContext) *gqlerror.Error {
	if len(op.Doc.Operations) != 1 {
		return &gqlerror.Error{Message: "exactly one operation per document is required", Extensions: map[string]any{"code": "BAD_USER_INPUT"}}
	}
	fields := 0
	// Expand every fragment occurrence, including aliases and skipped selections,
	// with early cutoffs so fragment multiplication cannot bypass the budget.
	var walk func(ast.SelectionSet, int, int) (int, bool)
	walk = func(set ast.SelectionSet, depth, multiplier int) (int, bool) {
		if depth > MaxDepth {
			return 0, false
		}
		cost := 0
		for _, selection := range set {
			switch field := selection.(type) {
			case *ast.Field:
				fields++
				if fields > MaxFields {
					return 0, false
				}
				cost += multiplier
				childMultiplier := multiplier
				if field.Name == "posts" || field.Name == "comments" {
					first, err := graphql.UnmarshalInt(field.ArgumentMap(op.Variables)["first"])
					if err != nil {
						return 0, false
					}
					if first < 1 || first > 100 {
						return 0, false
					}
					childMultiplier *= first
				}
				if childMultiplier > MaxCost {
					return 0, false
				}
				if len(field.SelectionSet) > 0 {
					n, ok := walk(field.SelectionSet, depth+1, childMultiplier)
					if !ok {
						return 0, false
					}
					cost += n
				}
			case *ast.FragmentSpread:
				fields++
				if fields > MaxFields {
					return 0, false
				}
				n, ok := walk(field.Definition.SelectionSet, depth, multiplier)
				if !ok {
					return 0, false
				}
				cost += n
			case *ast.InlineFragment:
				fields++
				if fields > MaxFields {
					return 0, false
				}
				n, ok := walk(field.SelectionSet, depth, multiplier)
				if !ok {
					return 0, false
				}
				cost += n
			}
			if cost > MaxCost {
				return 0, false
			}
		}
		return cost, true
	}
	// Only one operation is accepted, including for named operations.
	for _, operation := range op.Doc.Operations {
		if _, ok := walk(operation.SelectionSet, 1, 1); !ok {
			return &gqlerror.Error{Message: "document exceeds depth, field, page size or cost limits", Extensions: map[string]any{"code": "BAD_USER_INPUT"}}
		}
	}
	return nil
}
