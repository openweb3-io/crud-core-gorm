package query

import (
	"errors"
	"fmt"

	"gorm.io/gorm/clause"
)

func IsBetweenVal(
	val interface{},
) bool {
	if val == nil {
		return false
	}

	m, ok := val.(map[string]interface{})
	if !ok {
		return false
	}

	if m["lower"] == nil {
		return false
	}

	if m["upper"] == nil {
		return false
	}

	return true
}

type ExpressionFunc func(field string, value any) (clause.Expression, error)

var DEFAULT_COMPARISON_MAP = map[string]ExpressionFunc{
	"eq": func(field string, value any) (clause.Expression, error) {
		return clause.Eq{
			Column: field,
			Value:  value,
		}, nil
	},
	"neq": func(field string, value any) (clause.Expression, error) {
		return clause.Neq{
			Column: field,
			Value:  value,
		}, nil
	},
	"gt": func(field string, value any) (clause.Expression, error) {
		return clause.Gt{
			Column: field,
			Value:  value,
		}, nil
	},
	"gte": func(field string, value any) (clause.Expression, error) {
		return clause.Gte{
			Column: field,
			Value:  value,
		}, nil
	},
	"lt": func(field string, value any) (clause.Expression, error) {
		return clause.Lt{
			Column: field,
			Value:  value,
		}, nil
	},
	"lte": func(field string, value any) (clause.Expression, error) {
		return clause.Lte{
			Column: field,
			Value:  value,
		}, nil
	},
	"like": func(field string, value any) (clause.Expression, error) {
		return clause.Like{
			Column: field,
			Value:  value,
		}, nil
	},
	"notlike": func(field string, value any) (clause.Expression, error) {
		// NegationBuild
		return clause.Not(clause.Like{
			Column: field,
			Value:  value,
		}), nil
	},
	"ilike": func(field string, value any) (clause.Expression, error) {
		// NegationBuild
		return clause.Like{
			Column: field,
			Value:  value,
		}, nil
	},
	"notilike": func(field string, value any) (clause.Expression, error) {
		// NegationBuild
		return clause.Not(clause.Like{
			Column: field,
			Value:  value,
		}), nil
	},
	"in": func(field string, value any) (clause.Expression, error) {
		return clause.IN{
			Column: field,
			Values: value.([]any),
		}, nil
	},
	"notin": func(field string, value any) (clause.Expression, error) {
		// NegationBuild
		return clause.Not(clause.IN{
			Column: field,
			Values: value.([]any),
		}), nil
	},
	"between": func(field string, value any) (clause.Expression, error) {
		if !IsBetweenVal(value) {
			return nil, errors.New(fmt.Sprintf("Invalid value for between expected {lower: val, upper: val} got %v", value))
		}

		values := value.(map[string]any)

		return clause.And(
			clause.Gte{
				Column: field,
				Value:  values["lower"],
			},
			clause.Lte{
				Column: field,
				Value:  values["upper"],
			},
		), nil
	},
	"notbetween": func(field string, value any) (clause.Expression, error) {
		if !IsBetweenVal(value) {
			return nil, errors.New(fmt.Sprintf("Invalid value for between expected {lower: val, upper: val} got %v", value))
		}

		values := value.(map[string]any)

		return clause.Not(clause.And(
			clause.Gte{
				Column: field,
				Value:  values["lower"],
			},
			clause.Lte{
				Column: field,
				Value:  values["upper"],
			},
		)), nil
	},
}

type SQLComparisonBuilder struct {
}

func NewSQLComparisonBuilder() *SQLComparisonBuilder {
	return &SQLComparisonBuilder{}
}

func (b *SQLComparisonBuilder) Build(field string, cmp string, value any, alias string) (clause.Expression, error) {
	operator, ok := DEFAULT_COMPARISON_MAP[cmp]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Operator %s not found", cmp))
	}

	if len(alias) > 0 {
		field = fmt.Sprintf("%s.%s", alias, field)
	}

	return operator(field, value)
}
