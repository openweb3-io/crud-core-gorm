package query

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type WhereBuilder struct {
	sqlComparisonBuilder *SQLComparisonBuilder
}

func NewWhereBuilder() *WhereBuilder {
	return &WhereBuilder{
		sqlComparisonBuilder: NewSQLComparisonBuilder(),
	}
}

func (b *WhereBuilder) build(
	db *gorm.DB,
	filter map[string]any,
	relationNames map[string]any,
) ([]clause.Expression, error) {
	var expressions []clause.Expression

	if filter["and"] != nil {
		if and, ok := filter["and"].([]map[string]any); ok {
			if len(and) > 0 {
				var err error
				expression, err := b.filterAnd(db, and, relationNames)
				if err != nil {
					return nil, err
				}
				expressions = append(expressions, expression)
			}
		}
	}

	if filter["or"] != nil {
		if or, ok := filter["or"].([]map[string]any); ok {
			if len(or) > 0 {
				var err error
				expression, err := b.filterOr(db, or, relationNames)
				if err != nil {
					return nil, err
				}
				expressions = append(expressions, expression)
			}
		}
	}

	expression, err := b.filterFields(db, filter, relationNames)
	if err != nil {
		return nil, err
	}
	expressions = append(expressions, expression)

	return expressions, nil
}

func (b *WhereBuilder) buildWhere(db *gorm.DB, filter map[string]any, relationNames map[string]any) (clause.Expression, error) {
	var expressions []clause.Expression

	if filter["and"] != nil {
		subFilters := filter["and"].([]map[string]any)
		expression, err := b.filterAnd(db, subFilters, relationNames)
		if err != nil {
			return nil, err
		}

		expressions = append(expressions, expression)
	}

	if filter["or"] != nil {
		subFilters := filter["or"].([]map[string]any)
		expression, err := b.filterOr(db, subFilters, relationNames)
		if err != nil {
			return nil, err
		}

		expressions = append(expressions, expression)
	}

	if len(expressions) == 1 {
		return expressions[0], nil
	}

	return clause.And(expressions...), nil
}

func (b *WhereBuilder) filterAnd(db *gorm.DB, filters []map[string]any, relationNames map[string]any) (clause.Expression, error) {
	var expressions []clause.Expression
	for _, filter := range filters {
		expression, err := b.buildWhere(db, filter, relationNames)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, expression)
	}

	return clause.And(expressions...), nil
}

func (b *WhereBuilder) filterOr(db *gorm.DB, filters []map[string]interface{}, relationNames map[string]interface{}) (clause.Expression, error) {
	var expressions []clause.Expression
	for _, filter := range filters {
		expression, err := b.buildWhere(db, filter, relationNames)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, expression)
	}

	return clause.Or(expressions...), nil
}

func (b *WhereBuilder) filterFields(db *gorm.DB, filter map[string]any, relationNames map[string]any) (clause.Expression, error) {
	var expressions []clause.Expression

	for field, value := range filter {
		if field != "and" && field != "or" {
			fmt.Printf("filterFields: %s\n", field)
			expression, err := b.withFilterComparison(
				field,
				value.(map[string]any),
				relationNames,
			)
			if err != nil {
				return nil, err
			}

			expressions = append(expressions, expression)
		}
	}

	fmt.Printf("filterFields expressions: %v\n", expressions)

	return clause.And(expressions...), nil
}

func (b *WhereBuilder) withFilterComparison(field string, cmp map[string]any, relationNames map[string]any) (clause.Expression, error) {
	if relationNames[field] != nil {
		return b.withRelationFilter(field, cmp, relationNames[field].(map[string]any))
	}

	var sqlComparisons []clause.Expression
	for cmpType, value := range cmp {
		sqlComparison, err := b.sqlComparisonBuilder.Build(field, cmpType, value)
		if err != nil {
			return nil, err
		}
		sqlComparisons = append(sqlComparisons, sqlComparison)
	}

	fmt.Printf("withFilterComparison: %s, %v\n", field, sqlComparisons)

	return clause.And(clause.Or(sqlComparisons...)), nil
}

func (b *WhereBuilder) withRelationFilter(field string, cmp map[string]any, relationNames map[string]any) (clause.Expression, error) {
	return nil, nil
}
