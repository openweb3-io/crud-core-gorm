package query

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/duolacloud/crud-core/types"
	"gorm.io/gorm"
)

type AggregateFunc string

const (
	AggregateFuncAVG   AggregateFunc = "AVG"
	AggregateFuncSUM   AggregateFunc = "SUM"
	AggregateFuncCOUNT AggregateFunc = "COUNT"
	AggregateFuncMAX   AggregateFunc = "MAX"
	AggregateFuncMIN   AggregateFunc = "MIN"
)

var AGG_REGEXP = regexp.MustCompile("(AVG|SUM|COUNT|MAX|MIN|GROUP_BY|avg|sum|count|max|min|group_by)_(.*)")

func ConvertToAggregateResponse(aggregates []map[string]any) ([]*types.AggregateResponse, error) {
	r := make([]*types.AggregateResponse, len(aggregates))
	for i, aggregate := range aggregates {
		ar := &types.AggregateResponse{}

		agg, err := extractResponse(aggregate)
		ar.Merge(agg)
		if err != nil {
			return nil, err
		}

		r[i] = ar
	}

	return r, nil
}

func extractResponse(response map[string]any) (*types.AggregateResponse, error) {
	if response == nil {
		return &types.AggregateResponse{}, nil
	}

	agg := &types.AggregateResponse{}

	for resultField /*, v*/ := range response {
		resultField = strings.ToUpper(resultField)
		matchResult := AGG_REGEXP.FindAllStringSubmatch(resultField, -1)

		if len(matchResult[0]) != 3 {
			return nil, fmt.Errorf("unknown aggregate column encountered for %s", resultField)
		}

		matchedFunc := matchResult[0][1]
		matchedFieldName := matchResult[0][2]

		aggFunc := matchedFunc // camelCase(matchedFunc.toLowerCase())
		fieldName := matchedFieldName

		agg.Append(aggFunc, fieldName, response[resultField])
	}

	return agg, nil
}

type ColumnPair struct {
	Column string
	Alias  string
}

type aggregatePayload struct {
	Fn     AggregateFunc
	Fields []string
}

type AggregateBuilder struct {
}

func NewAggregateBuilder() *AggregateBuilder {
	return &AggregateBuilder{}
}

func (b *AggregateBuilder) Build(db *gorm.DB, aggregate *types.AggregateQuery, alias string) (*gorm.DB, error) {
	var totalColumns []ColumnPair

	columns, err := b.createGroupBySelect(db, aggregate.GroupBy, alias)
	if err != nil {
		return nil, err
	}
	totalColumns = append(totalColumns, columns...)

	aggregators := []aggregatePayload{
		{
			Fn:     AggregateFuncCOUNT,
			Fields: aggregate.Count,
		},
		{
			Fn:     AggregateFuncSUM,
			Fields: aggregate.Sum,
		},
		{
			Fn:     AggregateFuncAVG,
			Fields: aggregate.Avg,
		},
		{
			Fn:     AggregateFuncAVG,
			Fields: aggregate.Avg,
		},
		{
			Fn:     AggregateFuncMAX,
			Fields: aggregate.Max,
		},
		{
			Fn:     AggregateFuncMIN,
			Fields: aggregate.Min,
		},
	}

	for _, aggregator := range aggregators {
		columns, _ = b.createAggSelect(db, aggregator.Fn, aggregator.Fields, alias)
		totalColumns = append(totalColumns, columns...)
	}

	if len(totalColumns) == 0 {
		return nil, errors.New("no aggregate fields found")
	}

	/*
		const [head, ...tail] = selects
		return tail.reduce(
			(acc: Qb, [select, selectAlias]) => acc.addSelect(select, selectAlias),
			qb.select(head[0], head[1]),
		);
	*/

	var selects []string
	for _, column := range totalColumns {
		sel := fmt.Sprintf("%s AS %s", column.Column, column.Alias)
		selects = append(selects, sel)
	}

	db = db.Select(selects)

	return db, nil
}

func (b *AggregateBuilder) createGroupBySelect(db *gorm.DB, fields []string, alias string) ([]ColumnPair, error) {
	var columns []ColumnPair

	if len(fields) == 0 {
		return nil, nil
	}

	for _, field := range fields {
		if len(alias) > 0 {
			field = fmt.Sprintf("%s.%s", alias, field)
		}

		columns = append(columns, ColumnPair{
			Column: field,
			Alias:  getGroupByAlias(field),
		})
	}

	return columns, nil
}

func (b *AggregateBuilder) createAggSelect(db *gorm.DB, fn AggregateFunc, fields []string, alias string) ([]ColumnPair, error) {
	var columns []ColumnPair

	if len(fields) == 0 {
		return nil, nil
	}

	for _, field := range fields {
		aggregateAlias := getAggregateAlias(fn, field)
		if len(alias) > 0 {
			field = fmt.Sprintf("%s.%s", alias, field)
		}

		columns = append(columns, ColumnPair{
			Column: fmt.Sprintf("%s(%s)", fn, field),
			Alias:  aggregateAlias,
		})
	}

	return columns, nil
}

func getAggregateAlias(fn AggregateFunc, field string) string {
	return fmt.Sprintf("%s_%s", fn, field)
}

func getGroupByAlias(field string) string {
	return fmt.Sprintf("GROUP_BY_%s", field)
}
