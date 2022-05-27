package query

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/duolacloud/crud-core/types"
)

type AggregateFunc string

const (
	AggregateFuncAVG   AggregateFunc = "avg"
	AggregateFuncSUM   AggregateFunc = "sum"
	AggregateFuncCOUNT AggregateFunc = "count"
	AggregateFuncMAX   AggregateFunc = "max"
	AggregateFuncMIN   AggregateFunc = "min"
)

var AGG_REGEXP = regexp.MustCompile("(avg|sum|count|max|min|group_by)_(.*)")

func ConvertToAggregateResponse(aggregates []map[string]any) ([]*types.AggregateResponse, error) {
	r := make([]*types.AggregateResponse, len(aggregates))
	for i, aggregate := range aggregates {
		ar := &types.AggregateResponse{}

		agg, err := extractResponse(aggregate["_id"].(map[string]any))
		ar.Merge(agg)

		agg, err = extractResponse(aggregate)
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

	for resultField, _ /*v*/ := range response {
		if resultField == "_id" {
			continue
		}

		matchResult := AGG_REGEXP.FindAllStringSubmatch(resultField, -1)

		if len(matchResult[0]) != 3 {
			return nil, errors.New(fmt.Sprintf("Unknown aggregate column encountered for %s.", resultField))
		}

		matchedFunc := matchResult[0][1]
		matchedFieldName := matchResult[0][2]

		aggFunc := matchedFunc // camelCase(matchedFunc.toLowerCase())
		fieldName := matchedFieldName

		agg.Append(aggFunc, fieldName, response[resultField])
	}

	return agg, nil
}
