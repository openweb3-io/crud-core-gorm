package query

import (
	"fmt"
	"strings"
	"time"

	"github.com/duolacloud/crud-core/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type FilterQueryBuilder struct {
	schema           *schema.Schema
	whereBuilder     *WhereBuilder
	aggregateBuilder *AggregateBuilder
}

func NewFilterQueryBuilder(schema *schema.Schema) *FilterQueryBuilder {
	return &FilterQueryBuilder{
		schema:           schema,
		whereBuilder:     NewWhereBuilder(),
		aggregateBuilder: NewAggregateBuilder(),
	}
}

func (b *FilterQueryBuilder) BuildQuery(q *types.PageQuery, db *gorm.DB) (*gorm.DB, error) {
	// relation join
	hasRelations := b.filterHasRelations(q.Filter)

	if hasRelations {
		db = b.applyRelationJoinsRecursive(db, b.getReferencedRelationsRecursive(b.schema, q.Filter), "")
	}

	// filter
	db, err := b.applyFilter(db, q.Filter)
	if err != nil {
		return nil, err
	}

	// sort
	db, err = b.applySorting(db, q.Sort)
	if err != nil {
		return nil, err
	}

	// paging
	db, err = b.applyPaging(db, q.Page)
	return db, err
}

func (b *FilterQueryBuilder) BuildCursorQuery(q *types.CursorQuery, db *gorm.DB) (*gorm.DB, error) {
	// relation join
	hasRelations := b.filterHasRelations(q.Filter)

	if hasRelations {
		db = b.applyRelationJoinsRecursive(db, b.getReferencedRelationsRecursive(b.schema, q.Filter), "")
	}

	// filter
	db, err := b.applyFilter(db, q.Filter)
	if err != nil {
		return nil, err
	}

	// 追加主键排序，防止数据重复
	b.ensureOrders(q)

	// 游标过滤
	db, err = b.buildCursorFilter(db, q)
	if err != nil {
		return nil, err
	}

	// sort
	db, err = b.applySorting(db, q.Sort)
	if err != nil {
		return nil, err
	}

	limit := q.Limit + 1
	db = db.Limit(int(limit))

	return db, nil
}

func (b *FilterQueryBuilder) applyFilter(db *gorm.DB, filter map[string]any) (*gorm.DB, error) {
	if filter == nil {
		return db, nil
	}

	// j, _ := json.Marshal(b.getReferencedRelationsRecursive(b.schema, filter))
	// fmt.Printf("b.getReferencedRelationsRecursive(b.schema, filter): %v\n", string(j))

	expression, err := b.whereBuilder.build(filter, b.getReferencedRelationsRecursive(b.schema, filter), "")
	if err != nil {
		return nil, err
	}

	if expression != nil {
		db = db.Where(expression)
	}

	return db, nil
}

func (b *FilterQueryBuilder) applyRelationJoinsRecursive(db *gorm.DB, relationsMap map[string]any, alias string) *gorm.DB {
	if relationsMap == nil {
		return db
	}

	for relation := range relationsMap {
		subRelationsMap := relationsMap[relation].(map[string]any)

		if len(alias) > 0 {
			relation = fmt.Sprintf("%s.%s", alias, relation)
		}

		// fmt.Printf("join relation: %v\n", relation)

		// TODO 目前 join 无法完成 多级关联
		return b.applyRelationJoinsRecursive(
			db.Joins(relation),
			// db.Joins(relation),
			subRelationsMap,
			relation,
		)
	}

	return db
}

func (b *FilterQueryBuilder) getReferencedRelationsRecursive(schema *schema.Schema, filter map[string]any) map[string]any {
	relationMap := map[string]any{}

	for filterField, filterValue := range filter {
		if filterField == "and" || filterField == "or" {
			if subFilters, ok := filterValue.([]map[string]any); ok {
				for _, subFilter := range subFilters {
					subRelations := b.getReferencedRelationsRecursive(schema, subFilter)
					for key, subRelation := range subRelations {
						relationMap[key] = subRelation
					}
				}
			}
		} else {
			relationMetadata, ok := schema.Relationships.Relations[filterField]

			if !ok {
				continue
			}

			var mmm map[string]any
			if relationMap[filterField] != nil {
				mmm = relationMap[filterField].(map[string]any)
			}

			if mmm == nil {
				mmm = map[string]any{}
			}

			filterValue1, ok := filterValue.(map[string]any)
			if !ok {
				continue
			}

			subFilter := b.getReferencedRelationsRecursive(relationMetadata.FieldSchema, filterValue1)
			// fmt.Printf("relationMetadata.Schema: %v, %v, %v\n", relationMetadata.FieldSchema, filterValue1, subFilter)
			for k, v := range subFilter {
				mmm[k] = v
			}
			relationMap[filterField] = mmm
		}
	}

	return relationMap
}

func (b *FilterQueryBuilder) filterHasRelations(filter map[string]any) bool {
	if filter == nil {
		return false
	}

	return len(b.getReferencedRelations(filter)) > 0
}

func (b *FilterQueryBuilder) getReferencedRelations(filter map[string]any) []string {
	relations := b.schema.Relationships.Relations

	referencedFields := b.getFilterFields(filter)

	var referencedRelations []string

	for _, relation := range relations {
		for _, referencedField := range referencedFields {
			if relation.Name == referencedField {
				referencedRelations = append(referencedRelations, relation.Name)
			}
		}
	}

	return referencedRelations
}

func (b *FilterQueryBuilder) getFilterFields(filter map[string]any) []string {
	fieldMap := map[string]bool{}

	for filterField, fieldValue := range filter {
		if filterField == "and" || filterField == "or" {
			if fieldValue != nil {
				if subFilter, ok := fieldValue.(map[string]any); ok {
					subFields := b.getFilterFields(subFilter)
					for _, subField := range subFields {
						fieldMap[subField] = true
					}
				}
			}
		} else {
			fieldMap[filterField] = true
		}
	}

	var fields []string
	for key := range fieldMap {
		fields = append(fields, key)
	}

	return fields
}

func (b *FilterQueryBuilder) applySorting(db *gorm.DB, sort []string) (*gorm.DB, error) {
	for _, sortField := range sort {
		isDesc := false
		if sortField[0:1] == "-" {
			sortField = sortField[1:]
			isDesc = true
		}

		if sortField[0:1] == "+" {
			sortField = sortField[1:]
			isDesc = false
		}

		if !strings.Contains(sortField, ".") {
			// add table name to sort field, avoiding ambiguous column error
			sortField = strings.Join([]string{b.schema.Table, sortField}, ".")
		}

		db = db.Order(clause.OrderByColumn{Column: clause.Column{Name: sortField}, Desc: isDesc})
	}

	return db, nil
}

func (b *FilterQueryBuilder) applyPaging(db *gorm.DB, pagination map[string]int) (*gorm.DB, error) {
	// check for limit
	if limit, ok := pagination["limit"]; ok {
		db = db.Limit(int(limit))

		// check for offset (once limit is set)
		if offset, ok := pagination["offset"]; ok {
			db = db.Offset(int(offset))
		}

		// check for skip (once limit is set)
		if skip, ok := pagination["skip"]; ok {
			db = db.Offset(int(skip))
		}
	}

	// check for page and size
	if size, ok := pagination["size"]; ok {
		db = db.Limit(int(size))

		// set skip (requires understanding of size)
		if page, ok := pagination["page"]; ok {
			db = db.Offset(int((page - 1) * size))
		}
	}

	return db, nil
}

func (b *FilterQueryBuilder) ensureOrders(query *types.CursorQuery) {
	if query.Sort == nil {
		query.Sort = make([]string, 0)
	}

	missingPkFields := make([]string, 0)

	for _, pkField := range b.schema.PrimaryFieldDBNames {

		hasPkField := false

		for _, sortField := range query.Sort {
			if sortField[0:1] == "-" {
				sortField = sortField[1:]
			}

			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			if pkField == sortField {
				hasPkField = true
				break
			}
		}

		if !hasPkField {
			field := strings.Join([]string{b.schema.Table, pkField}, ".")
			missingPkFields = append(missingPkFields, field)
		}
	}

	if len(missingPkFields) > 0 {
		query.Sort = append(query.Sort, missingPkFields...)
	}
}

func (b *FilterQueryBuilder) buildCursorFilter(db *gorm.DB, query *types.CursorQuery) (*gorm.DB, error) {
	if len(query.Cursor) == 0 {
		return db, nil
	}

	cursor := &types.Cursor{}
	err := cursor.Unmarshal(query.Cursor)
	if err != nil {
		return nil, err
	}

	if len(cursor.Value) == 0 {
		return db, nil
	}
	if len(cursor.Value) != len(query.Sort) {
		return nil, fmt.Errorf("cursor format fields length: %d not match orders fields length: %d", len(cursor.Value), len(query.Sort))
	}

	fields := make([]string, len(cursor.Value))
	values := make([]any, len(cursor.Value))
	isDescs := make([]bool, len(cursor.Value))

	for i, value := range cursor.Value {
		sortField := query.Sort[i]
		isDesc := false

		if sortField[0:1] == "-" {
			sortField = sortField[1:]
			isDesc = true
		}

		if sortField[0:1] == "+" {
			sortField = sortField[1:]
		}

		// trim maybe existed tablename prefix
		baseFieldName := sortField
		if strings.Contains(baseFieldName, ".") {
			baseFieldName = strings.Split(baseFieldName, ".")[1]
		}
		field, ok := b.schema.FieldsByDBName[baseFieldName]
		if !ok {
			return nil, fmt.Errorf("ERR_DB_UNKNOWN_FIELD %s", baseFieldName)
		}
		fields[i] = sortField
		isDescs[i] = isDesc

		switch field.DataType {
		case "time":
			switch v := value.(type) {
			case int64:
				values[i] = time.UnixMilli(v)
			case time.Time, *time.Time:
				values[i] = v
			case string:
				if values[i], err = time.Parse(time.RFC3339, v); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("field %s value type error", sortField)
			}
		default:
			values[i] = value
		}
	}

	ors := []clause.Expression{}

	for i := 0; i < len(cursor.Value); i++ {

		ands := make([]clause.Expression, i+1)

		for j := 0; j < i; j++ {
			ands[j] = clause.Eq{Column: fields[j], Value: values[j]}
		}

		if query.Direction == types.CursorDirectionBefore {
			if isDescs[i] {
				ands[i] = clause.Gt{Column: fields[i], Value: values[i]}
			} else {
				ands[i] = clause.Lt{Column: fields[i], Value: values[i]}
			}
		} else {
			if isDescs[i] {
				ands[i] = clause.Lt{Column: fields[i], Value: values[i]}
			} else {
				ands[i] = clause.Gt{Column: fields[i], Value: values[i]}
			}
		}

		ors = append(ors, clause.And(ands...))
	}

	if len(ors) > 0 {
		db = db.Where(clause.Or(ors...))
	}

	return db, nil
}

func (b *FilterQueryBuilder) BuildAggregateQuery(db *gorm.DB, aggregate *types.AggregateQuery, filter map[string]any) (*gorm.DB, error) {
	hasRelations := b.filterHasRelations(filter)

	if hasRelations {
		db = b.applyRelationJoinsRecursive(db, b.getReferencedRelationsRecursive(b.schema, filter), "")
	}

	db, err := b.applyAggregate(db, aggregate, "")
	if err != nil {
		return nil, err
	}

	// filter
	db, err = b.applyFilter(db, filter)
	if err != nil {
		return nil, err
	}

	db, err = b.applyAggregateSorting(db, aggregate.GroupBy, "")
	if err != nil {
		return nil, err
	}

	db, err = b.applyGroupBy(db, aggregate.GroupBy, "")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (b *FilterQueryBuilder) applyAggregate(db *gorm.DB, aggregate *types.AggregateQuery, alias string) (*gorm.DB, error) {
	return b.aggregateBuilder.Build(db, aggregate, alias)
}

func (b *FilterQueryBuilder) applyAggregateSorting(db *gorm.DB, groupBy []string, alias string) (*gorm.DB, error) {
	return db, nil
}

func (b *FilterQueryBuilder) applyGroupBy(db *gorm.DB, groupBy []string, alias string) (*gorm.DB, error) {
	for _, group := range groupBy {
		if len(alias) > 0 {
			group = fmt.Sprintf("%s.%s", alias, group)
		}

		db = db.Group(group)
	}

	return db, nil
}
