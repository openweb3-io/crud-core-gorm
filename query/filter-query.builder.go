package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/duolacloud/crud-core/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type FilterQueryBuilder struct {
	schema       *schema.Schema
	whereBuilder *WhereBuilder
}

func NewFilterQueryBuilder(schema *schema.Schema) *FilterQueryBuilder {
	return &FilterQueryBuilder{
		schema:       schema,
		whereBuilder: NewWhereBuilder(),
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

	return db, nil
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

	b.ensureOrders(q)

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

	j, _ := json.Marshal(b.getReferencedRelationsRecursive(b.schema, filter))
	fmt.Printf("b.getReferencedRelationsRecursive(b.schema, filter): %v\n", string(j))

	expression, err := b.whereBuilder.build(filter, b.getReferencedRelationsRecursive(b.schema, filter), "")
	if err != nil {
		return nil, err
	}

	db = db.Where(expression)
	return db, nil
}

func (b *FilterQueryBuilder) applyRelationJoinsRecursive(db *gorm.DB, relationsMap map[string]any, alias string) *gorm.DB {
	if relationsMap == nil {
		return db
	}

	for relation, _ := range relationsMap {
		subRelationsMap := relationsMap[relation].(map[string]any)

		if len(alias) > 0 {
			relation = fmt.Sprintf("%s.%s", alias, relation)
		}

		fmt.Printf("join relation: %v\n", relation)

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
			fmt.Printf("relationMetadata.Schema: %v, %v, %v\n", relationMetadata.FieldSchema, filterValue1, subFilter)
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
	for key, _ := range fieldMap {
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
	hasId := false
	for _, sortField := range query.Sort {
		if sortField[0:1] == "-" {
			sortField = sortField[1:]
		}

		if sortField[0:1] == "+" {
			sortField = sortField[1:]
		}

		if sortField == "id" {
			hasId = true
		}
	}

	// 没有id的排序，直接要追加 ID
	if !hasId {
		index := 0
		if query.Sort == nil {
			query.Sort = make([]string, 1)
			query.Sort[0] = "id"
		} else {
			tmp := query.Sort
			query.Sort = make([]string, len(query.Sort)+1)
			query.Sort[0] = "id"
			for i, f := range tmp {
				query.Sort[i+1] = f
				index++
			}
		}
	}
}

func (b *FilterQueryBuilder) buildCursorFilter(db *gorm.DB, query *types.CursorQuery) (*gorm.DB, error) {
	ors := []clause.Expression{}

	if len(query.Cursor) > 0 {
		cursor := &types.Cursor{}
		err := cursor.Unmarshal(query.Cursor)
		if err != nil {
			return nil, err
		}

		if len(cursor.Value) == 0 {
			return nil, nil
		}

		if len(cursor.Value) != len(query.Sort) {
			return nil, errors.New(fmt.Sprintf("cursor format fields length: %d not match orders fields length: %d", len(cursor.Value), len(query.Sort)))
		}

		fields := make([]string, len(cursor.Value))
		values := make([]any, len(cursor.Value))

		for i, value := range cursor.Value {
			// val := 1
			sortField := query.Sort[i]

			if sortField[0:1] == "-" {
				sortField = sortField[1:]
				// val = -1
			}

			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			field, ok := b.schema.FieldsByDBName[sortField]
			if !ok {
				err := errors.New(fmt.Sprintf("ERR_DB_UNKNOWN_FIELD %s", sortField))
				return nil, err
			}
			fields[i] = sortField

			switch field.DataType {
			case "time":
				// 本身就是 time 类型
				if t, ok := value.(time.Time); ok {
					values[i] = t
				}

				v, err := time.Parse(time.RFC3339, value.(string))
				if err == nil {
					values[i] = v
				}

				// TODO 毫秒类型
			default:
				values[i] = value
			}
		}

		sort_field_0_direction := 1
		sort_field_0 := query.Sort[0]

		if sort_field_0[0:1] == "-" {
			sort_field_0 = sort_field_0[1:]
			sort_field_0_direction = -1
		}

		if sort_field_0[0:1] == "+" {
			sort_field_0 = sort_field_0[1:]
		}

		if query.Direction == types.CursorDirectionBefore {
			// before
			if sort_field_0_direction == -1 {
				var ands []clause.Expression
				for i, field := range fields {
					ands = append(ands, clause.Gt{Column: field, Value: values[i]})

					ors = append(ors, clause.And(ands...))
				}
			} else {
				var ands []clause.Expression
				for i, field := range fields {
					ands = append(ands, clause.Lt{Column: field, Value: values[i]})

					ors = append(ors, clause.And(ands...))
				}
			}
		} else {
			// after
			if sort_field_0_direction == -1 {
				var ands []clause.Expression
				for i, field := range fields {
					ands = append(ands, clause.Lt{Column: field, Value: values[i]})

					ors = append(ors, clause.And(ands...))
				}
			} else {
				var ands []clause.Expression
				for i, field := range fields {
					ands = append(ands, clause.Gt{Column: field, Value: values[i]})

					ors = append(ors, clause.And(ands...))
				}
			}
		}
	}

	if len(ors) > 0 {
		db = db.Where(clause.Or(ors...))
	}

	return db, nil
}
