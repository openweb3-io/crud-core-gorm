package query

import (
	"encoding/json"
	"fmt"

	"github.com/duolacloud/crud-core/types"
	"gorm.io/gorm"
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

	// paging

	return db, nil
}

func (b *FilterQueryBuilder) applyFilter(db *gorm.DB, filter map[string]interface{}) (*gorm.DB, error) {
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

func (b *FilterQueryBuilder) applyRelationJoinsRecursive(db *gorm.DB, relationsMap map[string]interface{}, alias string) *gorm.DB {
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

func (b *FilterQueryBuilder) filterHasRelations(filter map[string]interface{}) bool {
	if filter == nil {
		return false
	}

	return len(b.getReferencedRelations(filter)) > 0
}

func (b *FilterQueryBuilder) getReferencedRelations(filter map[string]interface{}) []string {
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

func (b *FilterQueryBuilder) getFilterFields(filter map[string]interface{}) []string {
	fieldMap := map[string]bool{}

	for filterField, fieldValue := range filter {
		if filterField == "and" || filterField == "or" {
			if fieldValue != nil {
				if subFilter, ok := fieldValue.(map[string]interface{}); ok {
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
