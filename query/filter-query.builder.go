package query

import (
	"fmt"

	"duolacloud.com/duolacloud/crud-core/types"
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

	return b.whereBuilder.build(db, filter, b.getReferencedRelationsRecursive(b.schema, filter))
}

func (b *FilterQueryBuilder) applyRelationJoinsRecursive(db *gorm.DB, relationsMap map[string]interface{}, alias string) *gorm.DB {
	if relationsMap == nil {
		return db
	}

	for relation, _ := range relationsMap {
		/*
			var name string
			if len(alias) > 0 {
				name = alias
			} else {
				name = b.schema.Table
			}*/
		fmt.Printf("join relation: %v\n", relation)

		return b.applyRelationJoinsRecursive(
			db.Joins(relation),
			// db.Joins(fmt.Sprintf("%s.%s", name, relation)),
			relationsMap[relation].(map[string]interface{}),
			relation,
		)
	}

	return db
}

func (b *FilterQueryBuilder) getReferencedRelationsRecursive(schema *schema.Schema, filter map[string]interface{}) map[string]interface{} {
	relationMap := map[string]interface{}{}

	for filterField, filterValue := range filter {
		if filterField == "and" || filterField == "or" {
			if subFilters, ok := filterValue.([]map[string]interface{}); ok {
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

			var mmm map[string]interface{}
			if relationMap[filterField] != nil {
				mmm = relationMap[filterField].(map[string]interface{})
			}

			if mmm == nil {
				mmm = map[string]interface{}{}
			}

			filterValue1, ok := filterValue.(map[string]interface{})
			if !ok {
				continue
			}

			subFilter := b.getReferencedRelationsRecursive(relationMetadata.Schema, filterValue1)
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
