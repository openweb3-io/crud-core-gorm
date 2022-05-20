package query

import (
	"gorm.io/gorm"
)

type WhereBuilder struct {
}

func NewWhereBuilder() *WhereBuilder {
	return &WhereBuilder{}
}

func (b *WhereBuilder) build(
	db *gorm.DB,
	filter map[string]interface{},
	relationNames map[string]interface{},
) (*gorm.DB, error) {
	if filter["and"] != nil {
		if and, ok := filter["and"].([]map[string]interface{}); ok {
			if len(and) > 0 {
				b.filterAnd(db, and, relationNames)
			}
		}
	}

	if filter["or"] != nil {
		if or, ok := filter["or"].([]map[string]interface{}); ok {
			if len(or) > 0 {
				b.filterOr(db, or, relationNames)
			}
		}
	}

	return b.filterFields(db, filter, relationNames)
}

func (b *WhereBuilder) filterAnd(db *gorm.DB, filters []map[string]interface{}, relationNames map[string]interface{}) {

}

func (b *WhereBuilder) filterOr(db *gorm.DB, filters []map[string]interface{}, relationNames map[string]interface{}) {

}

func (b *WhereBuilder) filterFields(db *gorm.DB, filter map[string]interface{}, relationNames map[string]interface{}) (*gorm.DB, error) {
	return db, nil
}
