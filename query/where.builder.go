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
				var err error
				db, err = b.filterAnd(db, and, relationNames)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if filter["or"] != nil {
		if or, ok := filter["or"].([]map[string]interface{}); ok {
			if len(or) > 0 {
				var err error
				db, err = b.filterOr(db, or, relationNames)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return b.filterFields(db, filter, relationNames)
}

func (b *WhereBuilder) filterAnd(db *gorm.DB, filters []map[string]interface{}, relationNames map[string]interface{}) (*gorm.DB, error) {
	db.Where(nil)
	/*
		return where.andWhere(
			new Brackets((qb) => filters.reduce((w, f) => qb.andWhere(this.createBrackets(f, relationNames, alias)), qb)),
		)
	*/
	return db, nil
}

func (b *WhereBuilder) filterOr(db *gorm.DB, filters []map[string]interface{}, relationNames map[string]interface{}) (*gorm.DB, error) {
	return db, nil
}

func (b *WhereBuilder) filterFields(db *gorm.DB, filter map[string]interface{}, relationNames map[string]interface{}) (*gorm.DB, error) {
	return db, nil
}
