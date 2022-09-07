package repositories

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/duolacloud/crud-core-gorm/query"
	"github.com/duolacloud/crud-core/types"
	"github.com/mitchellh/mapstructure"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type GormCrudRepositoryOptions struct {
}

type GormCrudRepositoryOption func(*GormCrudRepositoryOptions)

type GormCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	DB      *gorm.DB
	Schema  *schema.Schema
	Options *GormCrudRepositoryOptions
}

func NewGormCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	DB *gorm.DB,
	opts ...GormCrudRepositoryOption,
) *GormCrudRepository[DTO, CreateDTO, UpdateDTO] {
	r := &GormCrudRepository[DTO, CreateDTO, UpdateDTO]{
		DB: DB,
	}

	var dto DTO
	r.Schema, _ = schema.Parse(&dto, &sync.Map{}, schema.NamingStrategy{})

	r.Options = &GormCrudRepositoryOptions{}
	for _, o := range opts {
		o(r.Options)
	}

	return r
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	var dto DTO
	err := mapstructure.Decode(createDTO, &dto)
	if err != nil {
		return nil, err
	}

	res := r.DB.WithContext(c).Create(&dto)
	if res.Error != nil {
		return nil, res.Error
	}

	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) CreateMany(c context.Context, items []*CreateDTO, opts ...types.CreateManyOption) ([]*DTO, error) {
	dtos := make([]*DTO, len(items))
	for i, item := range items {
		var dto *DTO
		err := mapstructure.Decode(item, &dto)
		if err != nil {
			return nil, err
		}
		dtos[i] = dto
	}

	var _opts types.CreateManyOptions
	for _, o := range opts {
		o(&_opts)
	}

	createBatchSize := _opts.CreateBatchSize
	if createBatchSize <= 0 {
		createBatchSize = 200
	}

	res := r.DB.Session(&gorm.Session{CreateBatchSize: createBatchSize}).WithContext(c).Create(&dtos)
	if res.Error != nil {
		return nil, res.Error
	}

	return dtos, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	/*
		model, err := r.Get(c, id)
		if err != nil {
			return err
		}*/

	filter := make(map[string]any)

	if len(r.Schema.PrimaryFields) == 1 {
		fName := r.Schema.PrimaryFields[0].DBName
		filter[fName] = id
	} else if len(r.Schema.PrimaryFields) > 1 {
		ids, ok := id.(map[string]any)
		if !ok {
			return errors.New("invalid id, not match")
		}

		if len(ids) != len(r.Schema.PrimaryFields) {
			return errors.New("invalid id, size not match")
		}

		for _, primaryField := range r.Schema.PrimaryFields {
			// fmt.Printf("primaryField dbname: %s, name: %s\n", primaryField.DBName, primaryField.Name)
			filter[primaryField.DBName] = ids[primaryField.DBName]
		}
	}

	fmt.Printf("PrimaryFields: table: %s, %v\n", r.Schema.Table, filter)

	var dto DTO
	res := r.DB.WithContext(c).Delete(&dto, filter)
	return res.Error
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
	// modelValue := reflect.New(r.Schema.ModelType)
	dto, err := r.Get(c, id)
	if err != nil {
		return nil, err
	}

	// dto 在 updates之后也被改变了
	res := r.DB.Model(dto).WithContext(c).Updates(updateDTO)
	if res.Error != nil {
		return nil, res.Error
	}

	// TODO 返回对象
	return dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto DTO
	fName := r.Schema.PrimaryFields[0].DBName
	// 直接 First(&dto, id)，字符串不会转义
	err := r.DB.WithContext(c).First(&dto, fName+" = ?", id).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, q *types.PageQuery) ([]*DTO, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err := filterQueryBuilder.BuildQuery(q, r.DB)
	if err != nil {
		return nil, err
	}

	var dtos []*DTO
	res := db.WithContext(c).Find(&dtos)
	if res.Error != nil {
		return nil, err
	}

	return dtos, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery) (int64, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err := filterQueryBuilder.BuildQuery(q, r.DB)
	if err != nil {
		return 0, err
	}

	var dto DTO

	var count int64
	res := db.WithContext(c).Model(dto).Count(&count)
	if res.Error != nil {
		return 0, err
	}

	return count, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) QueryOne(c context.Context, filter map[string]any) (*DTO, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err := filterQueryBuilder.BuildQuery(&types.PageQuery{
		Filter: filter,
	}, r.DB)
	if err != nil {
		return nil, err
	}

	var dto *DTO
	res := db.Model(dto).WithContext(c).First(&dto)
	if res.Error != nil {
		return nil, err
	}

	return dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Aggregate(
	c context.Context,
	filter map[string]any,
	aggregateQuery *types.AggregateQuery,
) ([]*types.AggregateResponse, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	var dto DTO
	db := r.DB.Model(dto).WithContext(c)
	db, err := filterQueryBuilder.BuildAggregateQuery(db, aggregateQuery, filter)
	if err != nil {
		return nil, err
	}

	var results []map[string]any

	res := db.Find(&results)
	if res.Error != nil {
		return nil, res.Error
	}

	return query.ConvertToAggregateResponse(results)
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) CursorQuery(c context.Context, q *types.CursorQuery) ([]*DTO, *types.CursorExtra, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err := filterQueryBuilder.BuildCursorQuery(q, r.DB)
	if err != nil {
		return nil, nil, err
	}

	var result []*DTO
	res := db.WithContext(c).Find(&result)
	if res.Error != nil {
		return nil, nil, err
	}

	extra := &types.CursorExtra{}

	if len(result) == 0 {
		return nil, extra, nil
	}

	if len(result) == int(q.Limit+1) {
		extra.HasNext = true
		extra.HasPrevious = true

		result = result[0 : len(result)-1]
		fmt.Printf("len(result) == q.Limit itemCount: %d, limit: %d\n", len(result), q.Limit)
	}

	toCursor := func(item *DTO) (string, error) {
		sortFieldValues := make([]any, len(q.Sort))
		for i, sortField := range q.Sort {
			if sortField[0:1] == "-" {
				sortField = sortField[1:]
			}

			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			if _, ok := r.Schema.FieldsByDBName[sortField]; !ok {
				return "", fmt.Errorf("field %s not found", sortField)
			}

			var m map[string]any
			bytes, _ := json.Marshal(item)
			_ = json.Unmarshal(bytes, &m)

			sortFieldValues[i] = m[sortField]
		}

		cursor := &types.Cursor{
			Value: sortFieldValues,
		}

		w := new(bytes.Buffer)
		err = cursor.Marshal(w)
		if err != nil {
			return "", err
		}

		return w.String(), nil
	}

	itemCount := len(result)
	extra.StartCursor, err = toCursor(result[0])
	if err != nil {
		return nil, nil, err
	}

	extra.EndCursor, err = toCursor(result[itemCount-1])
	if err != nil {
		return nil, nil, err
	}

	return result, extra, nil
}
