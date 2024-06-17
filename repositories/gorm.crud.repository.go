package repositories

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/duolacloud/crud-core-gorm/query"
	"github.com/duolacloud/crud-core/datasource"
	"github.com/duolacloud/crud-core/types"
	"github.com/mitchellh/mapstructure"
	"github.com/oleiade/reflections"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type GormCrudRepositoryOptions struct {
}

type GormCrudRepositoryOption func(*GormCrudRepositoryOptions)

type GormCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	datasource datasource.DataSource[gorm.DB]
	Schema  *schema.Schema
	Options *GormCrudRepositoryOptions
}

func NewGormCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	datasource datasource.DataSource[gorm.DB],
	opts ...GormCrudRepositoryOption,
) *GormCrudRepository[DTO, CreateDTO, UpdateDTO] {
	r := &GormCrudRepository[DTO, CreateDTO, UpdateDTO]{datasource: datasource}

	var dto DTO
	r.Schema, _ = schema.Parse(&dto, &sync.Map{}, schema.NamingStrategy{})

	r.Options = &GormCrudRepositoryOptions{}
	for _, o := range opts {
		o(r.Options)
	}
	return r
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	db, err := r.datasource.GetDB(c)
	if err != nil {
		return nil, err
	}

	var dto DTO
	err = mapstructure.Decode(createDTO, &dto)
	if err != nil {
		return nil, err
	}
	res := db.WithContext(c).Create(&dto)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) CreateMany(c context.Context, items []*CreateDTO, opts ...types.CreateManyOption) ([]*DTO, error) {
	db, err := r.datasource.GetDB(c) 
        if err != nil {
                return nil, err
        }

	dtos := make([]*DTO, len(items))
	for i, item := range items {
		var dto DTO
		err := mapstructure.Decode(item, &dto)
		if err != nil {
			return nil, err
		}
		dtos[i] = &dto
	}

	var _opts types.CreateManyOptions
	for _, o := range opts {
		o(&_opts)
	}

	createBatchSize := _opts.CreateBatchSize
	if createBatchSize <= 0 {
		createBatchSize = 200
	}

	res := db.Session(&gorm.Session{CreateBatchSize: createBatchSize}).WithContext(c).Create(&dtos)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return dtos, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID, opts ...types.DeleteOption) error {
	db, err := r.datasource.GetDB(c) 
        if err != nil {
                return err
        }

	var _opts types.DeleteOptions
        for _, o := range opts {
                o(&_opts)
        }

	filter, err := r.primaryKeysFilter(id)
	if err != nil {
		return err
	}
	var dto DTO

	if _opts.DeleteMode == types.DeleteModeHard {
		db = db.Unscoped()
	}

	res := db.Clauses(clause.Returning{}).WithContext(c).Delete(&dto, filter)
	return wrapGormError(res.Error)
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, err
        }

	dto, err := r.Get(c, id)
	if err != nil {
		return nil, err
	}
	// dto 在 updates之后也被改变了
	res := db.Model(dto).WithContext(c).Updates(updateDTO)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID, opts... types.GetOption) (*DTO, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, err
        }

	filter, err := r.primaryKeysFilter(id)
	if err != nil {
		return nil, err
	}
	var dto DTO
	if err := db.WithContext(c).Where(filter).First(&dto).Error; err != nil {
		return nil, wrapGormError(err)
	}
	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, q *types.PageQuery) ([]*DTO, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, err
        }

	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err = filterQueryBuilder.BuildQuery(q, db)
	if err != nil {
		return nil, err
	}

	var dtos []*DTO
	res := db.WithContext(c).Find(&dtos)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return dtos, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery) (int64, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return 0, err
        }

	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err = filterQueryBuilder.BuildQuery(q, db)
	if err != nil {
		return 0, err
	}

	var dto DTO
	var count int64
	res := db.WithContext(c).Model(dto).Count(&count)
	if res.Error != nil {
		return 0, wrapGormError(res.Error)
	}
	return count, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) QueryOne(c context.Context, filter map[string]any) (*DTO, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, err
        }

	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err = filterQueryBuilder.BuildQuery(&types.PageQuery{Filter: filter}, db)
	if err != nil {
		return nil, err
	}

	var dto DTO
	res := db.Model(&dto).WithContext(c).First(&dto)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Aggregate(
	c context.Context,
	filter map[string]any,
	aggregateQuery *types.AggregateQuery,
) ([]*types.AggregateResponse, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, err
        }

	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	var dto DTO
	db = db.Model(dto).WithContext(c)
	db, err = filterQueryBuilder.BuildAggregateQuery(db, aggregateQuery, filter)
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	res := db.Find(&results)
	if res.Error != nil {
		return nil, wrapGormError(res.Error)
	}
	return query.ConvertToAggregateResponse(results)
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) CursorQuery(c context.Context, q *types.CursorQuery) ([]*DTO, *types.CursorExtra, error) {
	db, err := r.datasource.GetDB(c)
        if err != nil {
                return nil, nil, err
        }

	filterQueryBuilder := query.NewFilterQueryBuilder(r.Schema)

	db, err = filterQueryBuilder.BuildCursorQuery(q, db)
	if err != nil {
		return nil, nil, err
	}

	var result []*DTO
	res := db.WithContext(c).Find(&result)
	if res.Error != nil {
		return nil, nil, wrapGormError(res.Error)
	}

	extra := &types.CursorExtra{}

	if len(result) == 0 {
		return nil, extra, nil
	}

	if len(result) == int(q.Limit+1) {
		extra.HasNext = true
		extra.HasPrevious = true
		result = result[0 : len(result)-1]
	}

	toCursor := func(item *DTO) (string, error) {
		var err error
		sortFieldValues := make([]any, len(q.Sort))
		for i, sortField := range q.Sort {
			if sortField[0:1] == "-" {
				sortField = sortField[1:]
			}

			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			schemaField, ok := r.Schema.FieldsByDBName[sortField]
			if !ok {
				return "", fmt.Errorf("field %s not found", sortField)
			}

			sortFieldValues[i], err = reflections.GetField(*item, schemaField.Name)
			if err != nil {
				return "", fmt.Errorf("field %s value error", sortField)
			}
		}

		cursor := &types.Cursor{Value: sortFieldValues}
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

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) primaryKeysFilter(id types.ID) (map[string]any, error) {
	filter := make(map[string]any)

	if len(r.Schema.PrimaryFields) == 1 {
		fName := r.Schema.PrimaryFields[0].DBName
		filter[fName] = id

	} else if len(r.Schema.PrimaryFields) > 1 {
		ids, ok := id.(map[string]any)
		if !ok {
			return nil, errors.New("invalid id, should be associated primary keys")
		}
		if len(ids) != len(r.Schema.PrimaryFields) {
			return nil, errors.New("invalid id, primary keys' size not match")
		}
		for _, primaryField := range r.Schema.PrimaryFields {
			// fmt.Printf("primaryField dbname: %s, name: %s\n", primaryField.DBName, primaryField.Name)
			if value, ok := ids[primaryField.DBName]; ok {
				filter[primaryField.DBName] = value
			} else {
				return nil, errors.New("invalid id, missing primary key value")
			}
		}
	}
	// fmt.Printf("PrimaryFields: table: %s, %v\n", r.Schema.Table, filter)
	return filter, nil
}

func wrapGormError(err error) error {
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return types.ErrNotFound
		}
	}
	return err
}
