package repositories

import (
	"context"
	"gorm.io/gorm"
	"duolacloud.com/duolacloud/crud-core/types"
	// "duolacloud.com/duolacloud/crud-core-gorm/query"
)

type GormCrudRepositoryOptions struct {
	StrictValidation bool
}

type GormCrudRepositoryOption func (*GormCrudRepositoryOptions)

func WithStrictValidation(v bool) GormCrudRepositoryOption {
	return func(o *GormCrudRepositoryOptions) {
		o.StrictValidation = v
	}
}

type GormCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	DB *gorm.DB
	Options *GormCrudRepositoryOptions
}

func NewGormCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	DB *gorm.DB,
	opts ...GormCrudRepositoryOption,
) *GormCrudRepository[DTO, CreateDTO, UpdateDTO] {
	r := &GormCrudRepository[DTO, CreateDTO, UpdateDTO]{
		DB: DB,
	}

	r.Options = &GormCrudRepositoryOptions{}
	for _, o := range opts {
		o(r.Options)
	}

	return r
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	res := r.DB.Create(createDTO)
	if res.Error != nil {
		return nil, res.Error
	}

	return nil, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	/*
	model, err := r.Get(c, id)
	if err != nil {
		return err
	}*/
	var dto DTO
	res := r.DB.Delete(&dto, id)
	return res.Error
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
	res := r.DB.Save(updateDTO)
	if res.Error != nil {
		return nil, res.Error
	}

	// TODO
	return nil, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto DTO
	err := r.DB.First(&dto, id).Error
	if err != nil {
		return nil, err
	}

	return &dto, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, q *types.PageQuery) ([]*DTO, error) {
	/*
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return nil, err
	}
	*/

	var dtos []*DTO
	return dtos, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery) (int64, error) {
	/*
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return 0, err
	}
	*/

	// TODO
	count := int64(0)
	return count, nil
}