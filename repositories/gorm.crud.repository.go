package repositories

import (
	"context"
	"duolacloud.com/duolacloud/crud-core/types"
	"duolacloud.com/duolacloud/crud-core-gorm/query"
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

	r.Options = &MongoCrudRepositoryOptions{}
	for _, o := range opts {
		o(r.Options)
	}

	return r
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	res := r.DB.Create(c, createDTO)
	if res.Error != nil {
		return nil, res.Error
	}

	return nil, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	model, err := r.Get(c, id)
	if err != nil {
		return err
	}

	err := r.DB.Delete(model)
	return err
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
	res := c.Db.Save(updateDTO)
	if res.Error != nil {
		return nil, res.Error
	}

	return updateDTO, nil
}

func (r *GormCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto *DTO
	err := c.Db.First(dto, id).Error
	if err != nil {
		return nil, err
	}

	return dto, nil
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
	count := 0
	return count, err
}