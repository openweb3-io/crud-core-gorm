package repositories

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/duolacloud/crud-core/datasource"
	"github.com/duolacloud/crud-core/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type IdentityEntity struct {
	ID       string `gorm:"column:id;type:string; size:40; primaryKey"`
	UserID   string `gorm:"column:user_id"`
	Provider string `gorm:"column:provider"`
}

func (user *IdentityEntity) TableName() string {
	return "identities"
}

type UserEntity struct {
	// gorm.Model
	ID         string    `gorm:"column:id;type:string; size:40; primaryKey"`
	Name       string    `gorm:"column:name"`
	Country    string    `gorm:"column:country"`
	Age        int       `gorm:"column:age"`
	Birthday   time.Time `gorm:"column:birthday"`
	CreatedAt  *time.Time
	Identities []*IdentityEntity `json:"identities" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (user *UserEntity) TableName() string {
	return "users"
}

type UserRelationEntity struct {
	From      string `gorm:"primaryKey"`
	To        string `gorm:"primaryKey"`
	Status    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type OrganizationMemberEntity struct {
	ID             string      `gorm:"column:id;type:string; size:40; primaryKey"`
	Name           string      `gorm:"column:name"`
	UserID         string      `gorm:"column:user_id"`
	User           *UserEntity `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	OrganizationID string
	Organization   *OrganizationEntity `json:"organization" gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (user *OrganizationMemberEntity) TableName() string {
	return "organization_members"
}

type OrganizationEntity struct {
	ID   string `gorm:";type:string; size:40; primaryKey"`
	Name string `gorm:"name"`
}

func (user *OrganizationEntity) TableName() string {
	return "organizations"
}

func SetupDB() datasource.DataSource[gorm.DB] {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Info, // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,       // Disable color
		},
	)

	dsn := "host=localhost user=postgres password=postgres dbname=test port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, dberr := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if dberr != nil {
		panic(dberr)
	}

	dberr = db.AutoMigrate(&UserEntity{}, &IdentityEntity{}, &UserRelationEntity{}, &OrganizationEntity{}, &OrganizationMemberEntity{})
	if dberr != nil {
		panic(dberr)
	}

	db = db.Debug()
	dc := datasource.NewDataSource(db)
	return dc
}

func TestCreateMany(t *testing.T) {
	db := SetupDB()

	r := NewGormCrudRepository[UserEntity, UserEntity, map[string]any](db)
	// identityRepo := NewGormCrudRepository[IdentityEntity, IdentityEntity, IdentityEntity](db)

	c := context.TODO()

	birthday, _ := time.Parse("2006-01-02 15:04:05", "1989-03-02 12:00:01")
	t.Logf("birthday: %s\n", birthday)

	var users []*UserEntity
	for i := 1; i <= 5; i++ {
		userID := fmt.Sprintf("%v", i)
		users = append(users, &UserEntity{
			ID:       userID,
			Name:     fmt.Sprintf("用户%v", i),
			Country:  "china",
			Age:      18 + i,
			Birthday: birthday,
			Identities: []*IdentityEntity{
				{
					ID:       fmt.Sprintf("%v", i),
					UserID:   userID,
					Provider: "google",
				},
			},
		})
	}

	createdUsers, err := r.CreateMany(c, users, types.WithCreateBatchSize(3))
	assert.NoError(t, err)
	defer func() {
		for _, u := range createdUsers {
			_ = r.Delete(c, u.ID)
		}
	}()

	for _, u := range createdUsers {
		t.Logf("批量创建用户: %v\n", u)
	}
}

func TestGormCursorQuery(t *testing.T) {
	db := SetupDB()
	r := NewGormCrudRepository[UserEntity, UserEntity, map[string]any](db)
	c := context.TODO()

	createdUsers := make([]*UserEntity, 0)
	defer func() {
		for _, u := range createdUsers {
			if u != nil {
				r.Delete(c, u.ID)
			}
		}
	}()

	for i := 0; i < 20; i++ {
		u, err := r.Create(c, &UserEntity{
			ID:       fmt.Sprintf("%d", i),
			Name:     fmt.Sprintf("name%d", i),
			Birthday: time.Now().Add(time.Duration(i) * time.Hour),
		})
		assert.Nil(t, err)
		createdUsers = append(createdUsers, u)
	}

	users, extra, err := r.CursorQuery(c, &types.CursorQuery{
		Filter:    map[string]any{"name": map[string]any{"like": "name%"}},
		Limit:     5,
		Direction: types.CursorDirectionAfter,
		Sort:      []string{"-users.birthday", "+name"},
	})
	assert.Nil(t, err)
	assert.Equal(t, true, extra.HasNext)
	assert.Len(t, users, 5)

	users, extra, err = r.CursorQuery(c, &types.CursorQuery{
		Filter:    map[string]any{"name": map[string]any{"like": "name%"}},
		Cursor:    extra.EndCursor,
		Limit:     5,
		Direction: types.CursorDirectionAfter,
		Sort:      []string{"-birthday", "+name"},
	})
	assert.Nil(t, err)
	assert.Equal(t, true, extra.HasNext)
	assert.Equal(t, "14", users[0].ID)
}

func TestGormCrudRepository(t *testing.T) {
	db := SetupDB()

	r := NewGormCrudRepository[UserEntity, UserEntity, map[string]any](db)
	// identityRepo := NewGormCrudRepository[IdentityEntity, IdentityEntity, IdentityEntity](db)

	c := context.TODO()

	_ = r.Delete(c, "1")

	birthday, _ := time.Parse("2006-01-02 15:04:05", "1989-03-02 12:00:01")
	t.Logf("birthday: %s\n", birthday)

	u, err := r.Create(c, &UserEntity{
		ID:       "1",
		Name:     "张三",
		Country:  "china",
		Age:      18,
		Birthday: birthday,
		Identities: []*IdentityEntity{
			{
				ID:       "1",
				UserID:   "1",
				Provider: "google",
			},
		},
	})

	assert.NoError(t, err)
	defer func() {
		_ = r.Delete(c, u.ID)
	}()

	t.Logf("创建用户: %v\n", u)

	// update
	{
		u, err = r.Update(c, "1", &map[string]any{
			"name": "李四",
		})
		if err != nil {
			t.Error(err)
		}
		t.Logf("update user: %v\n", u)
	}

	// get
	{
		u, err = r.Get(c, "1")
		if err != nil {
			t.Error(err)
		}

		t.Logf("get user: %v\n", u)
	}

	query := &types.PageQuery{
		Fields: []string{
			"name",
			"_id",
		},
		Filter: map[string]any{
			"age": map[string]any{
				"between": map[string]any{
					"lower": 18,
					"upper": 24,
				},
			},
			/*"name": map[string]any{
				"in": []any{
					"李四",
					"哈哈",
				},
			},*/
			"birthday": map[string]any{
				"gt": "1987-02-02T12:00:01Z",
				"lt": "1999-02-02T12:00:01Z",
			},
		},
		Page: map[string]int{
			"limit":  1,
			"offset": 0,
		},
	}

	us, err := r.Query(c, query)
	if err != nil {
		t.Error(err)
	}

	for _, i := range us {
		t.Logf("记录: %v\n", i)
	}

	{
		us, extra, err := r.CursorQuery(c, &types.CursorQuery{
			Limit: 1,
		})
		if err != nil {
			t.Error(err)
		}

		t.Logf("extra: %v\n", extra)

		for _, i := range us {
			t.Logf("cursorQuery: 记录: %v\n", i)
		}
	}
}

func TestRelations(t *testing.T) {
	db := SetupDB()

	c := context.TODO()

	orgRepo := NewGormCrudRepository[OrganizationEntity, OrganizationEntity, OrganizationEntity](db)
	memberRepo := NewGormCrudRepository[OrganizationMemberEntity, OrganizationMemberEntity, OrganizationMemberEntity](db)
	userRepo := NewGormCrudRepository[UserEntity, UserEntity, UserEntity](db)

	org, err := orgRepo.Create(c, &OrganizationEntity{
		ID:   "1",
		Name: "组织1",
	})

	assert.NoError(t, err)
	defer func() {
		_ = orgRepo.Delete(c, org.ID)
	}()

	t.Logf("创建组织: %v\n", org)

	user, err := userRepo.Create(c, &UserEntity{
		ID:   "1",
		Name: "user1",
	})
	assert.NoError(t, err)
	defer func() {
		_ = userRepo.Delete(c, user.ID)
	}()

	member, err := memberRepo.Create(c, &OrganizationMemberEntity{
		ID:             "1",
		Name:           "成员",
		OrganizationID: "1",
		UserID:         "1",
	})

	assert.NoError(t, err)
	defer func() {
		_ = memberRepo.Delete(c, member.ID)
	}()
	t.Logf("创建成员: %v\n", member)

	query := &types.PageQuery{
		Fields: []string{
			"id",
			"name",
		},
		Filter: map[string]any{
			"User": map[string]any{
				// "Identities": map[string]any{
				"id": map[string]any{
					"eq": "1",
				},
				// },
			},
		},
		Sort: []string{
			"name",
		},
		Page: map[string]int{
			// "limit": 10,
			// "offset": 0,
			"size": 10,
			"page": 1,
		},
	}

	members, err := memberRepo.Query(c, query)
	assert.NoError(t, err)

	for _, m := range members {
		t.Logf("成员: %v\n", m)
	}

	{
		member, err := memberRepo.QueryOne(c, query.Filter)
		assert.NoError(t, err)

		t.Logf("queryOne: %v\n", member)
	}
}

func TestCount(t *testing.T) {
	db := SetupDB()

	memberRepo := NewGormCrudRepository[OrganizationMemberEntity, OrganizationMemberEntity, OrganizationMemberEntity](db)
	orgRepo := NewGormCrudRepository[OrganizationEntity, OrganizationEntity, OrganizationEntity](db)
	userRepo := NewGormCrudRepository[UserEntity, UserEntity, UserEntity](db)

	mem, err := memberRepo.Create(context.TODO(), &OrganizationMemberEntity{
		ID:             "1",
		Name:           "成员",
		OrganizationID: "1",
		UserID:         "1",
		User: &UserEntity{
			ID:   "1",
			Name: "user1",
		},
		Organization: &OrganizationEntity{
			ID:   "1",
			Name: "org1",
		},
	})

	assert.NoError(t, err)
	defer func() {
		memberRepo.Delete(context.TODO(), mem.ID)
		orgRepo.Delete(context.TODO(), mem.OrganizationID)
		userRepo.Delete(context.TODO(), mem.UserID)
	}()

	query := &types.PageQuery{
		Fields: []string{
			"id",
			"name",
		},
		Filter: map[string]any{
			"User": map[string]any{
				// "Identities": map[string]any{
				"id": map[string]any{
					"eq": "1",
				},
				// },
			},
		},
		Page: map[string]int{
			"limit":  10,
			"offset": 0,
		},
	}

	count, err := memberRepo.Count(context.TODO(), query)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	t.Logf("count: %v\n", count)
}

func TestAggregate(t *testing.T) {
	db := SetupDB()

	userRepo := NewGormCrudRepository[UserEntity, UserEntity, map[string]any](db)

	query := &types.PageQuery{
		Fields: []string{
			"id",
			"name",
		},
		Filter: map[string]any{
			"id": map[string]any{
				"eq": "1",
			},
		},
		Page: map[string]int{
			"limit":  10,
			"offset": 0,
		},
	}

	aggs, err := userRepo.Aggregate(context.TODO(), query.Filter, &types.AggregateQuery{
		GroupBy: []string{
			"country",
		},
		Count: []string{
			"country",
		},
		Max: []string{
			"age",
		},
		Min: []string{
			"age",
		},
	})

	assert.NoError(t, err)

	for _, agg := range aggs {
		js, err := json.Marshal(agg)

		assert.NoError(t, err)

		t.Logf("聚合: %v\n", string(js))
	}
}

func TestGet(t *testing.T) {
	db := SetupDB()

	r := NewGormCrudRepository[UserEntity, UserEntity, map[string]any](db)

	c := context.TODO()

	userID := uuid.NewString()
	identityID := uuid.NewString()
	user := &UserEntity{
		ID:       userID,
		Name:     fmt.Sprintf("用户%s", userID),
		Country:  "china",
		Age:      18,
		Birthday: time.Now(),
		Identities: []*IdentityEntity{
			{
				ID:       identityID,
				UserID:   userID,
				Provider: "google",
			},
		},
	}
	r.Create(c, user)
	defer r.Delete(c, userID)

	gotUser, err := r.Get(c, userID)
	assert.Nil(t, err)
	assert.Equal(t, user.ID, gotUser.ID)
	assert.Equal(t, user.Name, gotUser.Name)

	_, err = r.Get(c, "123456")
	assert.ErrorIs(t, err, types.ErrNotFound)

	_, err = r.Get(c, "Where 1 = 1")
	assert.ErrorIs(t, err, types.ErrNotFound)

	relationRepo := NewGormCrudRepository[UserRelationEntity, UserRelationEntity, map[string]any](db)

	from := uuid.NewString()
	to := uuid.NewString()
	relation := &UserRelationEntity{
		From:   from,
		To:     to,
		Status: true,
	}
	relationRepo.Create(c, relation)
	defer relationRepo.Delete(c, relation)

	_, err = relationRepo.Get(c, "123")
	assert.NotNil(t, err)

	_, err = relationRepo.Get(c, map[string]any{"from": from})
	assert.NotNil(t, err)

	gotRelation, err := relationRepo.Get(c, map[string]any{"from": from, "to": to})
	assert.Nil(t, err)
	assert.Equal(t, relation.From, gotRelation.From)
	assert.Equal(t, relation.To, gotRelation.To)
	assert.True(t, relation.Status)

	gotRelation, err = relationRepo.Get(c, map[string]any{"from": from, "to": "douyin|12345"})
	assert.ErrorIs(t, err, types.ErrNotFound)
}
