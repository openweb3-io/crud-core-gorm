package repositories

import (
	"os"
	"log"
	"context"
	"time"
	"testing"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	mysql "gorm.io/driver/mysql"
	"github.com/duolacloud/crud-core/types"
	"github.com/duolacloud/crud-core/repositories"
)

type IdentityEntity struct {
	ID string `gorm:"column:id;type:string; size:40; primaryKey"`
	UserID string `gorm:"column:user_id"`
	Provider string `gorm:"column:provider"`
}

func (user *IdentityEntity) TableName() string {
	return "identities"
}

type UserEntity struct {
	// gorm.Model
	ID string `gorm:"column:id;type:string; size:40; primaryKey"`
	Name string `gorm:"column:name"`
	Country string `gorm:"column:country"`
	Age int64 `gorm:"column:age"`
	Birthday time.Time `gorm:"column:birthday"`
	Identities []*IdentityEntity `json:"identities" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (user *UserEntity) TableName() string {
	return "users"
}

type OrganizationMemberEntity struct {
	ID string `gorm:"column:id;type:string; size:40; primaryKey"`
	Name string `gorm:"column:name"`
	UserID string `gorm:"column:user_id"`
	User *UserEntity `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	OrganizationID string
	Organization *OrganizationEntity `json:"organization" gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

func (user *OrganizationMemberEntity) TableName() string {
	return "organization_members"
}

type OrganizationEntity struct {
	ID string `gorm:";type:string; size:40; primaryKey"`
	Name string `gorm:"name"`
}

func (user *OrganizationEntity) TableName() string {
	return "organizations"
}

func SetupDB() *gorm.DB {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:              time.Second,   // Slow SQL threshold
			LogLevel:                   logger.Info, // Log level
			IgnoreRecordNotFoundError: true,           // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,          // Disable color
		},
	)

	dsn := "root:secret@(localhost)/test?charset=utf8mb4&parseTime=True&loc=Local"
	db, dberr := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if dberr != nil {
		panic(dberr)
	}

	dberr = db.AutoMigrate(&UserEntity{}, &IdentityEntity{}, &OrganizationEntity{}, &OrganizationMemberEntity{})
	if dberr != nil {
		panic(dberr)
	}

	return db
}

func TestGormCrudRepository(t *testing.T) {
	db := SetupDB()

	r := NewGormCrudRepository[UserEntity, UserEntity, UserEntity](db)
	// identityRepo := NewGormCrudRepository[IdentityEntity, IdentityEntity, IdentityEntity](db)
		
	c := context.TODO()
	
	_ = r.Delete(c, "1")
	
	birthday, _ := time.Parse("2006-01-02 15:04:05", "1989-03-02 12:00:01")
	t.Logf("birthday: %s\n", birthday)

	u, err := r.Create(c, &UserEntity{
		ID: "1",
		Name: "张三",
		Country: "china",
		Age: 18,
		Birthday: birthday, 
		Identities: []*IdentityEntity{
			&IdentityEntity{
				ID: "1",
				UserID: "1",
				Provider: "google",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	
	
	u, err = r.Get(c, "1")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("u: %v\n", u)

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
			"limit": 1,
			"offset": 0,
		},
	}

	us, err := r.Query(c, query)
	if err != nil {
		t.Fatal(err)
	}
	
	for _, i := range us {
		t.Logf("记录: %v\n", i)
	}
}

func TestRelations(t *testing.T) {
	db := SetupDB()

	c := context.TODO()

	var orgRepo repositories.CrudRepository[OrganizationEntity, OrganizationEntity, OrganizationEntity]
	orgRepo = NewGormCrudRepository[OrganizationEntity, OrganizationEntity, OrganizationEntity](db)
	memberRepo := NewGormCrudRepository[OrganizationMemberEntity, OrganizationMemberEntity, OrganizationMemberEntity](db)

	_ = orgRepo.Delete(c, "1")
	_ = memberRepo.Delete(c, "1")

	org, err := orgRepo.Create(c, &OrganizationEntity{
		ID: "1",
		Name: "组织1",
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("创建组织: %v\n", org)

	member, err := memberRepo.Create(c, &OrganizationMemberEntity{
		ID: "1",
		Name: "成员",
		OrganizationID: "1",
		UserID: "1",
	})

	if err != nil {
		t.Fatal(err)
	}

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
		Page: map[string]int{
			// "limit": 10,
			// "offset": 0,
			"size": 10,
			"page": 1,
		},
	}

	members, err := memberRepo.Query(c, query)
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range members {
		t.Logf("成员: %v\n", m)
	}

	{
		member, err := memberRepo.QueryOne(c, query.Filter)
		if err != nil {
			t.Fatal(err)
		}
	
		t.Logf("queryOne: %v\n", member)
	}
}

func TestCount(t *testing.T) {
	db := SetupDB()

	memberRepo := NewGormCrudRepository[OrganizationMemberEntity, OrganizationMemberEntity, OrganizationMemberEntity](db)

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
			"limit": 10,
			"offset": 0,
		},
	}

	count, err := memberRepo.Count(context.TODO(), query)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("count: %v\n", count)
}
