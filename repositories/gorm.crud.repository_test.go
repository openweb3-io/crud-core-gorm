package repositories

import (
	"context"
	"testing"
	"gorm.io/gorm"
	mysql "gorm.io/driver/mysql"
)

type UserEntity struct {
	gorm.Model
	ID string `gorm:"id"`
	Name string `gorm:"name"`
}

func TestGromCrudRepository(t *testing.T) {
	dsn := "root:secret@(localhost)/test?charset=utf8mb4&parseTime=True&loc=Local"
	db, dberr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if dberr != nil {
		panic(dberr)
	}

	dberr = db.AutoMigrate(&UserEntity{})
	if dberr != nil {
		panic(dberr)
	}


	r := NewGormCrudRepository[UserEntity, UserEntity, UserEntity](db)

	c := context.TODO()

	err := r.Delete(c, "1")

	u, err := r.Create(c, &UserEntity{
		ID: "1",
		Name: "张三",
	})
	if err != nil {
		t.Fatal(err)
	}

	u, err = r.Get(c, "1")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("u: %v\n", u)
}
