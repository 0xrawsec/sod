package sod

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

const (
	dbpath = "data/database"
)

type TestStruct struct {
	Item
	A int
	B int
	C string
}

func createFreshTestDb(n int) *DB {
	os.RemoveAll(dbpath)
	db := Open(dbpath)
	db.Create(&TestStruct{}, &DefaultSchema)
	for i := 0; i < n; i++ {
		if err := db.Update(&TestStruct{A: rand.Int(), B: rand.Int(), C: "Random"}); err != nil {
			panic(err)
		}
	}
	return db
}

func TestSimpleDb(t *testing.T) {
	os.RemoveAll(dbpath)
	db := Open(dbpath)

	s := Schema{Extension: ".json"}
	db.Create(&TestStruct{}, &s)

	t1 := TestStruct{A: 1, B: 2, C: "Test"}
	if err := db.Update(&t1); err != nil {
		t.Errorf("Failed to save structure: %s", err)
		t.FailNow()
	}
	t.Log(t1)

	ts := TestStruct{}

	if err := db.Get(&ts); err == nil {
		t.Error("This call should have failed")
	}

	if err := db.Get(&t1); err != nil {
		t.Error(err)
	}
}

func TestGetAll(t *testing.T) {
	count := 100
	db := createFreshTestDb(count)

	if s, err := db.All(&TestStruct{}); err != nil {
		t.Error(err)
	} else {
		if len(s) != count {
			t.Errorf("Expecting %d items, got %d", count, len(s))
		}
		if n, err := db.Count(&TestStruct{}); n != count {
			t.Errorf("Expecting %d items, got %d: %s", count, n, err)
		}
		for _, o := range s {
			t.Log(*(o.(*TestStruct)))
		}
	}
}

func corruptFile(path string) {
	var data []byte
	var err error

	if data, err = ioutil.ReadFile(path); err != nil {
		panic(err)
	}

	for i := range data {
		if rand.Int()%2 == 0 {
			data[i] = '\x00'
		}
	}

	if err := ioutil.WriteFile(path, data, DefaultPermissions); err != nil {
		panic(err)
	}

}

func TestCorruptedFiles(t *testing.T) {
	count := 100
	db := createFreshTestDb(count)

	if s, err := db.All(&TestStruct{}); err != nil {
		t.Error(err)
	} else {
		t.Log("Corrupting files")
		for _, o := range s {
			if path, err := db.oPath(o); err != nil {
				t.Error(err)
				t.FailNow()
			} else {
				corruptFile(path)
			}
		}
	}

	if s, err := db.All(&TestStruct{}); errors.Is(err, &json.MarshalerError{}) {
		t.Logf("Retrieved %d objects", len(s))
		t.Error("We should have encountered error")
	} else {
		t.Logf("Encountered error getting objects: %s", err)
	}

}

func TestDrop(t *testing.T) {
	n := 20
	deln := 10
	db := createFreshTestDb(n)
	if s, err := db.All(&TestStruct{}); err != nil {
		t.Error(err)
	} else {
		i := deln
		for _, o := range s {
			if i == 0 {
				break
			}
			t := o.(*TestStruct)
			db.Delete(t)
			i--
		}

		if c, err := db.Count(&TestStruct{}); c != n-deln {
			t.Errorf("Expecting %d items, got %d: %s", n-deln, c, err)
		}

		// droping all items
		db.DeleteAll(&TestStruct{})
		if c, err := db.Count(&TestStruct{}); c != 0 {
			t.Errorf("Expecting %d items, got %d: %s", n-deln, c, err)
		}

		if err := db.Drop(); err != nil {
			t.Error(err)
		}

		if _, err := os.Stat(dbpath); err == nil {
			t.Errorf("Database must have been deleted")
		}

	}
}
