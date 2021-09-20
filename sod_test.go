package sod

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	dbpath = "data/database"
)

type testStruct struct {
	Item
	A int
	B int
	C string
	D int16
	E int32
	F int64
	G uint8
	H uint16
	I uint32
	J uint64
	K float64
	L int8
	M time.Time
}

func randMod(mod int) int {
	return rand.Int() % mod
}

func createFreshTestDb(n int, s *Schema) *DB {
	os.RemoveAll(dbpath)
	db := Open(dbpath)
	if s == nil {
		s = &DefaultSchema
	}
	db.Create(&testStruct{}, s)
	for i := 0; i < n; i++ {
		c := "foo"
		if rand.Int()%2 == 0 {
			c = "bar"
		}
		ts := &testStruct{A: randMod(42),
			B: randMod(42),
			C: c,
			D: int16(randMod(42)),
			E: int32(randMod(42)),
			F: int64(randMod(42)),
			G: uint8(randMod(42)),
			H: uint16(randMod(42)),
			I: uint32(randMod(42)),
			K: float64(randMod(42)),
			L: int8(randMod(42)),
			M: time.Now(),
		}
		if err := db.InsertOrUpdate(ts); err != nil {
			panic(err)
		}
	}
	db.Commit(&testStruct{})
	return db
}
func TestTypeof(t *testing.T) {
	if typeof(&testStruct{}) != typeof(testStruct{}) {
		t.Error("Unexpected typeof behaviour")
	}
}

func TestSimpleDb(t *testing.T) {
	os.RemoveAll(dbpath)
	db := Open(dbpath)

	s := Schema{Extension: ".json"}
	db.Create(&testStruct{}, &s)

	t1 := testStruct{A: 1, B: 2, C: "Test"}
	if err := db.InsertOrUpdate(&t1); err != nil {
		t.Errorf("Failed to save structure: %s", err)
		t.FailNow()
	}
	t.Log(t1)

	ts := testStruct{}

	if err := db.Get(&ts); err == nil {
		t.Error("This call should have failed")
	}

	if err := db.Get(&t1); err != nil {
		t.Error(err)
	}
}

func TestGetAll(t *testing.T) {
	count := 100
	db := createFreshTestDb(count, nil)

	if s, err := db.All(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		if len(s) != count {
			t.Errorf("Expecting %d items, got %d", count, len(s))
		}
		if n, err := db.Count(&testStruct{}); n != count {
			t.Errorf("Expecting %d items, got %d: %s", count, n, err)
		}
		for _, o := range s {
			t.Log(*(o.(*testStruct)))
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
	db := createFreshTestDb(count, nil)

	if s, err := db.All(&testStruct{}); err != nil {
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

	if s, err := db.All(&testStruct{}); errors.Is(err, &json.MarshalerError{}) {
		t.Logf("Retrieved %d objects", len(s))
		t.Error("We should have encountered error")
	} else {
		t.Logf("Encountered error getting objects: %s", err)
	}

}

func TestDrop(t *testing.T) {
	n := 20
	deln := 10
	db := createFreshTestDb(n, nil)
	if s, err := db.All(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		// deleting deln objects
		i := deln
		for _, o := range s {
			if i == 0 {
				break
			}
			ts := o.(*testStruct)
			// we test if the object  exists
			if ok, err := db.Exist(ts); err != nil {
				t.Error(err)
			} else if !ok {
				t.Errorf("Object should exist")
			}
			// we delete the object
			db.Delete(ts)
			// we test if the object still exists
			if ok, err := db.Exist(ts); err != nil {
				t.Error(err)
			} else if ok {
				t.Errorf("Object should have been deleted")
			}
			i--
		}

		if c, err := db.Count(&testStruct{}); c != n-deln {
			t.Errorf("Expecting %d items, got %d: %s", n-deln, c, err)
		}

		// droping all items
		db.DeleteAll(&testStruct{})
		if c, err := db.Count(&testStruct{}); c != 0 {
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
func TestSchema(t *testing.T) {
	var s *Schema
	var err error

	size := 100
	s = &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	db := createFreshTestDb(size, s)
	db.Close()

	db = Open(dbpath)
	// first call should unmarshall schema from disk
	if s, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
	}

	// second call should use cached schema
	if s, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
	}
}

func TestCloseAndReopen(t *testing.T) {
	var s *Schema
	var err error

	size := 100
	s = &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	db := createFreshTestDb(size, s)

	if err := db.Close(); err != nil {
		t.Error(err)
	}

	db = Open(dbpath)
	defer db.Close()
	/*if s, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
		t.FailNow()
	}*/

	// we insert some more data
	for i := 0; i < size; i++ {
		c := "foo"
		if rand.Int()%2 == 0 {
			c = "bar"
		}
		if err = db.InsertOrUpdate(&testStruct{A: rand.Int() % 42, B: rand.Int() % 42, C: c}); err != nil {
			panic(err)
		}
	}

	t.Logf("Index size: %d", s.ObjectsIndex.Len())

	if c, err := db.Search(&testStruct{}, "A", "<", 10).Collect(); err != nil {
		t.Error(err)
	} else {
		t.Logf("Search result len: %d", len(c))
		for _, o := range c {
			ts := o.(*testStruct)
			if ts.A >= 10 {
				t.Error("Wrong value for A")
			}
		}
	}
}
func TestUpdateObject(t *testing.T) {
	var s *Schema

	size := 100
	s = &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	db := createFreshTestDb(size, s)
	defer db.Close()

	if s, err := db.All(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		for _, o := range s {
			ts := o.(*testStruct)
			ts.A = 42
			ts.B = 4242
			ts.C = "foobar"
			if err = db.InsertOrUpdate(ts); err != nil {
				t.Error(err)
			}
		}
	}

	if c, err := db.Search(&testStruct{}, "A", "=", 42).And("B", "=", 4242).And("C", "=", "foobar").Collect(); err != nil {
		t.Error(err)
	} else {
		if len(c) != size {
			t.Errorf("Expecting %d results got %d", size, len(c))
		}
	}

}

func TestIndexAllTypes(t *testing.T) {
	var s *Schema

	size := 100
	s = &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C", "E", "F", "G", "H", "I", "J", "K", "L", "M")}
	db := createFreshTestDb(size, s)
	db.Close()

	db = Open(dbpath)
	defer db.Close()

	if _, err := db.Search(&testStruct{}, "A", "<", 42).
		And("B", "<", 42).
		And("C", "=", "bar").
		And("D", "!=", 42).
		And("E", ">=", 0).
		And("F", ">", 4).
		And("G", "<=", uint(42)).
		And("H", "<", uint(42)).
		And("I", "<", uint(42)).
		And("J", "<", uint(42)).
		And("K", "<", float32(42)).
		And("L", "<", 42).
		And("M", "<", time.Now()).
		Collect(); err != nil {
		t.Error(err)
	}
}

func TestSearchError(t *testing.T) {

	size := 100
	s := &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C", "E", "F", "G", "H", "I", "J", "K", "L", "M")}
	db := createFreshTestDb(size, s)

	if s := db.Search(&testStruct{}, "A", "<>", 42).And("B", "=", 42).Or("C", "=", "bar"); !errors.Is(s.Err(), ErrUnkownSearchOperator) {
		t.Error("Should have raised error")
	}
}

func TestUnknownObject(t *testing.T) {
	size := 100
	s := &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C", "E", "F", "G", "H", "I", "J", "K", "L", "M")}
	db := createFreshTestDb(size, s)

	type Unknown struct {
		Item
	}

	if _, err := db.Schema(&Unknown{}); err == nil {
		t.Error("Should raise schema error")
	}

	if err := db.InsertOrUpdate(&Unknown{}); err == nil {
		t.Error("Should raise insert error")
	}

	if err := db.Delete(&Unknown{}); err == nil {
		t.Error("Should raise delete error")
	}

	if err := db.Commit(&Unknown{}); err == nil {
		t.Error("Should raise commit error")
	}
}
