package sod

import (
	"encoding/json"
	"errors"
	"fmt"
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
	A int       `sod:"index"`
	B int       `sod:"index"`
	C string    `sod:"index"`
	D int16     `sod:"index"`
	E int32     `sod:"index"`
	F int64     `sod:"index"`
	G uint8     `sod:"index"`
	H uint16    `sod:"index"`
	I uint32    `sod:"index"`
	J uint64    `sod:"index"`
	K float64   `sod:"index"`
	L int8      `sod:"index"`
	M time.Time `sod:"index"`
	N uint
}

type testStructUnique struct {
	Item
	A int    `sod:"unique"`
	B int32  `sod:"unique"`
	C string `sod:"unique"`
}

func randMod(mod int) int {
	return rand.Int() % mod
}

func genTestStructs(n int) chan Object {
	co := make(chan Object)
	go func() {
		defer close(co)
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
				N: uint(randMod(42)),
			}
			co <- ts
		}
	}()
	return co
}

func cleanup() {
	if err := os.RemoveAll(dbpath); err != nil {
		panic(err)
	}
}

func createFreshTestDb(n int, s Schema) *DB {
	cleanup()

	db := Open(dbpath)
	if err := db.Create(&testStruct{}, s); err != nil {
		panic(err)
	}
	if err := db.InsertOrUpdateBulk(genTestStructs(n)); err != nil {
		panic(err)
	}
	return db
}

func controlDB(t *testing.T, db *DB) {
	if err := db.Control(); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestTypeof(t *testing.T) {
	if typeof(&testStruct{}) != typeof(testStruct{}) {
		t.Error("Unexpected typeof behaviour")
	}
}

func TestSimpleDb(t *testing.T) {
	os.RemoveAll(dbpath)
	db := Open(dbpath)
	defer controlDB(t, db)

	db.Create(&testStruct{}, DefaultSchema)

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
	count := 200
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

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
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

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
	db := createFreshTestDb(n, DefaultSchema)
	defer controlDB(t, db)

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
	var err error

	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
	db.Close()

	db = Open(dbpath)
	// first call should unmarshall schema from disk
	if _, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
	}

	// second call should use cached schema
	if _, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
	}
}

func TestCloseAndReopen(t *testing.T) {
	var err error
	var s *Schema

	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	if err := db.Close(); err != nil {
		t.Error(err)
	}

	db = Open(dbpath)
	defer controlDB(t, db)
	defer db.Close()

	if s, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
		t.FailNow()
	}

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

	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
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

	if s, err := db.Schema(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		if err = s.ObjectsIndex.Control(); err != nil {
			t.Error(err)
		}
	}

}

func TestIndexAllTypes(t *testing.T) {

	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
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
		And("N", "<", uint(42)).
		Collect(); err != nil {
		t.Error(err)
	}
}

func TestSearchOrder(t *testing.T) {
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	// testing normal output
	if sr, err := db.Search(&testStruct{}, "A", "<", 42).Collect(); err != nil {
		t.Error(err)
	} else {
		var prev *testStruct

		for _, obj := range sr {
			ts := obj.(*testStruct)
			if prev == nil {
				prev = ts
				continue
			}
			if ts.A > prev.A {
				t.Error("order is not correct")
			}
		}

		if len(sr) != size {
			t.Error("normal order results are missing")
		}
	}

	// testing reversed output
	if sr, err := db.Search(&testStruct{}, "A", "<", 42).Reverse().Collect(); err != nil {
		t.Error(err)
	} else {
		var prev *testStruct

		for _, obj := range sr {
			ts := obj.(*testStruct)
			if prev == nil {
				prev = ts
				continue
			}
			if ts.A < prev.A {
				t.Error("reverse order is not correct")
			}
		}

		if len(sr) != size {
			t.Error("reverse ordered results are missing")
		}
	}
}

func TestSearchLimit(t *testing.T) {
	size := 100
	limit := uint64(10)
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	// testing normal output
	if sr, err := db.Search(&testStruct{}, "A", "<", 42).Limit(limit).Collect(); err != nil {
		t.Error(err)
	} else {
		if uint64(len(sr)) > limit {
			t.Error("more results than expected")
		}
		t.Logf("Expecting %d results, got %d", limit, len(sr))
	}
}

func TestSearchError(t *testing.T) {

	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	if s := db.Search(&testStruct{}, "A", "<>", 42).And("B", "=", 42).Or("C", "=", "bar"); !errors.Is(s.Err(), ErrUnkownSearchOperator) {
		t.Error("Should have raised error")
	}
}

func TestUnknownObject(t *testing.T) {
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

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

func TestDBBulkDeletion(t *testing.T) {
	// testing bulk object deletion
	size := 10000
	ndel := 0
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	if s := db.Search(&testStruct{}, "A", ">=", 12); s.Err() != nil {
		t.Error(s.Err())
	} else {
		ndel = s.Len()
		t.Logf("Deleting %d entries", s.Len())
		if err := s.Delete(); err != nil {
			t.Log(err)
		}

		// controlling size after deletion
		if n, err := db.Count(&testStruct{}); err != nil {
			t.Error(err)
		} else if n != size-s.Len() {
			t.Error("Bulk delete failed")
		}
	}

	db = Open(dbpath)
	// controlling size after re-opening the DB
	if n, err := db.Count(&testStruct{}); err != nil {
		t.Error(err)
	} else if n != size-ndel {
		t.Errorf("Bulk delete failed expecting size=%d, got %d", size-ndel, n)
	}
}

func TestUniqueObject(t *testing.T) {
	cleanup()

	db := Open(dbpath)
	defer controlDB(t, db)

	db.Create(&testStructUnique{}, DefaultSchema)

	db.InsertOrUpdate(&testStructUnique{A: 42, B: 43, C: "foo"})

	n := 0
	for i := 0; i < 1000; i++ {
		if rand.Int()%2 == 0 {
			if err := db.InsertOrUpdate(&testStructUnique{A: 42}); !IsUnique(err) {
				t.Error("Must have raised uniqueness error")
			}
			if err := db.InsertOrUpdate(&testStructUnique{B: 43}); !IsUnique(err) {
				t.Error("Must have raised uniqueness error")
			}
			if err := db.InsertOrUpdate(&testStructUnique{C: "foo"}); !IsUnique(err) {
				t.Error("Must have raised uniqueness error")
			}
		} else {
			ts := testStructUnique{A: rand.Int(), B: rand.Int31()}
			ts.C = fmt.Sprintf("bar%d%d", ts.A, ts.B)
			if ts.A != 42 && ts.B != 43 {
				if err := db.InsertOrUpdate(&ts); err != nil {
					t.Error(err)
				}
				// reinserting same object should work
				if err := db.InsertOrUpdate(&ts); err != nil {
					t.Error(err)
				}
				n++
			}
		}
	}

	// closing DB
	if err := db.Close(); err != nil {
		t.Error(err)
	}

	// reopening
	db = Open(dbpath)
	// test inserting after re-opening
	if err := db.InsertOrUpdate(&testStructUnique{A: 42}); !IsUnique(err) {
		t.Error("Must have raised uniqueness error")
	}

	if o, err := db.Search(&testStructUnique{}, "A", "=", 42).One(); err != nil {
		t.Error(err)
	} else {
		t.Logf("Found object, deleting it: %v", o)
		if err = db.Delete(o); err != nil {
			t.Error(err)
		}
	}

	// closing DB
	if err := db.Close(); err != nil {
		t.Error(err)
	}

	// reopening
	t.Logf("Reopening DB")
	db = Open(dbpath)

	if _, err := db.Search(&testStructUnique{}, "A", "=", 42).One(); !IsNoObjectFound(err) {
		if err != nil && !IsNoObjectFound(err) {
			t.Error(err)
		}
		t.Error("Object should have been deleted")
	}

	if count, err := db.Count(&testStructUnique{}); err != nil {
		t.Error(err)
	} else if n != count {
		t.Errorf("Wrong number of objects in DB, expects %d != %d", n, count)
	} else {
		t.Logf("%d objects in DB", count)
	}
}

func TestDeleteUniqueObject(t *testing.T) {
	cleanup()

	size := 1000
	db := Open(dbpath)
	defer controlDB(t, db)

	db.Create(&testStructUnique{}, DefaultSchema)

	// inserting objects
	for i := 0; i < size; {
		ts := testStructUnique{A: rand.Int(), B: rand.Int31(), C: fmt.Sprintf("bar-%d", rand.Int())}
		if err := db.insertOrUpdate(&ts, false); err != nil && !IsUnique(err) {
			t.Error(err)
			t.FailNow()
		} else if !IsUnique(err) {
			i++
		}
	}
	db.Close()

	db = Open(dbpath)
	defer controlDB(t, db)

	// we control that we have the good number of objects
	if n, err := db.Count(&testStructUnique{}); err != nil {
		t.Error(err)
	} else if n != size {
		t.Errorf("Wrong number of objects in DB, expects %d != %d", size, n)
	}

	ndel := 0
	deleted := make([]Object, 0)
	if obj, err := db.All(&testStructUnique{}); err != nil {
		t.Error(err)
	} else {
		// deleting objects
		for _, o := range obj {
			if rand.Int()%2 == 0 {
				db.delete(o)
				deleted = append(deleted, o)
				ndel++
			}
		}
	}
	db.Close()

	db = Open(dbpath)
	defer controlDB(t, db)

	// we check that all objects deleted are not in the DB anymore
	for _, o := range deleted {
		ts := o.(*testStructUnique)
		if _, err := db.Search(&testStructUnique{}, "A", "=", ts.A).
			Or("B", "=", ts.B).
			Or("C", "=", ts.C).One(); !IsNoObjectFound(err) {
			if err != nil {
				t.Error(err)
			} else {
				t.Error("Object must not be in database")
			}
		}
	}

	// we control that we have the good number of objects
	if n, err := db.Count(&testStructUnique{}); err != nil {
		t.Error(err)
	} else if n != size-ndel {
		t.Errorf("Wrong number of objects in DB, expects %d != %d", size, n)
	}

}
