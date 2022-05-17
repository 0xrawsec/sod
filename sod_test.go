package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0xrawsec/toast"
)

const (
	dbroot = "data/"
)

func init() {
	os.RemoveAll(dbroot)
}

type testStruct struct {
	Item
	A      int       `sod:"index"`
	B      int       `sod:"index"`
	C      string    `sod:"index"`
	D      int16     `sod:"index"`
	E      int32     `sod:"index"`
	F      int64     `sod:"index"`
	G      uint8     `sod:"index"`
	H      uint16    `sod:"index"`
	I      uint32    `sod:"index"`
	J      uint64    `sod:"index"`
	K      float64   `sod:"index"`
	L      int8      `sod:"index"`
	M      time.Time `sod:"index"`
	N      uint
	O      string
	Upper  string `sod:"upper,index"`
	Lower  string `sod:"lower"`
	Nested *nestedStruct
	Ptr    *int
}

type testStructUnique struct {
	Item
	A int    `sod:"unique"`
	B int32  `sod:"unique"`
	C string `sod:"unique"`
}

func jsonOrPanic(i interface{}) string {
	if b, err := json.MarshalIndent(i, "", "  "); err != nil {
		panic(err)
	} else {
		return string(b)
	}
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
			o := "bar"
			if rand.Int()%2 == 0 {
				c = "bar"
				o = "foo"
			}
			k := 42
			ts := &testStruct{
				A:     randMod(42),
				B:     randMod(42),
				C:     c,
				D:     int16(randMod(42)),
				E:     int32(randMod(42)),
				F:     int64(randMod(42)),
				G:     uint8(randMod(42)),
				H:     uint16(randMod(42)),
				I:     uint32(randMod(42)),
				K:     float64(randMod(42)) * 42.42 * 0.42,
				L:     int8(randMod(42)),
				M:     time.Now(),
				N:     uint(randMod(42)),
				O:     o,
				Upper: "upper",
				Lower: "lower",
				Ptr:   &k,
			}
			co <- ts
		}
	}()
	return co
}

func randDBPath() string {
	return fmt.Sprintf("data/database-%d", rand.Uint64())
}

func createFreshTestDb(n int, s Schema) *DB {
	//s.AsyncWrites = &Async{Enable: true, Timeout: 5}
	db := Open(randDBPath())
	if err := db.Create(&testStruct{}, s); err != nil {
		panic(err)
	}
	if err := db.InsertOrUpdateBulk(genTestStructs(n), n/5); err != nil {
		panic(err)
	}
	return db
}

func closeAndReOpen(db *DB) *DB {
	if err := db.Close(); err != nil {
		panic(err)
	}
	return Open(db.root)
}

func controlDBSize(t *testing.T, db *DB, o Object, n int) {
	if c, err := db.Count(o); err != nil {
		t.Error(err)
		t.FailNow()
	} else if n != c {
		t.Errorf("Wrong size, expecting %d, got %d", n, c)
		t.FailNow()
	}
}

func controlDB(t *testing.T, db *DB) {
	if err := db.Control(); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func assert(test bool, message string) {
	if !test {
		panic(message)
	}
}

func TestSnakeCase(t *testing.T) {
	assert(camelToSnake("TestTest") == "test_test", "Unexpected snake case")
	assert(camelToSnake("TestTEST") == "test_test", "Unexpected snake case")
	assert(camelToSnake("OneTWOThree") == "one_two_three", "Unexpected snake case")
	assert(camelToSnake("One2Three") == "one_2_three", "Unexpected snake case")
	assert(camelToSnake("One23") == "one_23", "Unexpected snake case")
	assert(camelToSnake("1Step2Step") == "1_step_2_step", "Unexpected snake case")
	assert(camelToSnake("123") == "123", "Unexpected snake case")
}

func TestTypeof(t *testing.T) {
	if typeof(&testStruct{}) != typeof(testStruct{}) {
		t.Error("Unexpected typeof behaviour")
	}
}

func TestSimpleDb(t *testing.T) {
	t.Parallel()
	db := createFreshTestDb(0, DefaultSchema)
	defer controlDB(t, db)

	tt := toast.FromT(t)
	db.Create(&testStruct{}, DefaultSchema)

	t1 := testStruct{A: 1, B: 2, C: "Test"}
	tt.CheckErr(db.InsertOrUpdate(&t1))
	t.Log(&t1)

	ts := testStruct{}

	_, err := db.Get(&ts)
	tt.ExpectErr(err, os.ErrNotExist)

	_, err = db.Get(&t1)
	tt.CheckErr(err)

	o, err := db.GetByUUID(&testStruct{}, t1.UUID())
	tt.CheckErr(err)
	t.Log(o)
}

func TestGetAll(t *testing.T) {
	t.Parallel()

	var tsSlice []*testStruct
	var db *DB

	tt := toast.FromT(t)
	count := 10000
	tt.TimeIt("Inserting",
		func() {
			db = createFreshTestDb(count, DefaultSchema)
		})

	defer controlDB(t, db)

	tt.TimeIt("db.All", func() {
		s, err := db.All(&testStruct{})
		tt.CheckErr(err)
		tt.Assert(len(s) == count)
	})

	n, err := db.Count(&testStruct{})
	tt.CheckErr(err)
	tt.Assert(n == count)

	tt.TimeIt("db.AssignAll", func() {
		tt.CheckErr(db.AssignAll(&testStruct{}, &tsSlice))
		tt.Assert(len(tsSlice) == count)
	})
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
	t.Parallel()
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	if s, err := db.All(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		t.Log("Corrupting files")
		if schema, err := db.Schema(&testStruct{}); err != nil {
			t.Error(err)
		} else {
			for _, o := range s {
				corruptFile(db.oPath(schema, o))
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
	t.Parallel()
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

		if _, err := os.Stat(db.root); err == nil {
			t.Errorf("Database must have been deleted")
		}

	}
}
func TestSchema(t *testing.T) {
	var err error

	t.Parallel()
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
	db.Close()

	db = Open(db.root)
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

	t.Parallel()
	tt := toast.FromT(t)
	size := 100
	db := createFreshTestDb(size, DefaultSchema)

	tt.CheckErr(db.Close())

	db = Open(db.root)
	defer tt.CheckErr(db.Control())
	defer tt.CheckErr(db.Close())

	s, err = db.Schema(&testStruct{})
	tt.CheckErr(err)

	// we insert some more data
	for i := 0; i < size; i++ {
		c := "foo"
		if rand.Int()%2 == 0 {
			c = "bar"
		}
		tt.CheckErr(db.InsertOrUpdate(&testStruct{A: rand.Int() % 42, B: rand.Int() % 42, C: c}))
	}

	t.Logf("Index size: %d", s.ObjectIndex.len())

	var res []*testStruct
	tt.CheckErr(db.Search(&testStruct{}, "A", "<", 10).Assign(&res))
	t.Logf("Search result len: %d", len(res))
	for _, ts := range res {
		tt.Assert(ts.A < 10)
	}
}

func TestUpdateObject(t *testing.T) {
	var out []*testStruct

	t.Parallel()
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
	defer db.Close()

	tt := toast.FromT(t)

	s, err := db.All(&testStruct{})
	tt.CheckErr(err)

	for _, o := range s {
		ts := o.(*testStruct)
		ts.A = 42
		ts.B = 4242
		ts.C = "foobar"
		tt.CheckErr(db.InsertOrUpdate(ts))
	}

	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).And("B", "=", 4242).And("C", "=", "foobar").Assign(&out))
	tt.Assert(len(out) == size)

	for _, ts := range out {
		ts.C = "foofoo"
		tt.CheckErr(db.InsertOrUpdate(ts))
	}

	// we should not find any struct with C=foobar because we just change them
	tt.CheckErr(db.Search(&testStruct{}, "C", "=", "foobar").Assign(&out))
	tt.Assert(len(out) == 0)

	// all elements should have C=foofoo
	tt.CheckErr(db.Search(&testStruct{}, "C", "=", "foofoo").Assign(&out))
	tt.Assert(len(out) == size)

	schema, err := db.Schema(&testStruct{})
	tt.CheckErr(err)
	tt.CheckErr(schema.control())
}

func TestIndexAllTypes(t *testing.T) {

	t.Parallel()
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
	db.Close()

	db = Open(db.root)
	defer db.Close()

	if sr, err := db.Search(&testStruct{}, "A", "<", 42).
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
		Operation("AND", "M", "<", time.Now()).
		Operation("&&", "N", "<", uint(42)).
		Operation("and", "O", "~=", "(?i:(FOO|BAR))").
		Collect(); err != nil {
		t.Error(err)
	} else if len(sr) == 0 {
		t.Error("Expecting some results")
	}
}

func TestRegexSearch(t *testing.T) {
	var err error
	var eqOnC, rexOnO []Object

	t.Parallel()
	size := 1000
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)
	db.Close()

	db = Open(db.root)
	defer db.Close()

	if eqOnC, err = db.Search(&testStruct{}, "C", "=", "foo").
		Collect(); err != nil {
		t.Error(err)
	}

	if rexOnO, err = db.Search(&testStruct{}, "O", "~=", "(?i:BAR)").
		Collect(); err != nil {
		t.Error(err)
	}

	if eqOnO, err := db.Search(&testStruct{}, "O", "=", "bar").Collect(); err != nil {
		t.Error(err)
	} else if len(rexOnO) != len(eqOnO) {
		t.Error("Both searches must have same number of elements")
	}

	if rexOnC, err := db.Search(&testStruct{}, "C", "~=", "(?i:Foo)").Collect(); err != nil {
		t.Error(err)
	} else if len(eqOnC) != len(rexOnC) {
		t.Error("Both searches must have same number of elements")
	}

	// test set is created so that when C==foo -> O==bar
	if len(eqOnC) != len(rexOnO) {
		t.Error("Both searches must have same number of elements")
	}
}

func TestSearchOrder(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

	t.Parallel()
	size := 100
	db := createFreshTestDb(size, DefaultSchema)
	defer controlDB(t, db)

	if s := db.Search(&testStruct{}, "A", "<>", 42).And("B", "=", 42).Operation("or", "C", "=", "bar"); !errors.Is(s.Err(), ErrUnkownSearchOperator) {
		t.Error("Should have raised error")
	}
}

func TestUnknownObject(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

	db = Open(db.root)
	// controlling size after re-opening the DB
	if n, err := db.Count(&testStruct{}); err != nil {
		t.Error(err)
	} else if n != size-ndel {
		t.Errorf("Bulk delete failed expecting size=%d, got %d", size-ndel, n)
	}
}

func TestUniqueObject(t *testing.T) {
	var uninit *testStructUnique
	t.Parallel()

	tt := toast.FromT(t)
	db := createFreshTestDb(0, DefaultSchema)

	tt.CheckErr(db.Create(&testStructUnique{}, DefaultSchema))
	tt.CheckErr(db.InsertOrUpdate(&testStructUnique{A: 42, B: 43, C: "foo"}))

	n := 0
	for i := 0; i < 1000; i++ {
		if rand.Int()%2 == 0 {
			tt.ExpectErr(db.InsertOrUpdate(&testStructUnique{A: 42}), ErrFieldUnique)
			tt.ExpectErr(db.InsertOrUpdate(&testStructUnique{B: 43}), ErrFieldUnique)
			tt.ExpectErr(db.InsertOrUpdate(&testStructUnique{C: "foo"}), ErrFieldUnique)
		} else {
			ts := testStructUnique{A: rand.Int(), B: rand.Int31()}
			ts.C = fmt.Sprintf("bar%d%d", ts.A, ts.B)
			if ts.A != 42 && ts.B != 43 {
				tt.CheckErr(db.InsertOrUpdate(&ts))
				// reinserting same object should work
				tt.CheckErr(db.InsertOrUpdate(&ts))
				n++
			}
		}
	}

	// closing DB
	if err := db.Close(); err != nil {
		t.Error(err)
	}

	// reopening
	db = Open(db.root)
	// test inserting after re-opening
	tt.ExpectErr(db.InsertOrUpdate(&testStructUnique{A: 42}), ErrFieldUnique)

	tt.ShouldPanic(func() { db.Search(&testStructUnique{}, "A", "=", 42).AssignOne(nil) })
	tt.ExpectErr(db.Search(&testStructUnique{}, "A", "=", 42).Expects(2).AssignOne(&uninit), ErrUnexpectedNumberOfResults)
	tt.CheckErr(db.Search(&testStructUnique{}, "A", "=", 42).Expects(1).AssignOne(&uninit))
	tt.CheckErr(db.Search(&testStructUnique{}, "A", "=", 42).AssignUnique(&uninit))
	tt.Assert(uninit.A == 42)

	ts := &testStructUnique{}
	tt.CheckErr(db.Search(&testStructUnique{}, "A", "=", 42).AssignOne(&ts))
	tt.Assert(ts.A == 42 && ts.B == 43 && ts.C == "foo")
	// we delete object
	tt.CheckErr(db.Delete(ts))

	// closing DB
	tt.CheckErr(db.Close())

	// reopening
	t.Logf("Reopening DB")
	db = Open(db.root)

	tt.ExpectErr(db.Search(&testStructUnique{}, "A", "=", 42).AssignOne(&ts), ErrNoObjectFound)

	count, err := db.Count(&testStructUnique{})
	tt.CheckErr(err)
	tt.Assert(n == count)
	t.Logf("%d objects in DB", count)
}

func TestDeleteUniqueObject(t *testing.T) {
	t.Parallel()

	size := 1000
	db := createFreshTestDb(0, DefaultSchema)
	tt := toast.FromT(t)

	db.Create(&testStructUnique{}, DefaultSchema)

	s, err := db.Schema(&testStructUnique{})
	tt.CheckErr(err)

	// inserting unique objects
	for i := 0; i < size; {
		ts := testStructUnique{A: rand.Int(), B: rand.Int31(), C: fmt.Sprintf("bar-%d", rand.Int())}
		if err := db.insertOrUpdate(s, &ts, false); err != nil && !IsUnique(err) {
			t.Error(err)
			t.FailNow()
		} else if !IsUnique(err) {
			i++
		}
	}
	db.Close()

	db = Open(db.root)
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

	db = Open(db.root)
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

func stress(t *testing.T, db *DB, jobs int) {
	wg := sync.WaitGroup{}

	if cnt, err := db.Count(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		t.Logf("Stressing out db with %d object", cnt)
	}

	for i := 0; i < jobs; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()
			var s []*testStruct

			time.Sleep(time.Millisecond * time.Duration(rand.Int()%500))
			t.Logf("starting job %d", n)
			timer := time.Now()

			if err := db.Search(&testStruct{}, "A", "<", rand.Int()%42).Assign(&s); err != nil {
				t.Error(err)
			} else {
				t.Logf("job %d: search time = %s", n, time.Since(timer))
				t.Logf("job %d: modifying %d objects", n, len(s))
				timer = time.Now()
				k := randMod(42)
				for _, ts := range s {
					//new := copyObject(ts).(*testStruct)
					new := ts
					new.A = 4242
					new.M = time.Now()
					new.Ptr = &k
					db.InsertOrUpdate(new)
					if rand.Int()%5 == 0 {
						if err := db.Flush(new); err != nil {
							t.Error(err)
						}
					}
				}
				delta := time.Since(timer)
				timePerObj := time.Duration(0)
				if len(s) > 0 {
					timePerObj = time.Duration(float64(delta) / float64(len(s)))
				}
				t.Logf("job %d: #obj=%d mod_time=%s (%s per/obj)", n, len(s), delta, timePerObj)
			}
			t.Logf("stopping job %d", n)
		}()
	}
	wg.Wait()
}

func TestAsyncWrites(t *testing.T) {
	t.Parallel()

	tt := toast.FromT(t)
	size := 5000
	timeout := 500 * time.Millisecond
	s := DefaultSchema
	s.Asynchrone(500, timeout)

	db := createFreshTestDb(size, s)

	controlDBSize(t, db, &testStruct{}, size)

	db = closeAndReOpen(db)

	controlDBSize(t, db, &testStruct{}, size)

	search := db.Search(&testStruct{}, "A", "<", 10)
	if err := search.Delete(); err != nil {
		t.Error(err)
	}
	t.Logf("Deleted %d objects", search.Len())
	controlDBSize(t, db, &testStruct{}, size-search.Len())

	t.Logf("Close and repopen")
	db = closeAndReOpen(db)
	controlDBSize(t, db, &testStruct{}, size-search.Len())

	t.Logf("Stressing out")
	stress(t, db, 10)
	db = closeAndReOpen(db)
	controlDBSize(t, db, &testStruct{}, size-search.Len())
	t.Logf("dropping db: %s", db.root)
	tt.CheckErr(db.Drop())

	// we wait two timeouts before checking
	time.Sleep(2 * timeout)
	tt.Assert(!isDirAndExist(db.root))
}

func TestAsyncWritesFastDelete(t *testing.T) {
	t.Parallel()
	// there is a bug when a file is deleted while it has not
	// been written yet to disk. This test checks for that bug
	tt := toast.FromT(t)
	size := 1000
	s := DefaultSchema
	timeout := 500 * time.Millisecond
	s.Asynchrone(100, timeout)

	db := createFreshTestDb(size, s)

	controlDBSize(t, db, &testStruct{}, size)

	search := db.Search(&testStruct{}, "A", "<", 10)
	if err := search.Delete(); err != nil {
		t.Error(err)
	}
	t.Logf("Deleted %d objects", search.Len())
	controlDBSize(t, db, &testStruct{}, size-search.Len())

	db = closeAndReOpen(db)
	controlDBSize(t, db, &testStruct{}, size-search.Len())

	stress(t, db, 10)
	db = closeAndReOpen(db)
	controlDBSize(t, db, &testStruct{}, size-search.Len())
	tt.CheckErr(db.Drop())
	// we wait two timeouts before checking
	time.Sleep(2 * timeout)
	tt.Assert(!isDirAndExist(db.root))
}

type invalidStruct struct {
	Item
	A int
}

func (s *invalidStruct) Validate() error {
	if s.A == 42 {
		return errors.New("A must not be 42")
	}
	return nil
}

func TestValidation(t *testing.T) {
	t.Parallel()
	tt := toast.FromT(t)
	db := createFreshTestDb(0, DefaultSchema)

	db.Create(&invalidStruct{}, DefaultSchema)

	tt.CheckErr(db.InsertOrUpdate(&invalidStruct{A: 41}))
	tt.ExpectErr(db.InsertOrUpdate(&invalidStruct{A: 42}), ErrInvalidObject)

	structs := make([]*invalidStruct, 0)
	for i := 0; i < 1000; i++ {
		structs = append(structs, &invalidStruct{A: rand.Int() % 42})
	}

	tt.CheckErr(db.InsertOrUpdateMany(ToObjectSlice(structs)...))
	tt.CheckErr(db.InsertOrUpdateBulk(ToObjectChan(structs), 42))

	structs = make([]*invalidStruct, 0)
	for i := 0; i < 1000; i++ {
		structs = append(structs, &invalidStruct{A: rand.Int() % 43})
	}

	tt.ExpectErr(db.InsertOrUpdateMany(ToObjectSlice(structs)...), ErrInvalidObject)
	tt.ExpectErr(db.InsertOrUpdateBulk(ToObjectChan(structs), 42), ErrInvalidObject)
}

func TestAssign(t *testing.T) {
	t.Parallel()
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	tt := toast.FromT(t)

	var ts *testStruct
	tt.CheckErr(db.Search(&testStruct{}, "A", "<", 42).AssignOne(&ts))
	t.Log(ts)
	tt.ShouldPanic(func() { db.Search(&testStruct{}, "A", "<", 21).AssignOne(ts) })

	var s []*testStruct
	tt.CheckErr(db.Search(&testStruct{}, "A", "<", 21).And("B", ">", 21).Assign(&s))
	t.Log(len(s))
	for _, ts := range s {
		tt.Assert(ts.A < 21)
		t.Log(ts)
	}

	// should panic because s is not a *[]Object
	tt.ShouldPanic(func() { db.Search(&testStruct{}, "A", "<", 21).Assign(s) })
	// should panic because ts is not a slice
	tt.ShouldPanic(func() { db.Search(&testStruct{}, "A", "=", 0).Assign(&ts) })
}

func TestNestedStruct(t *testing.T) {
	var ts *testStruct

	t.Parallel()
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	tt := toast.FromT(t)
	tt.CheckErr(db.Search(&testStruct{}, "A", "<", 42).AssignOne(&ts))
	tt.CheckErr(db.Search(&testStruct{}, "Nested.C", "<", 42.0).AssignOne(&ts))
	t.Log(ts)
}

func TestConstraintTransform(t *testing.T) {
	var out []*testStruct

	t.Parallel()
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	tt := toast.FromT(t)

	tt.CheckErr(db.Search(&testStruct{}, "A", "<=", 42).Assign(&out))

	for _, ts := range out {
		tt.Assert(ts.Upper == strings.ToUpper(ts.Upper))
		tt.Assert(ts.Lower == strings.ToLower(ts.Lower))
	}

	tt.CheckErr(db.Search(&testStruct{}, "Upper", "=", "upper").Assign(&out))
	t.Log(len(out))
	tt.Assert(len(out) == 100)

	tt.CheckErr(db.Search(&testStruct{}, "Lower", "=", "LOWER").Assign(&out))
	t.Log(len(out))
	tt.Assert(len(out) == 100)
}

func TestRepairFull(t *testing.T) {
	var db *DB

	t.Parallel()
	tt := toast.FromT(t)
	count := 10000

	tt.TimeIt("creating DB", func() { db = createFreshTestDb(count, DefaultSchema) })
	defer controlDB(t, db)

	tt.CheckErr(db.deleteSchema(&testStruct{}))
	db = closeAndReOpen(db)
	defer db.Close()
	// we should get an error since the schema is not there anymore
	tt.ExpectErr(db.Search(&testStruct{}, "A", "<=", 42).Err(), os.ErrNotExist)

	tt.ExpectErr(db.Create(&testStruct{}, DefaultSchema), ErrIndexCorrupted)
	s, err := db.Schema(&testStruct{})
	tt.ExpectErr(err, ErrIndexCorrupted)

	tt.TimeIt("controlling", func() { tt.ExpectErr(s.control(), ErrIndexCorrupted) })
	tt.TimeIt("reindexing full DB", func() { tt.CheckErr(db.Repair(&testStruct{})) })
	tt.TimeIt("controlling repaired", func() { tt.CheckErr(s.control()) })
}

func TestRepairPartial(t *testing.T) {
	var db *DB

	t.Parallel()
	tt := toast.FromT(t)
	count := 10000
	corruptPerc := 0.1
	del := int(float64(count) * corruptPerc)

	tt.TimeIt("creating DB", func() { db = createFreshTestDb(count, DefaultSchema) })
	odir := db.oDir(&testStruct{})

	// we corrupt schema
	s, err := db.Schema(&testStruct{})
	tt.CheckErr(err)
	uuids, err := uuidsFromDir(odir)
	tt.CheckErr(err)

	t.Logf("Corrupting %d entries (%.2f%%)", del, corruptPerc*100)
	for del > 0 {
		for uuid := range uuids {
			if del == 0 {
				break
			}
			if randMod(10) > 5 {
				continue
			}

			if randMod(10) <= 5 {
				s.ObjectIndex.deleteByUUID(uuid)
			} else {
				tt.CheckErr(os.Remove(filepath.Join(odir, s.filenameFromUUID(uuid))))
			}

			delete(uuids, uuid)
			del--
		}
	}

	// we create a new schema
	tt.CheckErr(db.Create(&testStruct{}, DefaultSchema))

	db = closeAndReOpen(db)
	defer db.Close()

	s, err = db.Schema(&testStruct{})
	tt.ExpectErr(err, ErrIndexCorrupted)
	tt.TimeIt("controlling", func() { tt.ExpectErr(s.control(), ErrIndexCorrupted) })
	tt.TimeIt("reindexing partial DB", func() { tt.CheckErr(db.Repair(&testStruct{})) })
	s, err = db.Schema(&testStruct{})
	tt.CheckErr(err)
	tt.TimeIt("controlling repaired", func() { tt.CheckErr(s.control()) })
}

func TestBuggySchema(t *testing.T) {
	/* There is a bug at schema creation, when using
	a custom schema because we compare a custom FieldDescriptors
	with FieldDescriptors of the Object */
	tt := toast.FromT(t)
	count := 100

	fds := FieldDescriptors(&testStruct{})
	fds.Constraint("A", Constraints{Upper: true})
	custom := NewCustomSchema(fds, DefaultExtension)

	db := createFreshTestDb(count, custom)
	db = closeAndReOpen(db)

	tt.CheckErr(db.Create(&testStruct{}, custom))

	db = closeAndReOpen(db)
	tt.ExpectErr(db.Create(&testStruct{}, DefaultSchema), ErrFieldDescModif)
}

func TestSearchExpect(t *testing.T) {
	t.Parallel()
	var ts *testStruct
	tt := toast.FromT(t)
	db := createFreshTestDb(0, DefaultSchema)
	defer controlDB(t, db)

	tt.CheckErr(db.InsertOrUpdate(&testStruct{A: 42}))
	tt.ExpectErr(db.Search(&testStruct{}, "A", "=", 40).Expects(1).Err(), ErrUnexpectedNumberOfResults)

	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 40).Expects(0).Err())
	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 40).ExpectsZeroOrN(1).Err())
	tt.ExpectErr(db.Search(&testStruct{}, "A", "=", 40).AssignUnique(&ts), ErrNoObjectFound)

	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).ExpectsZeroOrN(1).Err())
	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).AssignUnique(&ts))

	// adding another object with same field
	tt.CheckErr(db.InsertOrUpdate(&testStruct{A: 42}))
	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).Expects(2).Err())
	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).ExpectsZeroOrN(2).Err())
	tt.ExpectErr(db.Search(&testStruct{}, "A", "=", 42).AssignUnique(&ts), ErrUnexpectedNumberOfResults)
}

func TestAssignIndex(t *testing.T) {

	t.Parallel()
	tt := toast.FromT(t)
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	var strIndex []string
	var intIndex []int
	var floatIndex []float32
	var timeIndex []time.Time

	// testing string index
	tt.CheckErr(db.AssignIndex(&testStruct{}, "C", &strIndex))
	t.Log(strIndex)

	// testing int index
	tt.CheckErr(db.AssignIndex(&testStruct{}, "A", &intIndex))
	t.Log(intIndex)

	// testing float index
	tt.CheckErr(db.AssignIndex(&testStruct{}, "K", &floatIndex))
	t.Log(floatIndex)

	tt.CheckErr(db.AssignIndex(&testStruct{}, "M", &timeIndex))
	t.Log(timeIndex)

	// testing non-existing index
	tt.ExpectErr(db.AssignIndex(&testStruct{}, "N", &floatIndex), ErrUnindexedField)

	tt.ShouldPanic(func() { db.AssignIndex(&testStruct{}, "A", intIndex) })
}

func TestBugCasting(t *testing.T) {
	// there is a bug when a value is searched before anything got inserted in the index
	t.Parallel()
	tt := toast.FromT(t)
	count := 0
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	tt.CheckErr(db.Search(&testStruct{}, "A", "=", 42).Err())
}

func TestIndexCorruption(t *testing.T) {
	/*
		Bug that does not return ErrIndexCorrupted under some circumstances:
			1. schema got deleted (on-disk)
			2. sod.Create is used to create a new Schema
		Expected: sod.Create should return ErrIndexCorrupted error
	*/
	var db *DB

	t.Parallel()
	tt := toast.FromT(t)
	count := 100

	tt.TimeIt("creating DB", func() { db = createFreshTestDb(count, DefaultSchema) })
	odir := db.oDir(&testStruct{})
	schemaPath := filepath.Join(odir, SchemaFilename)

	// we close database
	tt.CheckErr(db.Close())

	// we delete schema manually
	tt.CheckErr(os.Remove(schemaPath))

	db = Open(db.root)

	tt.ExpectErr(db.Create(&testStruct{}, DefaultSchema), ErrIndexCorrupted)
}

func TestErrors(t *testing.T) {

	t.Parallel()
	count := 100
	db := createFreshTestDb(count, DefaultSchema)
	defer controlDB(t, db)

	tt := toast.FromT(t)

	var ts *testStruct
	type wrongStruct struct {
		Item
		Sub struct {
			A int
		}
	}
	var s []*testStruct

	tt.ExpectErr(db.Search(&testStruct{}, "UnknownField", "=", 0).AssignOne(&ts), ErrUnkownField)
	tt.ExpectErr(db.Search(&testStruct{}, "UnknownField", "=", 0).Assign(&s), ErrUnkownField)

	tt.ExpectErr(db.Search(&testStruct{}, "A", "=", 4242).AssignOne(&ts), ErrNoObjectFound)

	tt.ExpectErr(db.Search(&testStruct{}, "A", "<>", 0).AssignOne(&ts), ErrUnkownSearchOperator)
	tt.ExpectErr(db.Search(&testStruct{}, "A", "<>", 0).Assign(&s), ErrUnkownSearchOperator)

	// C is not an int type so we should raise a casting error
	tt.ExpectErr(db.Search(&testStruct{}, "C", "<", 0).Assign(&s), ErrCasting)
	// should raise an error on non indexed field
	tt.ExpectErr(db.Search(&testStruct{}, "N", ">", 0).Assign(&s), ErrCasting)

	var wrong *wrongStruct
	// testing to assign to a wrong Object
	tt.ShouldPanic(func() { db.Search(&testStruct{}, "A", "<=", 42).AssignOne(&wrong) })

	wrong = &wrongStruct{}
	wrong.Sub.A = 42
	db.Create(&wrongStruct{}, DefaultSchema)
	tt.CheckErr(db.InsertOrUpdate(wrong))
	tt.ExpectErr(db.Search(&wrongStruct{}, "Sub", "=", 42).AssignOne(&wrong), ErrUnknownKeyType)

	// should not raise any error as we have not change structure yet
	db = closeAndReOpen(db)
	tt.CheckErr(db.Create(&testStruct{}, DefaultSchema))

	db = closeAndReOpen(db)
	// we redefine a testStruct that does not correspond to the other one
	type testStruct struct {
		Item
		Sub struct {
			A int
		}
	}

	fake := &testStruct{}
	t.Log(db.Create(&testStruct{}, DefaultSchema))
	tt.ExpectErr(db.Create(&testStruct{}, DefaultSchema), ErrStructureChanged)
	tt.ExpectErr(db.Search(&testStruct{}, "A", "=", 42).AssignOne(&fake), ErrStructureChanged)
	tt.ExpectErr(db.InsertOrUpdate(&testStruct{}), ErrStructureChanged)
}
