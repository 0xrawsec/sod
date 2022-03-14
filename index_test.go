package sod

import (
	"errors"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/0xrawsec/toast"
)

func TestSearchNoIndexObject(t *testing.T) {
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, DefaultSchema)
	defer db.Close()

	start := time.Now()
	if sr := db.Search(&testStruct{}, "A", "<", 10).And("C", "=", "bar").And("B", ">", 10); sr.Err() != nil {
		t.Log(errors.Is(sr.Err(), ErrFieldNotIndexed))
		t.Error(sr.Err())
	} else {
		t.Log(sr.Len())
		t.Logf("Search time: %s", time.Since(start))
		if objects, err := sr.Collect(); err != nil {
			t.Error(err)
		} else {
			for _, o := range objects {
				ts := o.(*testStruct)
				switch {
				case ts.A >= 10:
					t.Error("A must be lesser than 10")
				case ts.B <= 10:
					t.Error("B must be greater than 10")
				case ts.C != "bar":
					t.Error("C must be equal to \"bar\"")
				}
			}
		}
	}
}

func TestSearchIndexObject(t *testing.T) {
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, DefaultSchema)
	defer db.Close()

	start := time.Now()
	if sr := db.Search(&testStruct{}, "A", "<", 10).And("C", "=", "bar").And("B", ">", 10); sr.Err() != nil {
		t.Log(errors.Is(sr.Err(), ErrFieldNotIndexed))
		t.Error(sr.Err())
	} else {
		t.Log(sr.Len())
		t.Logf("Search time: %s", time.Since(start))
		if objects, err := sr.Collect(); err != nil {
			t.Error(err)
		} else {
			for _, o := range objects {
				ts := o.(*testStruct)
				switch {
				case ts.A >= 10:
					t.Error("A must be lesser than 10")
				case ts.B <= 10:
					t.Error("B must be greater than 10")
				case ts.C != "bar":
					t.Error("C must be equal to \"bar\"")
				}
			}
		}
	}
}

func TestSearchOr(t *testing.T) {
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, DefaultSchema)
	defer db.Close()

	start := time.Now()
	if sr := db.Search(&testStruct{}, "A", "<", 10).Or("C", "=", "bar"); sr.Err() != nil {
		t.Log(errors.Is(sr.Err(), ErrFieldNotIndexed))
		t.Error(sr.Err())
	} else {
		t.Log(sr.Len())
		t.Logf("Search time: %s", time.Since(start))
		if objects, err := sr.Collect(); err != nil {
			t.Error(err)
		} else {
			for _, o := range objects {
				ts := o.(*testStruct)
				if !(ts.A < 10 || ts.C == "bar") {
					t.Errorf("Or failed")
				}
			}
		}
	}
}

func TestSearchDeleteObject(t *testing.T) {
	var s *Schema
	var err error

	deln := 0
	size := 10000
	os.RemoveAll(dbpath)
	db := createFreshTestDb(size, DefaultSchema)
	defer db.Close()

	if s, err = db.Schema(&testStruct{}); err != nil {
		t.Error(err)
		t.FailNow()
	}

	for fn, fi := range s.ObjectsIndex.Fields {
		t.Logf("FieldIndex %s size: %d", fn, fi.Len())
	}

	if s, err := db.All(&testStruct{}); err != nil {
		t.Error(err)
	} else {
		// deleting deln objects
		for _, o := range s {
			if rand.Int()%2 == 0 {
				t := o.(*testStruct)
				db.delete(t)
				deln++
			}
		}
	}

	if s.ObjectsIndex.Len() != size-deln {
		t.Errorf("Expecting index length of %d got %d", size-deln, s.ObjectsIndex.Len())
	}

	for fn, fi := range s.ObjectsIndex.Fields {
		t.Logf("FieldIndex %s size after deletion: %d", fn, fi.Len())
		if fi.Len() != size-deln {
			t.Errorf("Expecting field index (%s) length of %d got %d", fn, size-deln, fi.Len())
		}
	}

	start := time.Now()
	if sr := db.Search(&testStruct{}, "A", "<", 10).And("C", "=", "bar").And("B", ">", 10); sr.Err() != nil {
		t.Log(errors.Is(sr.Err(), ErrFieldNotIndexed))
		t.Error(sr.Err())
	} else {
		t.Log(sr.Len())
		t.Logf("Search time: %s", time.Since(start))
		if objects, err := sr.Collect(); err != nil {
			t.Error(err)
		} else {
			for _, o := range objects {
				ts := o.(*testStruct)
				switch {
				case ts.A >= 10:
					t.Error("A must be lesser than 10")
				case ts.B <= 10:
					t.Error("B must be greater than 10")
				case ts.C != "bar":
					t.Error("C must be equal to \"bar\"")
				}
			}
		}
	}
}

func TestFieldPath(t *testing.T) {

	tt := toast.FromT(t)

	s := &nestedStruct{
		A: 41,
		B: 42,
		C: 43,
		In: inner{
			D: 44,
			E: 45,
			F: "F",
		},
	}
	s.In.Anon.G = "G"

	i, ok := fieldByName(s, fieldPath("A"))
	tt.Assert(ok)
	tt.Assert(i.(int) == 41)

	i, ok = fieldByName(s, fieldPath("In.D"))
	tt.Assert(ok)
	tt.Assert(i.(float64) == 44)

	i, ok = fieldByName(s, fieldPath("In.Anon.G"))
	tt.Assert(ok)
	tt.Assert(i.(string) == "G")

	i, ok = fieldByName(s, fieldPath(""))
	tt.Assert(!ok)

}
