package sod

import (
	"errors"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestSearchNoIndexObject(t *testing.T) {
	s := &Schema{Extension: ".json"}
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, s)
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
	s := &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, s)
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
	s := &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	os.RemoveAll(dbpath)
	db := createFreshTestDb(10000, s)
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
	deln := 0
	size := 10000
	s := &Schema{Extension: ".json", ObjectsIndex: NewIndex("A", "B", "C")}
	os.RemoveAll(dbpath)
	db := createFreshTestDb(size, s)
	defer db.Close()

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
				db.Delete(t)
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
