package sod

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/0xrawsec/toast"
)

type inner struct {
	D    float64
	E    int
	F    string `sod:"index"`
	Anon struct {
		G string `sod:"index"`
	}
}

type nestedStruct struct {
	Item
	A  int
	B  uint64
	C  float32 `sod:"index"`
	In inner
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func searchFieldOrPanic(value interface{}) *IndexedField {
	if sf, err := searchField(value); err != nil {
		panic(err)
	} else {
		return sf
	}
}

func randomIndex(size int) *fieldIndex {
	i := NewFieldIndex(FieldDescriptor{}, 0, size)
	for k := 0; k < size; k++ {
		i.Insert(rand.Int(), uint64(k))
	}
	return i
}

func shouldPanic(t *testing.T, f func()) {
	defer func() { recover() }()
	f()
	t.Errorf("should have panicked")
}

func TestFieldByName(t *testing.T) {
	type A struct {
		Aa int
	}

	type B struct {
		Item
		A
		Bb string
	}

	b := B{}
	b.A = A{42}

	if i, ok := fieldByName(&b, fieldPath("Aa")); !ok {
		t.Errorf("Must have found field")
	} else if i.(int) != 42 {
		t.Errorf("Unexpected interface value")
	}
}

func TestSimpleIndex(t *testing.T) {
	size := 1000
	i := NewFieldIndex(FieldDescriptor{}, 0, size)
	for k := 0; k < size; k++ {
		i.Insert(rand.Int()%50, uint64(k))
	}

	if !i.Control() {
		t.Errorf("index is not ordered")
	}
	for k := 0; k < size; k++ {
		i.Delete(uint64(k))
		// control after each deletion
		if !i.Control() {
			t.Errorf("index is not ordered")
		}
	}
}

func TestBigIndex(t *testing.T) {
	size := 50000
	i := randomIndex(size)

	for k := 0; k < size; k++ {
		i.Insert(rand.Int(), uint64(k))
	}

	if !i.Control() {
		t.Errorf("index is not ordered")
	}
}

func TestIndexJsonMarshal(t *testing.T) {
	var data []byte
	var err error
	var new *fieldIndex

	size := 1000
	i := randomIndex(size)
	if data, err = json.Marshal(i); err != nil {
		t.Error(err)
	}
	if err = json.Unmarshal(data, &new); err != nil {
		t.Error(err)
	}

	if new.Len() != i.Len() {
		t.Errorf("bad length after unmarshal")
	}

	for k := 0; k < size; k++ {
		new.Delete(uint64(k))
		// control after each deletion
		if !new.Control() {
			t.Errorf("index is not ordered")
		}
	}

}

func TestIndexSearchGreaterOrEqual(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		k := rand.Int() % i.Len()
		if k%2 == 0 {
			k = 0
		}
		sk := i.Index[k]
		s := i.SearchGreaterOrEqual(sk)
		if len(s) == 0 {
			t.Error("search result must not be empty")
		}
		for _, k := range s {
			if k.Less(sk) {
				t.Errorf("%s not greater than or equal to %s", k, sk)
			}
		}
	}

	i = randomIndex(0)
	s := i.SearchGreaterOrEqual(searchFieldOrPanic(42))
	tt.Assert(len(s) == 0)

}

func TestIndexSearchGreater(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchGreater(sk)
		for _, k := range s {
			tt.Assert(k.Greater(sk))
		}
	}
}

func TestIndexSearchLess(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLess(sk)
		for _, k := range s {
			tt.Assert(k.Less(sk))
		}
	}
}
func TestIndexSearchLessOrEqual(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLessOrEqual(sk)
		tt.Assert(len(s) != 0)
		for _, k := range s {
			tt.Assert(!k.Greater(sk))
		}
	}
}

func TestIndexSearchEqual(t *testing.T) {
	size := 10000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchEqual(sk)

		if len(s) == 0 {
			t.Error("search result must not be empty")
		}

		for _, k := range s {
			tt.Assert(k.Equal(sk))
		}
	}
}

func TestIndexSearchNotEqual(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		j := rand.Intn(len(i.Index))
		sk := i.Index[j]
		s := i.SearchNotEqual(sk)

		tt.Assert(len(s) > 0)

		for _, k := range s {
			tt.Assert(!k.Equal(sk))
		}
	}
}

func TestIndexConstrain(t *testing.T) {
	size := 30
	i := randomIndex(size)
	o := randomIndex(1000)

	tt := toast.FromT(t)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLess(sk)
		inter := o.Constrain(s)
		tt.Assert(inter.Len() == len(s))
		tt.Assert(inter.Len() <= i.Len())
		/*
			if inter.Len() != len(s) || inter.Len() > i.Len() {
				t.Errorf("bad intersection length")
			}*/
	}
	t.Log(i.Slice())
	t.Log(i)
}

func TestIndexEvaluate(t *testing.T) {
	size := 30
	i := randomIndex(size)

	for j := 0; j < size*2; j++ {
		one := i.Index[rand.Int()%i.Len()]
		other := i.Index[rand.Int()%i.Len()]
		one.Evaluate("=", other)
		one.Evaluate("!=", other)
		one.Evaluate("<", other)
		one.Evaluate("<=", other)
		one.Evaluate(">=", other)
		one.Evaluate(">", other)
		// this test should panic
		shouldPanic(t, func() { one.Evaluate("<>", other) })
	}
}

func newIndexedFieldOrPanic(i interface{}) *IndexedField {
	if f, err := NewIndexedField(i, rand.Uint64()); err != nil {
		panic(err)
	} else {
		return f
	}
}

func TestIndexEvaluateRegex(t *testing.T) {
	var field *IndexedField
	var rex *IndexedField

	field = newIndexedFieldOrPanic("Test")
	rex = newIndexedFieldOrPanic("Test")

	if !field.Evaluate("~=", rex) {
		t.Error("Should match")
	}

	if !field.Evaluate("~=", newIndexedFieldOrPanic("(?i:test)")) {
		t.Error("Should match")
	}

	if !field.Evaluate("~=", newIndexedFieldOrPanic(".*")) {
		t.Error("Should match")
	}

	if field.Evaluate("~=", newIndexedFieldOrPanic("test")) {
		t.Error("Should not match")
	}

	if field.Evaluate("~=", newIndexedFieldOrPanic("testtest")) {
		t.Error("Should not match")
	}
}

func TestIndexDelete(t *testing.T) {
	size := 10000
	i := randomIndex(size)
	del := 0

	for j := 0; j < i.Len(); j++ {
		if rand.Int()%2 == 0 {
			i.Delete(uint64(j))
			del++
		}
	}

	if i.Len() != size-del {
		t.Errorf("Wrong index size, expecting %d got %d", size-del, i.Len())
	}
}

func TestIndexUpdate(t *testing.T) {
	size := 10000
	i := randomIndex(size)
	for j := 0; j < i.Len(); j++ {
		i.Update(rand.Int(), uint64(j))
	}

	if i.Len() != size {
		t.Error("Wrong size after update")
	}
}

func TestBuildFieldDescriptors(t *testing.T) {

	//tt := toast.FromT(t)

	t.Log(FieldDescriptors(&testStruct{}))
	t.Log(FieldDescriptors(&nestedStruct{}))
	t.Log(ObjectFingerprint(&testStruct{}))

}
