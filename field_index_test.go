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

func searchFieldOrPanic(value interface{}) *indexedField {
	if sf, err := searchField(value); err != nil {
		panic(err)
	} else {
		return sf
	}
}

func randomIndex(size int) *fieldIndex {
	i := newFieldIndex(FieldDescriptor{Type: "uint64"}, 0, size)
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
	i := newFieldIndex(FieldDescriptor{Type: "uint64"}, 0, size)
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
			if k.less(sk) {
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
			tt.Assert(k.greater(sk))
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
			tt.Assert(k.less(sk))
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
			tt.Assert(!k.greater(sk))
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
			tt.Assert(k.equal(sk))
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
			tt.Assert(!k.equal(sk))
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
		one.evaluate("=", other)
		one.evaluate("!=", other)
		one.evaluate("<", other)
		one.evaluate("<=", other)
		one.evaluate(">=", other)
		one.evaluate(">", other)
		// this test should panic
		shouldPanic(t, func() { one.evaluate("<>", other) })
	}
}

func newIndexedFieldOrPanic(i interface{}) *indexedField {
	if f, err := newIndexedField(i, rand.Uint64()); err != nil {
		panic(err)
	} else {
		return f
	}
}

func TestIndexEvaluateRegex(t *testing.T) {
	var field *indexedField
	var rex *indexedField

	field = newIndexedFieldOrPanic("Test")
	rex = newIndexedFieldOrPanic("Test")

	if !field.evaluate("~=", rex) {
		t.Error("Should match")
	}

	if !field.evaluate("~=", newIndexedFieldOrPanic("(?i:test)")) {
		t.Error("Should match")
	}

	if !field.evaluate("~=", newIndexedFieldOrPanic(".*")) {
		t.Error("Should match")
	}

	if field.evaluate("~=", newIndexedFieldOrPanic("test")) {
		t.Error("Should not match")
	}

	if field.evaluate("~=", newIndexedFieldOrPanic("testtest")) {
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

	tt := toast.FromT(t)
	m := FieldDescriptors(&testStruct{})

	tt.Assert(m["A"].Type == "int")
	tt.Assert(m["B"].Type == "int")
	tt.Assert(m["C"].Type == "string")
	tt.Assert(m["D"].Type == "int16")
	tt.Assert(m["E"].Type == "int32")
	tt.Assert(m["F"].Type == "int64")
	tt.Assert(m["G"].Type == "uint8")
	tt.Assert(m["H"].Type == "uint16")
	tt.Assert(m["I"].Type == "uint32")
	tt.Assert(m["J"].Type == "uint64")
	tt.Assert(m["K"].Type == "float64")
	tt.Assert(m["L"].Type == "int8")
	tt.Assert(m["M"].Type == "time.Time")
	tt.Assert(m["N"].Type == "uint")
	tt.Assert(m["O"].Type == "string")
	tt.Assert(m["Upper"].Type == "string")
	tt.Assert(m["Lower"].Type == "string")
	tt.Assert(m["Nested.A"].Type == "int")
	tt.Assert(m["Nested.B"].Type == "uint64")
	tt.Assert(m["Nested.C"].Type == "float32")
	tt.Assert(m["Nested.In.D"].Type == "float64")
	tt.Assert(m["Nested.In.E"].Type == "int")
	tt.Assert(m["Nested.In.F"].Type == "string")
	tt.Assert(m["Nested.In.Anon.G"].Type == "string")
	tt.Assert(m["Ptr"].Type == "*int")
	t.Log(m)

	// asserting indexed fields
	indexed := map[string]bool{
		"A":                true,
		"B":                true,
		"C":                true,
		"D":                true,
		"E":                true,
		"F":                true,
		"G":                true,
		"H":                true,
		"I":                true,
		"J":                true,
		"K":                true,
		"L":                true,
		"M":                true,
		"Upper":            true,
		"Nested.C":         true,
		"Nested.In.F":      true,
		"Nested.In.Anon.G": true,
	}

	for f := range indexed {
		tt.Assert(m[f].Constraints.Index, "Asserting", f)
	}

	for f, fd := range m {
		if !indexed[f] {
			tt.Assert(!fd.Constraints.Index, "Asserting", f)
		}
	}

	tt.Assert(m["Upper"].Constraints.Upper)
	tt.Assert(m["Lower"].Constraints.Lower)

	//t.Log(FieldDescriptors(&nestedStruct{}))
	//t.Log(FieldDescriptors(&testStruct{}).Fingerprint())

}
