package sod

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomIndex(size int) *fieldIndex {
	i := NewFieldIndex(0, size)
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

	if i, ok := fieldByName(&b, "Aa"); !ok {
		t.Errorf("Must have found field")
	} else if i.(int) != 42 {
		t.Errorf("Unexpected interface value")
	}
}

func TestSimpleIndex(t *testing.T) {
	size := 1000
	i := NewFieldIndex(0, size)
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

	for j := 0; j < size*2; j++ {
		k := rand.Int() % i.Len()
		if k%2 == 0 {
			k = 0
		}
		sk := i.Index[k]
		s := i.SearchGreaterOrEqual(sk.Value)
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
	if len(i.SearchGreaterOrEqual(42)) != 0 {
		t.Error("search result must be empty")
	}
}

func TestIndexSearchGreater(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchGreater(sk.Value)
		for _, k := range s {
			if k.Less(sk) || k.Equal(sk) {
				t.Errorf("%s not greater than %s", k, sk)
			}
		}
	}
}

func TestIndexSearchLess(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLess(sk.Value)
		for _, k := range s {
			if k.Greater(sk) || k.Equal(sk) {
				t.Errorf("%s not less than %s", k, sk)
			}
		}
	}
}
func TestIndexSearchLessOrEqual(t *testing.T) {
	size := 1000
	i := randomIndex(size)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLessOrEqual(sk.Value)
		if len(s) == 0 {
			t.Error("search result must not be empty")
		}
		for _, k := range s {
			if k.Greater(sk) {
				t.Errorf("%s not less than or equal to %s", k, sk)
			}
		}
	}
}

func TestIndexSearchEqual(t *testing.T) {
	size := 10000
	i := randomIndex(size)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchEqual(sk.Value)
		if len(s) == 0 {
			t.Error("search result must not be empty")
		}
		for _, k := range s {
			if !k.Equal(sk) {
				t.Errorf("%s not equal to %s", k, sk)
			}
		}
	}
}

func TestIndexSearchNotEqual(t *testing.T) {
	size := 1000
	i := randomIndex(size)
	for j := 0; j < size*2; j++ {
		j := rand.Intn(len(i.Index))
		sk := i.Index[j]
		s := i.SearchNotEqual(sk.Value)

		if len(s) == 0 {
			t.Error("search result must not be empty")
		}
		for _, k := range s {
			if k.Equal(sk) {
				t.Errorf("%s  equal to %s", k, sk)
			}
		}
	}
}

func TestIndexConstrain(t *testing.T) {
	size := 30
	i := randomIndex(size)
	o := randomIndex(1000)

	for j := 0; j < size*2; j++ {
		sk := i.Index[rand.Int()%i.Len()]
		s := i.SearchLess(sk.Value)
		inter := o.Constrain(s)

		if inter.Len() != len(s) || inter.Len() > i.Len() {
			t.Errorf("bad intersection length")
		}
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
