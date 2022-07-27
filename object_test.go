package sod

import (
	"reflect"
	"testing"
	"time"

	"github.com/0xrawsec/toast"
)

type subSubStruct struct {
	S string
	I interface{}
}

type subStruct struct {
	PtrMap map[string]*subSubStruct
	I      interface{}
}

type structPtr struct {
	Item
	Time       time.Time
	Slice      []string
	EmptySlice []int
	Map        map[int]string
	EmptyMap   map[string]int
	//PtrMap     map[string]*testStruct
	Sub *subStruct
	Ptr *int
	I1  interface{}
	I2  interface{}
	I3  interface{}
}

func TestCloneObject(t *testing.T) {
	tt := toast.FromT(t)

	ts := &testStruct{
		M: time.Now(),
	}
	ts.A = 42
	ts.B = 43

	new := CloneObject(ts).(*testStruct)
	tt.Assert(ts.A == new.A)
	tt.Assert(ts.B == new.B)
	tt.Assert(ts.M.Equal(new.M))
	new.M = time.Now()
	tt.Assert(!ts.M.Equal(new.M))
	tt.Assert(ts.M.Before(new.M))

	// modifying one struct
	ts.A = 41
	ts.B = 44
	tt.Assert(ts.A != new.A)
	tt.Assert(ts.B != new.B)
}

func TestCloneObjectWithPtr(t *testing.T) {
	tt := toast.FromT(t)

	i := 42
	s1 := &structPtr{
		Ptr:   &i,
		Slice: []string{"foo", "bar"},
		Map: map[int]string{
			0: "foo",
			1: "bar",
		},
	}

	s2 := CloneObject(s1).(*structPtr)
	tt.Assert(*s1.Ptr == 42)
	tt.Assert(*s2.Ptr == 42)
	tt.Assert(s2.I1 == nil)
	tt.Assert(s2.I2 == nil)
	tt.Assert(s2.I3 == nil)
	tt.Assert(s2.Sub == nil)
	tt.Assert(s2.EmptyMap == nil)
	tt.Assert(s2.EmptySlice == nil)

	*s1.Ptr = 43
	(*s1).Slice[0] = "foofoo"
	(*s1).Map[0] = "foofoo"
	tt.Assert(*s1.Ptr == 43)

	t.Log(*s2.Ptr)
	tt.Assert(*s2.Ptr == 42)
	tt.Assert(s1.UUID() == s2.UUID())

	// testing slice copy
	tt.Assert(len((*s1).Slice) == len((*s2).Slice))
	tt.Assert(!reflect.DeepEqual((*s1).Slice, (*s2).Slice))
	tt.Assert((*s1).Slice[0] == "foofoo")
	tt.Assert((*s2).Slice[0] == "foo")

	// testing map copy
	tt.Assert(len((*s1).Map) == len((*s2).Map))
	tt.Assert((*s1).Map[0] == "foofoo")
	tt.Assert((*s2).Map[0] == "foo")
}

func TestCloneBug(t *testing.T) {

	tt := toast.FromT(t)
	i := 42

	s1 := &structPtr{
		Ptr:   &i,
		Slice: []string{"foo", "bar"},
		Map: map[int]string{
			0: "foo",
			1: "bar",
		},
		I1: map[string]string{
			"foo": "bar",
		},
		I2: []string{
			"foofoo", "babar",
		},
	}

	s2 := CloneObject(s1).(*structPtr)
	tt.Assert(*s1.Ptr == 42)
	tt.Assert(*s2.Ptr == 42)

	*s1.Ptr = 43
	(*s1).Slice[0] = "foofoo"
	(*s1).Map[0] = "foofoo"
	tt.Assert(*s1.Ptr == 43)

	tt.Assert(*s2.Ptr == 42)
	tt.Assert(s1.UUID() == s2.UUID())

	// testing slice copy
	tt.Assert(len((*s1).Slice) == len((*s2).Slice))
	tt.Assert(!reflect.DeepEqual((*s1).Slice, (*s2).Slice))
	tt.Assert((*s1).Slice[0] == "foofoo")
	tt.Assert((*s2).Slice[0] == "foo")

	// testing map copy
	tt.Assert(len((*s1).Map) == len((*s2).Map))
	tt.Assert((*s1).Map[0] == "foofoo")
	tt.Assert((*s2).Map[0] == "foo")

	im := s2.I1.(map[string]string)
	tt.Assert(im["foo"] == "bar")

	is := s2.I2.([]string)
	tt.Assert(is[0] == "foofoo")
	tt.Assert(is[1] == "babar")

	tt.Assert(s2.I3 == nil)

}
