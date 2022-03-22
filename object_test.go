package sod

import (
	"reflect"
	"testing"

	"github.com/0xrawsec/toast"
)

type structPtr struct {
	Item
	Slice      []string
	EmptySlice []int
	Map        map[int]string
	EmptyMap   map[string]int
	Ptr        *int
}

func TestCloneObject(t *testing.T) {
	tt := toast.FromT(t)

	ts := &testStruct{}
	ts.A = 42
	ts.B = 43

	new := CloneObject(ts).(*testStruct)
	tt.Assert(ts.A == new.A)
	tt.Assert(ts.B == new.B)

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

}
