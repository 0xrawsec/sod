package sod

import (
	"testing"

	"github.com/0xrawsec/toast"
)

func TestTransform(t *testing.T) {
	tt := toast.FromT(t)

	up := Constraints{
		Upper: true,
	}

	low := Constraints{
		Lower: true,
	}

	foo := "foo"
	up.Transform(&foo)
	tt.Assert(foo == "FOO")
	tt.ShouldPanic(func() { up.Transform(foo) })
	up.Transform(&tt)

	// testing out with structures
	ts := &testStruct{C: "fOo"}
	up.TransformField("Nested.In.F", ts)
	ts.Nested = &nestedStruct{}
	ts.Nested.In.F = "Bar"

	// test one level of recursion
	up.TransformField("C", ts)
	tt.Assert(ts.C == "FOO")
	low.TransformField("C", ts)
	tt.Assert(ts.C == "foo")

	// test deeper level of recursion
	up.TransformField("Nested.In.F", ts)
	tt.Assert(ts.Nested.In.F == "BAR")
	low.TransformField("Nested.In.F", ts)
	tt.Assert(ts.Nested.In.F == "bar")
}
