package sod

import (
	"testing"

	"github.com/0xrawsec/toast"
)

func TestFieldDescriptors(t *testing.T) {
	type foo struct {
		Item
		Foo int
		Bar string
	}

	tt := toast.FromT(t)
	fds := FieldDescriptors(&testStruct{})
	tt.CheckErr(fds.CompatibleWith(FieldDescriptors(&testStruct{})))

	// adding some constraint to a field descriptor
	tt.CheckErr(fds.Constraint("A", Constraints{Unique: true}))
	tt.ExpectErr(fds.CompatibleWith(FieldDescriptors(&testStruct{})), ErrFieldDescModif)

	// comparing with a completely different struct
	tt.ExpectErr(fds.CompatibleWith(FieldDescriptors(&foo{})), ErrUnkownField)
}
