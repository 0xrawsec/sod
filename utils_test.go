package sod

import (
	"reflect"
	"testing"
)

func name(i interface{}) {

}

func TestObjectName(t *testing.T) {
	t.Log(reflect.TypeOf(&testStruct{}).Elem().String())
	t.Log(reflect.TypeOf(&testStruct{}).Elem().Name())
	t.Log(reflect.TypeOf(&testStruct{}).Elem().PkgPath())
}
