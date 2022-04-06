package sod

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrInvalidObject = errors.New("object is not valid")
)

func validationErr(o Object, err error) error {
	return fmt.Errorf("%s %w: %s", stype(o), ErrInvalidObject, err)
}

func cloneValue(src interface{}, dst interface{}) {

	srcVal := reflect.ValueOf(src)
	srcType := reflect.TypeOf(src)

	// must be a pointer to a structure of src type
	dstVal := reflect.ValueOf(dst)
	dstType := reflect.TypeOf(dst)

	if !srcVal.IsValid() {
		// happens when we have an unitialized interface{} field in struct
		return
	}

	if !srcType.AssignableTo(dstType.Elem()) {
		panic(fmt.Sprintf("%s is not assignable to %s", srcType, dstType.Elem()))
	}

	switch srcVal.Kind() {
	case reflect.Ptr:
		srcElem := srcVal.Elem()
		dstElem := dstVal.Elem()

		// if it is a nil pointer
		if srcVal.IsZero() {
			// we create a new zero value for the value referenced by srcVal
			srcElem = reflect.Zero(srcVal.Type().Elem())
		}

		if dstElem.IsZero() {
			dstElem.Set(reflect.New(srcElem.Type()))
		}

		cloneValue(srcElem.Interface(), dstElem.Interface())

	case reflect.Slice:
		dstElem := dstVal.Elem()
		dstElem.Set(reflect.MakeSlice(srcType, srcVal.Len(), srcVal.Cap()))
		// because MakeSlice does not change Kind of dstElem if interface{}
		// we need to do that not to bug on dstElem.Index
		dstElem = reflect.ValueOf(dstElem.Interface())

		// if a slice of pointers reflect.Copy will copy pointers as is
		// however we want pointers to new structures
		for i := 0; i < srcVal.Len(); i++ {
			cloneValue(srcVal.Index(i).Interface(), dstElem.Index(i).Addr().Interface())
		}

	case reflect.Map:
		dstElem := dstVal.Elem()
		dstElem.Set(reflect.MakeMap(srcType))
		// because MakeMap does not change Kind of dstElem if interface{}
		// we need to do that not to bug on SetMapIndex
		dstElem = reflect.ValueOf(dstElem.Interface())

		iter := srcVal.MapRange()
		for ok := iter.Next(); ok; ok = iter.Next() {
			srcKey := iter.Key()
			srcVal := iter.Value()
			dstVal := reflect.New(srcVal.Type()).Elem()
			cloneValue(srcVal.Interface(), dstVal.Addr().Interface())
			dstElem.SetMapIndex(srcKey, dstVal)
		}

	case reflect.Struct:
		srcType := srcVal.Type()
		for i := 0; i < srcVal.NumField(); i++ {
			structField := srcType.Field(i)
			srcField := srcVal.Field(i)
			dstField := dstVal.Elem().Field(i)
			if structField.IsExported() {
				cloneValue(srcField.Interface(), dstField.Addr().Interface())
			}
		}

	default:
		dst := dstVal.Elem()
		if dst.IsZero() {
			dst.Set(reflect.Zero(srcVal.Type()))
		}
		dstVal.Elem().Set(srcVal)
	}
}

func CloneObject(o Object) (out Object) {
	cloneValue(o, &out)
	out.Initialize(o.UUID())
	return out
}

type Object interface {
	// UUID returns a unique identifier used to store the
	// Object in the database
	UUID() string

	// Initialize is called to initialize the UUID associated
	// to an Object
	Initialize(string)

	// Transform is called prior to Object insertion and
	// can be used to apply some transformation on the data
	// to insert.
	Transform()

	// Validate is called every time an Object is inserted
	// if an error is returned by this function the Object
	// will not be inserted.
	Validate() error
}

// Item is a base structure implementing Object interface
type Item struct {
	uuid string
}

func (o *Item) Initialize(uuid string) {
	o.uuid = uuid
}

func (o *Item) UUID() string {
	return o.uuid
}

func (o *Item) Transform() {}

func (o *Item) Validate() error {
	return nil
}
