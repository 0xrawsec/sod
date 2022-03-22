package sod

import (
	"fmt"
	"reflect"
	"strings"
)

type Constraints struct {
	Index  bool `json:"index,omitempty"`
	Unique bool `json:"unique,omitempty"`
	Upper  bool `json:"upper,omitempty"`
	Lower  bool `json:"lower,omitempty"`
}

func (c Constraints) String() string {
	return fmt.Sprintf("index:%t unique:%t upper:%t lower:%t", c.Index, c.Unique, c.Upper, c.Lower)
}

func (c *Constraints) Transform(i interface{}) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		panic("interface must be a pointer")
	}

	c.transform(v)
}

func (c *Constraints) TransformField(fieldPath string, o Object) {
	if !c.Transformer() {
		return
	}

	c.recursiveTransform(strings.Split(fieldPath, "."), reflect.ValueOf(o))
}

func (c *Constraints) Transformer() bool {
	return c.Upper || c.Lower
}

func (c *Constraints) transform(v reflect.Value) {

	// dereference value if needed
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// if value cannot be set we return to prevent panic
	if !v.CanSet() {
		return
	}

	// handing upper constraint
	if c.Upper {
		switch v.Kind() {
		case reflect.Interface:
			// in case we passed a pointer to an interface which is a string
			i := v.Elem().Interface()
			if s, ok := i.(string); ok {
				v.Set(reflect.ValueOf(strings.ToUpper(s)))
			}

		case reflect.String:
			// can only apply upper transform to string
			v.SetString(strings.ToUpper(v.Interface().(string)))
		}
	}

	// handing lower constraint
	if c.Lower {
		switch v.Kind() {
		case reflect.Interface:
			// in case we passed a pointer to an interface which is a string
			i := v.Elem().Interface()
			if s, ok := i.(string); ok {
				v.Set(reflect.ValueOf(strings.ToLower(s)))
			}

		case reflect.String:
			// can only apply upper transform to string
			v.SetString(strings.ToLower(v.Interface().(string)))
		}
	}
}

func (c *Constraints) recursiveTransform(fieldPath []string, v reflect.Value) {
	if v.Kind() == reflect.Ptr {
		//v = v.Elem()
		c.recursiveTransform(fieldPath, v.Elem())
	}

	if v.Kind() == reflect.Struct {
		v = v.FieldByName(fieldPath[0])

		if len(fieldPath) > 1 {
			c.recursiveTransform(fieldPath[1:], v)
			return
		}

		c.transform(v)
	}
}
