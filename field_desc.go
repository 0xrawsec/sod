package sod

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type Constraints struct {
	Unique bool `json:"unique"`
}

type FieldDescriptor struct {
	Path       string      `json:"-"`
	Type       string      `json:"-"`
	Index      bool        `json:"-"`
	Constraint Constraints `json:"constraint"`
}

func FieldDescriptors(o Object) (fds []FieldDescriptor) {
	fds = make([]FieldDescriptor, 0)
	recFieldDescriptors(reflect.ValueOf(o), "", &fds)
	return
}

func ObjectFingerprint(o Object) (fp string, err error) {
	var b []byte

	h := md5.New()

	if b, err = json.Marshal(FieldDescriptors(o)); err != nil {
		return
	}

	h.Write(b)
	fp = hex.EncodeToString(h.Sum(nil))
	return
}

func recFieldDescriptors(v reflect.Value, path string, fds *[]FieldDescriptor) {
	typ := v.Type()

	if v.Kind() == reflect.Ptr {
		recFieldDescriptors(v.Elem(), path, fds)
		return
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := typ.Field(i)

		if field.Kind() == reflect.Ptr {
			// create a new field
			field = reflect.New(fieldType.Type.Elem()).Elem()
			nextPath := fieldType.Name
			if path != "" {
				nextPath = strings.Join([]string{path, fieldType.Name}, ".")
			}
			recFieldDescriptors(field, nextPath, fds)
			continue
		}

		if field.Kind() == reflect.Struct {
			nextPath := fieldType.Name
			if path != "" {
				nextPath = strings.Join([]string{path, fieldType.Name}, ".")
			}
			recFieldDescriptors(field, nextPath, fds)
			continue
		}

		if tag, ok := fieldType.Tag.Lookup("sod"); ok {
			fdPath := fieldType.Name
			if path != "" {
				fdPath = fmt.Sprintf("%s.%s", path, fdPath)
			}
			fd := FieldDescriptor{
				Path: fdPath,
				Type: fieldType.Type.Name(),
			}
			for _, tv := range strings.Split(tag, ",") {
				switch tv {
				case "index":
					fd.Index = true
				case "unique":
					fd.Index = true
					fd.Constraint.Unique = true
				}
			}
			*fds = append(*fds, fd)
		}
	}
}
