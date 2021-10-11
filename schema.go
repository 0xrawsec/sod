package sod

import (
	"errors"
	"strings"
)

const (
	SchemaFilename = "schema.json"
)

var (
	ErrBadSchema       = errors.New("schema must be a file")
	ErrMissingObjIndex = errors.New("schema is missing object index")

	DefaultSchema = Schema{Extension: ".json"}
)

type Schema struct {
	object       Object
	Extension    string `json:"extension"`
	ObjectsIndex *Index `json:"index"`
}

func (s *Schema) Initialize(o Object) {
	s.object = o

	t := typeof(o)
	indexedFields := make([]FieldDescriptor, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if value, ok := f.Tag.Lookup("sod"); ok {
			fd := FieldDescriptor{Name: f.Name}
			for _, v := range strings.Split(value, ",") {
				switch v {
				case "index":
					fd.Index = true
				case "unique":
					fd.Index = true
					fd.Unique = true
				}
			}
			if fd.Index {
				indexedFields = append(indexedFields, fd)
			}
		}
	}

	if s.ObjectsIndex == nil {
		s.ObjectsIndex = NewIndex(indexedFields...)
	}
}

func (s *Schema) Index(o Object) error {
	return s.ObjectsIndex.InsertOrUpdate(o)
}

func (s *Schema) Unindex(o Object) {
	s.ObjectsIndex.Delete(o)
}
