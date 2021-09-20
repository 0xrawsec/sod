package sod

import (
	"errors"
)

const (
	SchemaFilename = "schema.json"
)

var (
	ErrBadSchema = errors.New("schema must be a file")

	DefaultSchema = Schema{Extension: ".json"}
)

type Schema struct {
	object       Object
	Extension    string `json:"extension"`
	ObjectsIndex *Index `json:"index"`
}

func (s *Schema) Initialize(o Object) {
	s.object = o
	if s.ObjectsIndex == nil {
		s.ObjectsIndex = NewIndex()
	}
}

func (s *Schema) Index(o Object) error {
	return s.ObjectsIndex.InsertOrUpdate(o)
}

func (s *Schema) Unindex(o Object) {
	s.ObjectsIndex.Delete(o)
}
