package sod

import "errors"

const (
	SchemaFilename = "schema.json"
)

var (
	ErrBadSchema = errors.New("schema must be a file")

	DefaultSchema = Schema{Extension: ".json"}
)

type Schema struct {
	Extension string
	// For later use
	//IndexedFields []string
}
