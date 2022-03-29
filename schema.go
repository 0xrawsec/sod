package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	SchemaFilename = "schema.json"
)

var (
	ErrIndexCorrupted    = errors.New("index is corrupted")
	ErrBadSchema         = errors.New("schema must be a file")
	ErrMissingObjIndex   = errors.New("schema is missing object index")
	ErrStructureChanged  = errors.New("object structure changed")
	ErrExtensionMismatch = errors.New("extension mismatch")

	DefaultExtension   = ".json"
	DefaultCompression = false
	DefaultSchema      = Schema{
		Extension: DefaultExtension,
		Compress:  DefaultCompression}
	DefaultSchemaCompress = Schema{
		Extension: DefaultExtension,
		Compress:  true}

	compressedExtension = ".gz"
)

type jsonAsync struct {
	Enable    bool   `json:"enable"`
	Threshold int    `json:"threshold"`
	Timeout   string `json:"timeout"`
}

type Async struct {
	routineStarted bool
	Enable         bool
	Threshold      int
	Timeout        time.Duration
}

func (a *Async) MarshalJSON() ([]byte, error) {
	t := jsonAsync{
		a.Enable,
		a.Threshold,
		a.Timeout.String(),
	}
	return json.Marshal(&t)
}

func (a *Async) UnmarshalJSON(b []byte) (err error) {
	t := jsonAsync{}
	if err = json.Unmarshal(b, &t); err != nil {
		return
	}
	// copying fields
	a.Enable = t.Enable
	a.Threshold = t.Threshold
	if a.Timeout, err = time.ParseDuration(t.Timeout); err != nil {
		return
	}
	return
}

type Schema struct {
	db           *DB
	object       Object
	transformers []FieldDescriptor

	Fields      FieldDescMap `json:"fields"`
	Extension   string       `json:"extension"`
	Compress    bool         `json:"compress"`
	Cache       bool         `json:"cache"`
	AsyncWrites *Async       `json:"async-writes,omitempty"`
	ObjectIndex *objIndex    `json:"index"`
}

func NewCustomSchema(fields FieldDescMap, ext string) (s Schema) {
	return Schema{
		Extension:   ext,
		Fields:      fields,
		ObjectIndex: newIndex(fields),
	}
}

// Asynchrone makes the data described by this schema managed asynchronously
// Objects will be written either if more than threshold events are modified
// or at every timeout
func (s *Schema) Asynchrone(threshold int, timeout time.Duration) {
	s.AsyncWrites = &Async{
		Enable:    true,
		Threshold: threshold,
		Timeout:   timeout}
}

// Indexed returns the FieldDescriptors of indexed fields
func (s *Schema) Indexed() (desc []FieldDescriptor) {
	desc = make([]FieldDescriptor, 0)

	for fpath := range s.ObjectIndex.Fields {
		desc = append(desc, s.Fields[fpath])
	}

	return
}

func (s *Schema) initialize(db *DB, o Object) (err error) {
	// initialize db using this schema
	s.db = db

	// initialize object associtated to the schema
	s.object = o

	// initialize fields
	if s.Fields == nil {
		s.Fields = FieldDescriptors(o)
	}

	// initializes the list of tranformers
	s.transformers = s.Fields.Transformers()

	// initializes ObjectsIndex if needed
	if s.ObjectIndex == nil {
		s.ObjectIndex = newIndex(s.Fields)
	}

	return
}

// prepare applies transform on search value
func (s *Schema) prepare(fpath string, value interface{}) {
	if fd, ok := s.Fields[fpath]; ok {
		// we transform search value only if we have a transformer constraint
		if fd.Constraints.Transformer() {
			fd.Transform(value)
		}
	}
}

// transform applies transform constraints defined in Schema
func (s *Schema) transform(o Object) {
	// transform Object
	for _, t := range s.transformers {
		t.Transform(o)
	}
}

// index indexes an Object
func (s *Schema) index(o Object) error {
	return s.ObjectIndex.insertOrUpdate(o)
}

func (s *Schema) isUUIDIndexed(uuid string) bool {
	_, ok := s.ObjectIndex.uuids[uuid]
	return ok
}

func (s *Schema) unindexByUUID(uuid string) {
	s.ObjectIndex.deleteByUUID(uuid)
}

// Index un-indexes an Object
func (s *Schema) unindex(o Object) {
	s.ObjectIndex.deleteByUUID(o.UUID())
}

func (s *Schema) filenameFromUUID(uuid string) string {
	if s.Compress {
		return fmt.Sprintf("%s%s%s", uuid, s.Extension, compressedExtension)
	}
	return fmt.Sprintf("%s%s", uuid, s.Extension)
}

func (s *Schema) filename(o Object) string {
	return s.filenameFromUUID(o.UUID())
}

func (s *Schema) isCompatibleWith(other *Schema) (err error) {
	// check if extension are compatible
	if s.Extension != other.Extension {
		return ErrExtensionMismatch
	}

	// check if FieldDescriptors are compatible
	if err = s.Fields.CompatibleWith(other.Fields); err != nil {
		return
	}

	return
}

func (s *Schema) update(from *Schema) (err error) {
	// we check if both the schema are compatible
	if err = s.isCompatibleWith(from); err != nil {
		return
	}

	s.Cache = from.Cache
	s.AsyncWrites = from.AsyncWrites

	return
}

func (s *Schema) mustCache() bool {
	return s.Cache || s.asyncWritesEnabled()
}

func (s *Schema) asyncWritesEnabled() bool {
	if s.AsyncWrites != nil {
		return s.AsyncWrites.Enable
	}
	return false
}

func (s *Schema) control() (err error) {
	var uuids map[string]bool

	dir := s.db.oDir(s.object)

	// control that object structure did not change
	if err := s.Fields.FieldsCompatibleWith(FieldDescriptors(s.object)); err != nil {
		return fmt.Errorf("%T %w: %s", s.object, ErrStructureChanged, err)
	}

	// controlling index in memory
	if err = s.ObjectIndex.control(); err != nil {
		return
	}

	// verifying index integrity (longer process so done at last)
	// we control any index corruption
	if uuids, err = uuidsFromDir(dir); err != nil && !os.IsNotExist(err) {
		return
	}

	// we iterate over all the uuids found on disk
	for uuid := range uuids {
		// if file is on disk but not indexed
		if !s.isUUIDIndexed(uuid) {
			return fmt.Errorf("%s %w: schema index is missing entry", typeof(s.object), ErrIndexCorrupted)
		}
	}

	// we de-index missing objects
	for uuid := range s.ObjectIndex.uuids {
		if !uuids[uuid] {
			return fmt.Errorf("%s %w: object deleted but still indexed", typeof(s.object), ErrIndexCorrupted)
		}
	}

	// force nil otherwise takes NoExist error skipped above
	return nil
}
