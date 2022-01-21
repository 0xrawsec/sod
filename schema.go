package sod

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const (
	SchemaFilename = "schema.json"
)

var (
	ErrBadSchema       = errors.New("schema must be a file")
	ErrMissingObjIndex = errors.New("schema is missing object index")

	DefaultSchema = Schema{Extension: ".json"}
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
	object       Object
	Extension    string `json:"extension"`
	Cache        bool   `json:"cache"`
	AsyncWrites  *Async `json:"async-writes,omitempty"`
	ObjectsIndex *Index `json:"index"`
}

func (s *Schema) update(from *Schema) {
	s.Extension = from.Extension
	s.Cache = from.Cache
	s.AsyncWrites = from.AsyncWrites
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

func (s *Schema) control() error {
	return s.ObjectsIndex.Control()
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
					fd.Constraint.Unique = true
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

// Asynchrone makes the data described by this schema managed asynchronously
// Objects will be written either if more than threshold events are modified
// or at every timeout
func (s *Schema) Asynchrone(threshold int, timeout time.Duration) {
	s.AsyncWrites = &Async{
		Enable:    true,
		Threshold: threshold,
		Timeout:   timeout}
}

// Index indexes an Object
func (s *Schema) Index(o Object) error {
	return s.ObjectsIndex.InsertOrUpdate(o)
}

// Index un-indexes an Object
func (s *Schema) Unindex(o Object) {
	s.ObjectsIndex.Delete(o)
}
