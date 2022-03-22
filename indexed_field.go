package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrUnknownKeyType = errors.New("unknown key type")
)

type indexedField struct {
	// the value we want to index
	Value interface{}
	// the ObjectId of the object in the list of object
	// it must be unique accross one ObjectId
	ObjectId uint64
}

func (f *indexedField) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{f.Value, f.ObjectId})
}

func (f *indexedField) UnmarshalJSON(data []byte) error {
	var tuple []interface{}
	if err := json.Unmarshal(data, &tuple); err != nil {
		return err
	}
	f.Value = tuple[0]
	// Json unmarshals integer to interface{} as float64
	f.ObjectId = uint64(tuple[1].(float64))
	return nil
}

func (f *indexedField) String() string {
	return fmt.Sprintf("(%v, %d)", f.Value, f.ObjectId)
}

func searchField(value interface{}) (k *indexedField, err error) {
	return newIndexedField(value, 0)
}

func newIndexedField(value interface{}, objid uint64) (*indexedField, error) {
	var err error

	switch k := value.(type) {
	case uint8:
		value = uint64(k)
	case uint16:
		value = uint64(k)
	case uint32:
		value = uint64(k)
	case uint:
		value = uint64(k)
	case int8:
		value = int64(k)
	case int16:
		value = int64(k)
	case int32:
		value = int64(k)
	case int:
		value = int64(k)
	case float32:
		value = float64(k)
	case time.Time:
		value = k.UTC().UnixNano()
	case string, float64, uint64, int64:
		value = k
	default:
		err = fmt.Errorf("%w %T", ErrUnknownKeyType, value)
	}
	return &indexedField{value, objid}, err
}

func (f *indexedField) valueTypeFromString(t string) {
	// we cast everything to float64 because json unmarshal interface{}
	// to float64 and that is a current limitation of the indexing
	switch t {
	case "float64":
		f.Value = f.Value.(float64)
	case "int64":
		f.Value = int64(f.Value.(float64))
	case "uint64":
		f.Value = uint64(f.Value.(float64))
	case "string":
	default:
		panic(fmt.Errorf("%w %s", ErrUnknownKeyType, t))
	}
}

func (f *indexedField) valueTypeString() string {
	switch f.Value.(type) {
	case float64:
		return "float64"
	case int64:
		return "int64"
	case uint64:
		return "uint64"
	case string:
		return "string"
	default:
		panic(fmt.Errorf("%w %T", ErrUnknownKeyType, f.Value))
	}
}

func (f *indexedField) equal(other *indexedField) bool {
	switch kt := f.Value.(type) {
	case int64:
		return kt == other.Value.(int64)
	case uint64:
		return kt == other.Value.(uint64)
	case float64:
		return kt == other.Value.(float64)
	case string:
		return kt == other.Value.(string)
	default:
		panic(fmt.Errorf("%w %T", ErrUnknownKeyType, f.Value))
	}
}

func (f *indexedField) deepEqual(other *indexedField) bool {
	if f.ObjectId != other.ObjectId {
		return false
	}
	return f.equal(other)
}

func (f *indexedField) greater(other *indexedField) bool {
	return !f.less(other) && !f.equal(other)
}

func (f *indexedField) less(other *indexedField) bool {
	switch kt := f.Value.(type) {
	case int64:
		return kt < other.Value.(int64)
	case uint64:
		return kt < other.Value.(uint64)
	case float64:
		return kt < other.Value.(float64)
	case string:
		return kt < other.Value.(string)
	default:
		panic(fmt.Errorf("%w %T", ErrUnknownKeyType, f.Value))
	}
}

func (f *indexedField) evaluate(operator string, other *indexedField) bool {
	switch operator {
	case "!=":
		return !f.equal(other)
	case "=":
		return f.equal(other)
	case ">":
		return f.greater(other)
	case ">=":
		return f.greater(other) || f.equal(other)
	case "<":
		return f.less(other)
	case "<=":
		return f.less(other) || f.equal(other)
	case "~=":
		var err error
		var rex *regexp.Regexp

		if sov, ok := other.Value.(string); ok {
			if rex, err = regexp.Compile(sov); err != nil {
				return false
			}
		}

		if sv, ok := f.Value.(string); ok {
			return rex.MatchString(sv)
		}

		return false
	default:
		panic(ErrUnkownSearchOperator)
	}
}
