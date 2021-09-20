package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrUnkownField          = errors.New("unknown object field")
	ErrFieldNotIndexed      = errors.New("field not indexed")
	ErrUnkownSearchOperator = errors.New("unknown search operator")
)

type jsonIndex struct {
	Fields    map[string]*fieldIndex `json:"fields"`
	ObjectIds map[uint64]string      `json:"object-ids"`
}

type Index struct {
	// used to generate ObjectId
	i uint64
	// mapping Object UUID -> ObjectId (in the index)
	uuids map[string]uint64

	Fields map[string]*fieldIndex
	// mapping ObjectId -> Object UUID
	ObjectIds map[uint64]string
}

func fieldByName(o Object, field string) (i interface{}, ok bool) {
	v := reflect.ValueOf(o)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	v = v.FieldByName(field)
	return v.Interface(), v.IsValid()
}

func NewIndex(fields ...string) *Index {
	i := &Index{i: 0,
		uuids:     make(map[string]uint64),
		Fields:    make(map[string]*fieldIndex),
		ObjectIds: make(map[uint64]string)}

	for _, f := range fields {
		i.Fields[f] = NewFieldIndex()
	}

	return i
}

func (in *Index) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonIndex{Fields: in.Fields, ObjectIds: in.ObjectIds})
}

func (in *Index) UnmarshalJSON(data []byte) error {
	tmp := jsonIndex{}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	in.i = 0
	in.Fields = tmp.Fields
	in.ObjectIds = tmp.ObjectIds
	in.uuids = make(map[string]uint64)

	// we search next index to use for object
	for i, uuid := range in.ObjectIds {
		if i > in.i {
			in.i = i
		}
		in.uuids[uuid] = i
	}
	// we don't want to reuse an existing index
	in.i++

	return nil
}

func (in *Index) InsertOrUpdate(o Object) (err error) {
	// the object is already known, we update
	if i, ok := in.uuids[o.UUID()]; ok {
		for fn, fi := range in.Fields {
			if v, ok := fieldByName(o, fn); ok {
				if err = fi.Update(v, i); err != nil {
					return
				}
			} else {
				return fmt.Errorf("%w %s", ErrUnkownField, fn)
			}
		}
	} else {
		// we insert
		in.ObjectIds[in.i] = o.UUID()
		in.uuids[o.UUID()] = in.i
		for fn, fi := range in.Fields {
			if v, ok := fieldByName(o, fn); ok {
				if err = fi.Insert(v, in.i); err != nil {
					return
				}
			} else {
				return fmt.Errorf("%w %s", ErrUnkownField, fn)
			}
		}
		in.i++
	}
	return nil
}

func (in *Index) Delete(o Object) {
	if index, ok := in.uuids[o.UUID()]; ok {
		for _, fi := range in.Fields {
			fi.Delete(index)
			delete(in.ObjectIds, index)
			delete(in.uuids, o.UUID())
		}
	}
}

func (in *Index) Len() int {
	return len(in.ObjectIds)
}

func (in *Index) Search(o Object, field string, operator string, value interface{}, constrain []*IndexedField) ([]*IndexedField, error) {
	if _, ok := fieldByName(o, field); ok {
		// if field is indexed
		if fi, ok := in.Fields[field]; ok {
			if constrain != nil {
				fi = fi.Constrain(constrain)
			}
			switch operator {
			case "!=":
				return fi.SearchNotEqual(value), nil
			case "=":
				return fi.SearchEqual(value), nil
			case ">":
				return fi.SearchGreater(value), nil
			case ">=":
				return fi.SearchGreaterOrEqual(value), nil
			case "<":
				return fi.SearchLess(value), nil
			case "<=":
				return fi.SearchLessOrEqual(value), nil
			default:
				return nil, fmt.Errorf("%w %s", ErrUnkownSearchOperator, operator)
			}
		}
		return nil, fmt.Errorf("%w %s", ErrFieldNotIndexed, field)
	} else {

		return nil, fmt.Errorf("%w %s for object %T", ErrUnkownField, field, o)
	}
}
