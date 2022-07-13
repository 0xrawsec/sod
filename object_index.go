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
	ErrCasting              = errors.New("casting error")

	ErrConstraintUnique = errors.New("uniqueness constraint")
)

func IsUnique(err error) bool {
	return errors.Is(err, ErrConstraintUnique)
}

func valueFieldByName(v reflect.Value, fields []string) (out reflect.Value, ok bool) {

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	out = v.FieldByName(fields[0])

	// if pointer we dereference
	if out.Kind() == reflect.Ptr {
		if out.IsZero() {
			out = reflect.New(out.Type().Elem())
		} else {
			out = out.Elem()
		}
		return valueFieldByName(out, fields[1:])
	}

	if out.Kind() == reflect.Struct && len(fields) > 1 {
		return valueFieldByName(out, fields[1:])
	}

	return out, out.IsValid()
}

func fieldByName(o Object, fpath []string) (i interface{}, ok bool) {
	v := reflect.ValueOf(o)

	v, ok = valueFieldByName(v, fpath)
	if !ok {
		return nil, ok
	}

	return v.Interface(), ok
}

type jsonObjIndex struct {
	Fields    map[string]*fieldIndex `json:"fields"`
	ObjectIds map[uint64]string      `json:"object-ids"`
}

type objIndex struct {
	// used to generate ObjectId
	i uint64
	// mapping Object UUID -> ObjectId (in the index)
	uuids map[string]uint64

	Fields map[string]*fieldIndex
	// mapping ObjectId -> Object UUID
	ObjectIds map[uint64]string
}

func (in *objIndex) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonObjIndex{Fields: in.Fields, ObjectIds: in.ObjectIds})
}

func (in *objIndex) UnmarshalJSON(data []byte) error {
	tmp := jsonObjIndex{}
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

func newIndex(fields FieldDescMap) *objIndex {
	i := &objIndex{i: 0,
		uuids:     make(map[string]uint64),
		Fields:    make(map[string]*fieldIndex),
		ObjectIds: make(map[uint64]string)}

	for _, fd := range fields {
		if fd.Constraints.Index || fd.Constraints.Unique {
			i.Fields[fd.Path] = newFieldIndex(fd)
		}
	}

	return i
}

func (in *objIndex) satisfyAll(o Object) (err error) {
	for fn, fi := range in.Fields {
		if v, ok := fieldByName(o, fi.nameSplit); ok {
			var iField *indexedField

			if iField, err = searchField(v); err != nil {
				return
			}

			// check constraint on value
			objid, exists := in.uuids[o.UUID()]
			if err = fi.Satisfy(objid, exists, iField); err != nil {
				return fmt.Errorf("field %s does not satisfy %w", fn, err)
			}
		} else {
			return fmt.Errorf("cannot satisfy constraint %w %s", ErrUnkownField, fn)
		}
	}
	return
}

func (in *objIndex) insertOrUpdate(o Object) (err error) {
	// check constraint on all index first to prevent
	// inconsistencies across indexes
	if err = in.satisfyAll(o); err != nil {
		return
	}

	// the object is already known, we update
	if i, ok := in.uuids[o.UUID()]; ok {
		for fn, fi := range in.Fields {
			if v, ok := fieldByName(o, fi.nameSplit); ok {
				if err = fi.Update(v, i); err != nil {
					return
				}
			} else {
				return fmt.Errorf("%w %s", ErrUnkownField, fn)
			}
		}
	} else {
		for fn, fi := range in.Fields {
			if v, ok := fieldByName(o, fi.nameSplit); ok {
				if err = fi.Insert(v, in.i); err != nil {
					return
				}
			} else {
				return fmt.Errorf("%w %s", ErrUnkownField, fn)
			}
		}
		// we insert after any potential error
		in.ObjectIds[in.i] = o.UUID()
		in.uuids[o.UUID()] = in.i
		in.i++
	}
	return nil
}

func (in *objIndex) deleteByUUID(uuid string) {
	if index, ok := in.uuids[uuid]; ok {
		for _, fi := range in.Fields {
			fi.Delete(index)
		}
		delete(in.ObjectIds, index)
		delete(in.uuids, uuid)
	}
}

func (in *objIndex) search(o Object, field string, operator string, value interface{}, constrain []*indexedField) ([]*indexedField, error) {
	var iField *indexedField
	var err error

	if _, ok := fieldByName(o, fieldPath(field)); ok {

		if iField, err = searchField(value); err != nil {
			return nil, err
		}

		// if field is indexed
		if fi, ok := in.Fields[field]; ok {
			if fi.Cast != iField.valueTypeString() {
				return nil, fmt.Errorf("%w, cannot cast %T(%v) to %s", ErrCasting, value, value, fi.Cast)
			}

			if constrain != nil {
				fi = fi.Constrain(constrain)
			}

			switch operator {
			case "!=":
				return fi.SearchNotEqual(iField), nil
			case "=":
				return fi.SearchEqual(iField), nil
			case ">":
				return fi.SearchGreater(iField), nil
			case ">=":
				return fi.SearchGreaterOrEqual(iField), nil
			case "<":
				return fi.SearchLess(iField), nil
			case "<=":
				return fi.SearchLessOrEqual(iField), nil
			case "~=":
				return fi.SearchByRegex(iField)
			default:
				return nil, fmt.Errorf("%w %s", ErrUnkownSearchOperator, operator)
			}
		}
		return nil, fmt.Errorf("%w %s", ErrFieldNotIndexed, field)
	} else {
		return nil, fmt.Errorf("%w %s for object %T", ErrUnkownField, field, o)
	}
}

func (in *objIndex) control() error {
	for fn := range in.Fields {
		if !in.Fields[fn].Control() {
			return fmt.Errorf("field index %s is not ordered", fn)
		}
		if in.Fields[fn].Len() != in.len() {
			return fmt.Errorf("index and fields index must have the same size, len(index)=%d len(index[%s])=%d", in.len(), fn, in.Fields[fn].Len())
		}
	}
	return nil
}

func (in *objIndex) len() int {
	return len(in.ObjectIds)
}
