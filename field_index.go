package sod

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// fieldIndex structure
// by convention the smallest value is at the end
type fieldIndex struct {
	Name string `json:"name"`
	// Cast is used to store type casting for the field value.
	// Because of JSON serialization the original type is lost as
	// IndexedField.Value is an interface{}
	Cast        string          `json:"cast"`
	Constraints Constraints     `json:"constraints"`
	Index       []*indexedField `json:"index"`
	objectIds   map[uint64]*indexedField
	nameSplit   []string
}

func (i *fieldIndex) UnmarshalJSON(data []byte) error {
	type tmp struct {
		Name        string          `json:"name"`
		Cast        string          `json:"cast"`
		Constraints Constraints     `json:"constraints"`
		Index       []*indexedField `json:"index"`
	}
	t := tmp{}
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	i.Name = t.Name
	i.Cast = t.Cast
	i.Constraints = t.Constraints
	i.Index = t.Index
	i.nameSplit = fieldPath(i.Name)

	for _, f := range i.Index {
		f.valueTypeFromString(i.Cast)
	}

	i.objectIds = make(map[uint64]*indexedField)
	for _, k := range i.Index {
		i.objectIds[k.ObjectId] = k
	}
	return nil
}

// newFieldIndex returns an empty initialized slice. Opts takes len and cap in
// order to initialize the underlying slice
func newFieldIndex(desc FieldDescriptor, opts ...int) *fieldIndex {
	l, c := 0, 0
	if len(opts) >= 1 {
		l = opts[0]
	}
	if len(opts) >= 2 {
		c = opts[1]
	}
	return &fieldIndex{
		Name:        desc.Path,
		Index:       make([]*indexedField, l, c),
		Constraints: desc.Constraints,
		objectIds:   make(map[uint64]*indexedField),
		nameSplit:   fieldPath(desc.Path)}
}

func (in *fieldIndex) Initialize(k *indexedField) {
	if in.Cast == "" {
		in.Cast = k.valueTypeString()
	}
}

func (in *fieldIndex) InsertionIndex(k *indexedField) int {
	return in.insertionIndexRec(k, 0, in.Len())
}

// Recursive function to search for the next index less than Sortable
func (in *fieldIndex) insertionIndexRec(k *indexedField, i, j int) int {
	// case where index is empty
	if in.Len() == 0 {
		return 0
	}

	// case where there is only one element
	if in.Len() == 1 {
		if in.Index[0].less(k) {
			return 0
		}
		return 1
	}

	// only one element to test == s[i:i+1]
	if j-i == 1 {
		if in.Index[i].less(k) {
			// before i
			return i
		}
		// after i
		return j
	}

	// recursive search
	pivot := ((j + 1 - i) / 2) + i
	if in.Index[pivot].less(k) {
		return in.insertionIndexRec(k, i, pivot)
	}

	return in.insertionIndexRec(k, pivot, j)
}

func (in *fieldIndex) rangeEqual(k *indexedField) (i, j int) {
	j = in.InsertionIndex(k) - 1
	for i = j; i >= 0 && in.Index[i].equal(k); i-- {
	}
	i++
	return
}

// Satisfy checks whether the value satisfies the constraints fixed by index
func (in *fieldIndex) Satisfy(objid uint64, exist bool, fvalue *indexedField) (err error) {

	constraint := in.Constraints

	// handling uniqueness
	if constraint.Unique {

		equals := in.SearchEqual(fvalue)

		if len(equals) > 1 {
			return ErrFieldUnique
		} else if len(equals) == 1 {
			// objid == 0 if object does not exists so we need to check exist flag
			if !exist || (equals[0].ObjectId != objid) {
				return ErrFieldUnique
			}
		}
	}

	return
}

func (in *fieldIndex) Has(value *indexedField) bool {
	return len(in.SearchEqual(value)) > 0
}

func (in *fieldIndex) SearchEqual(value *indexedField) []*indexedField {

	i, j := in.rangeEqual(value)

	if i == j {
		if in.Len() > 0 {
			return []*indexedField{in.Index[i]}
		}
	}

	return in.Index[i : j+1]
}

func (in *fieldIndex) SearchNotEqual(value *indexedField) (f []*indexedField) {

	i, j := in.rangeEqual(value)
	f = make([]*indexedField, len(in.Index[0:i]))
	copy(f, in.Index[0:i])
	f = append(f, in.Index[j+1:]...)

	return
}

func (in *fieldIndex) SearchGreaterOrEqual(value *indexedField) []*indexedField {

	i := in.InsertionIndex(value)

	// the only case when it is (logicaly) possible is when index is empty
	if i == 0 {
		return []*indexedField{}
	}

	return in.Index[:i]
}

func (in *fieldIndex) SearchGreater(value *indexedField) (f []*indexedField) {

	i := in.InsertionIndex(value)
	if i > in.lastIndex() {
		i--
	}

	// we need to go backward until one element is greater
	for i >= 0 {
		if in.Index[i].greater(value) {
			break
		}
		i--
	}

	if i == 0 {
		if in.Len() > 0 && in.Index[0].greater(value) {
			return []*indexedField{in.Index[0]}
		}
	}

	return in.Index[:i+1]
}

func (in *fieldIndex) SearchLess(value *indexedField) []*indexedField {

	i := in.InsertionIndex(value)
	if i > in.lastIndex() {
		return []*indexedField{}
	}

	return in.Index[i:]
}

func (in *fieldIndex) SearchLessOrEqual(value *indexedField) []*indexedField {

	i := in.InsertionIndex(value)
	if i > in.lastIndex() {
		i--
	}

	for i >= 0 {
		if in.Index[i].greater(value) {
			break
		}
		i--
	}

	return in.Index[i+1:]
}

func (in *fieldIndex) SearchByRegex(value *indexedField) (out []*indexedField, err error) {
	var rex *regexp.Regexp

	out = make([]*indexedField, 0)

	if sval, ok := value.Value.(string); ok {
		if rex, err = regexp.Compile(sval); err != nil {
			return
		}
	}

	for _, f := range in.Index {
		if sval, ok := f.Value.(string); ok {
			if rex.MatchString(sval) {
				out = append(out, f)
			}
		} else {
			return
		}
	}

	return
}

func (in *fieldIndex) insert(field *indexedField) {

	i := in.InsertionIndex(field)

	switch {
	case i > in.lastIndex():
		in.Index = append(in.Index, field)
	default:
		// Avoid creating intermediary slices
		in.Index = append(in.Index, field)
		copy(in.Index[i+1:], in.Index[i:])
		in.Index[i] = field
	}

	in.objectIds[field.ObjectId] = field
}

// Insertion method in the slice for a structure implementing Sortable
func (in *fieldIndex) Insert(value interface{}, objid uint64) (err error) {
	var field *indexedField

	if field, err = newIndexedField(value, objid); err != nil {
		return
	}

	in.Initialize(field)
	in.insert(field)

	return
}

func (in *fieldIndex) lastIndex() int {
	return in.Len() - 1
}

func (in *fieldIndex) Len() int {
	return len(in.Index)
}

func (in *fieldIndex) Update(value interface{}, objid uint64) (err error) {
	in.Delete(objid)
	return in.Insert(value, objid)
}

func (in *fieldIndex) SearchKey(k *indexedField) (i int, ok bool) {

	i, j := in.rangeEqual(k)
	if i == j {
		return i, in.Index[i].deepEqual(k)
	}

	for ; i <= j; i++ {
		if in.Index[i].deepEqual(k) {
			return i, true
		}
	}

	return 0, false

}

func (in *fieldIndex) Delete(objid uint64) {
	if field, ok := in.objectIds[objid]; ok {
		if i, ok := in.SearchKey(field); ok {
			if len(in.Index) == 1 {
				in.Index = make([]*indexedField, 0)
			} else {
				in.Index = append(in.Index[:i], in.Index[i+1:]...)
			}
			delete(in.objectIds, objid)
		} else {
			panic("key not found")
		}
	} else {
		panic("object id not found")
	}
}

// Constrain returns an index which intersects with other fields
// we can build some query logic based on that function searching an
// index from the result of another index
func (in *fieldIndex) Constrain(fields []*indexedField) (new *fieldIndex) {
	new = newFieldIndex(FieldDescriptor{}, 0, len(fields))
	for _, fi := range fields {
		if field, ok := in.objectIds[fi.ObjectId]; ok {
			new.insert(field)
		}
	}
	return
}

// Slice returns the underlying slice
func (in *fieldIndex) Slice() []*indexedField {
	return in.Index
}

// Control controls if the slice has been properly ordered. A return value of
// true means it is in good order
func (in *fieldIndex) Control() bool {
	if in.Len() == 0 {
		return true
	}

	v := in.Index[0]
	for _, tv := range in.Index {
		if !v.equal(tv) && !tv.less(v) {
			return false
		}
		v = tv
	}
	return true
}

func (in *fieldIndex) String() string {
	return fmt.Sprintf("%v", in.Index)
}
