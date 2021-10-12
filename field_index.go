package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	ErrUnknownKeyType = errors.New("unknown key type")
)

type IndexedField struct {
	// the value we want to index
	Value interface{}
	// the ObjectId of the object in the list of object
	// it must be unique accross one ObjectId
	ObjectId uint64
}

func searchField(value interface{}) (k *IndexedField) {
	var err error
	if k, err = NewIndexedField(value, 0); err != nil {
		panic(err)
	}
	return
}

func NewIndexedField(value interface{}, objid uint64) (*IndexedField, error) {
	var err error

	// we cast everything to float64 because json unmarshal interface{}
	// to float64 and that is a current limitation of the indexing
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
	return &IndexedField{value, objid}, err
}

func (f *IndexedField) ValueTypeFromString(t string) {

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

func (f *IndexedField) ValueTypeString() string {
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

func (f *IndexedField) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{f.Value, f.ObjectId})
}

func (f *IndexedField) UnmarshalJSON(data []byte) error {
	var tuple []interface{}
	if err := json.Unmarshal(data, &tuple); err != nil {
		return err
	}
	f.Value = tuple[0]
	// Json unmarshals integer to interface{} as float64
	f.ObjectId = uint64(tuple[1].(float64))
	return nil
}

func (f *IndexedField) String() string {
	return fmt.Sprintf("(%v, %d)", f.Value, f.ObjectId)
}

func (f *IndexedField) Equal(other *IndexedField) bool {
	return reflect.DeepEqual(f.Value, other.Value)
}

func (f *IndexedField) DeepEqual(other *IndexedField) bool {
	if f.ObjectId != other.ObjectId {
		return false
	}
	return reflect.DeepEqual(f.Value, other.Value)
}

func (f *IndexedField) Greater(other *IndexedField) bool {
	return !f.Less(other) && !f.Equal(other)
}

func (f *IndexedField) Less(other *IndexedField) bool {
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

func (f *IndexedField) Evaluate(operator string, other *IndexedField) bool {
	switch operator {
	case "!=":
		return !f.Equal(other)
	case "=":
		return f.Equal(other)
	case ">":
		return f.Greater(other)
	case ">=":
		return f.Greater(other) || f.Equal(other)
	case "<":
		return f.Less(other)
	case "<=":
		return f.Less(other) || f.Equal(other)
	default:
		panic(ErrUnkownSearchOperator)
	}
}

type Constraints struct {
	Unique bool `json:"unique"`
}

type FieldDescriptor struct {
	Name       string      `json:"-"`
	Index      bool        `json:"-"`
	Constraint Constraints `json:"constraint"`
}

// fieldIndex structure
// by convention the smallest value is at the end
type fieldIndex struct {
	// Cast is used to store type casting for the field value.
	// Because of JSON serialization the original type is lost as
	// IndexedField.Value is an interface{}
	Cast        string          `json:"cast"`
	Constraints Constraints     `json:"constraints"`
	Index       []*IndexedField `json:"index"`
	objectIds   map[uint64]*IndexedField
}

func (i *fieldIndex) UnmarshalJSON(data []byte) error {
	type tmp struct {
		Cast        string          `json:"cast"`
		Constraints Constraints     `json:"constraints"`
		Index       []*IndexedField `json:"index"`
	}
	t := tmp{}
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	i.Cast = t.Cast
	i.Constraints = t.Constraints
	i.Index = t.Index
	for _, f := range i.Index {
		f.ValueTypeFromString(i.Cast)
	}

	i.objectIds = make(map[uint64]*IndexedField)
	for _, k := range i.Index {
		i.objectIds[k.ObjectId] = k
	}
	return nil
}

// NewFieldIndex returns an empty initialized slice. Opts takes len and cap in
// order to initialize the underlying slice
func NewFieldIndex(desc FieldDescriptor, opts ...int) *fieldIndex {
	l, c := 0, 0
	if len(opts) >= 1 {
		l = opts[0]
	}
	if len(opts) >= 2 {
		c = opts[1]
	}
	return &fieldIndex{
		Index:       make([]*IndexedField, l, c),
		Constraints: desc.Constraint,
		objectIds:   make(map[uint64]*IndexedField)}
}

func (in *fieldIndex) Initialize(k *IndexedField) {
	if in.Cast == "" {
		in.Cast = k.ValueTypeString()
	}
}

func (in *fieldIndex) InsertionIndex(k *IndexedField) int {
	return in.insertionIndexRec(k, 0, in.Len())
}

// Recursive function to search for the next index less than Sortable
func (in *fieldIndex) insertionIndexRec(k *IndexedField, i, j int) int {
	// case where index is empty
	if in.Len() == 0 {
		return 0
	}

	// case where there is only one element
	if in.Len() == 1 {
		if in.Index[0].Less(k) {
			return 0
		}
		return 1
	}

	// only one element to test == s[i:i+1]
	if j-i == 1 {
		if in.Index[i].Less(k) {
			// before i
			return i
		}
		// after i
		return j
	}

	// recursive search
	pivot := ((j + 1 - i) / 2) + i
	if in.Index[pivot].Less(k) {
		return in.insertionIndexRec(k, i, pivot)
	}

	return in.insertionIndexRec(k, pivot, j)
}

func (in *fieldIndex) rangeEqual(k *IndexedField) (i, j int) {
	j = in.InsertionIndex(k) - 1
	for i = j; i >= 0 && in.Index[i].Equal(k); i-- {
	}
	i++
	return
}

// Satisfy checks whether the value satisfies the constraints fixed by index
func (in *fieldIndex) Satisfy(value interface{}) (err error) {
	constraint := in.Constraints
	if constraint.Unique && in.Has(value) {
		return ErrFieldUnique
	}
	return
}

func (in *fieldIndex) Has(value interface{}) bool {
	return len(in.SearchEqual(value)) > 0
}

func (in *fieldIndex) SearchEqual(value interface{}) []*IndexedField {
	k := searchField(value)
	i, j := in.rangeEqual(k)
	if i == j {
		if in.Len() > 0 {
			return []*IndexedField{in.Index[i]}
		}
	}
	return in.Index[i : j+1]
}

func (in *fieldIndex) SearchNotEqual(value interface{}) []*IndexedField {
	k := searchField(value)
	i, j := in.rangeEqual(k)
	c := make([]*IndexedField, len(in.Index[0:i]))
	copy(c, in.Index[0:i])
	c = append(c, in.Index[j+1:]...)
	return c
}

func (in *fieldIndex) SearchGreaterOrEqual(value interface{}) []*IndexedField {
	k := searchField(value)
	i := in.InsertionIndex(k)
	// the only case when it is (logicaly) possible is when index is empty
	if i == 0 {
		return []*IndexedField{}
	}
	return in.Index[:i]
}

func (in *fieldIndex) SearchGreater(value interface{}) []*IndexedField {
	k := searchField(value)
	i := in.InsertionIndex(k)
	if i > in.lastIndex() {
		i--
	}
	// we need to go backward until one element is greater
	for i >= 0 {
		if in.Index[i].Greater(k) {
			break
		}
		i--
	}

	if i == 0 {
		if in.Len() > 0 && in.Index[0].Greater(k) {
			return []*IndexedField{in.Index[0]}
		}
	}

	return in.Index[:i+1]
}

func (in *fieldIndex) SearchLess(value interface{}) []*IndexedField {
	k := searchField(value)
	i := in.InsertionIndex(k)
	if i > in.lastIndex() {
		return []*IndexedField{}
	}
	return in.Index[i:]
}

func (in *fieldIndex) SearchLessOrEqual(value interface{}) []*IndexedField {
	k := searchField(value)
	i := in.InsertionIndex(k)
	if i > in.lastIndex() {
		i--
	}
	for i >= 0 {
		if in.Index[i].Greater(k) {
			break
		}
		i--
	}
	return in.Index[i+1:]
}

func (in *fieldIndex) insert(field *IndexedField) {

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
	var field *IndexedField

	if field, err = NewIndexedField(value, objid); err != nil {
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

func (in *fieldIndex) SearchKey(k *IndexedField) (i int, ok bool) {

	i, j := in.rangeEqual(k)
	if i == j {
		return i, in.Index[i].DeepEqual(k)
	}

	for ; i <= j; i++ {
		if in.Index[i].DeepEqual(k) {
			return i, true
		}
	}

	return 0, false

}

func (in *fieldIndex) Delete(objid uint64) {
	if field, ok := in.objectIds[objid]; ok {
		if i, ok := in.SearchKey(field); ok {
			if len(in.Index) == 1 {
				in.Index = make([]*IndexedField, 0)
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
func (in *fieldIndex) Constrain(fields []*IndexedField) (new *fieldIndex) {
	new = NewFieldIndex(FieldDescriptor{}, 0, len(fields))
	for _, fi := range fields {
		if field, ok := in.objectIds[fi.ObjectId]; ok {
			new.insert(field)
		}
	}
	return
}

// Slice returns the underlying slice
func (in *fieldIndex) Slice() []*IndexedField {
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
		if !v.Equal(tv) && !tv.Less(v) {
			return false
		}
		v = tv
	}
	return true
}

func (in *fieldIndex) String() string {
	return fmt.Sprintf("%v", in.Index)
}
