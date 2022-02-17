package sod

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

var (
	ErrUnknownOperator = errors.New("unknown logical operator")
	ErrNoObjectFound   = errors.New("no object found")
)

func IsNoObjectFound(err error) bool {
	return errors.Is(err, ErrNoObjectFound)
}

// Search helper structure to easily build search queries on objects
// and retrieve the results
type Search struct {
	db      *DB
	object  Object
	fields  []*IndexedField
	limit   uint64
	reverse bool
	err     error
}

func newSearch(db *DB, o Object, f []*IndexedField, err error) *Search {
	return &Search{db: db, object: o, fields: f, limit: math.MaxUint, err: err}
}

// Operation performs a new Search while ANDing or ORing the results
// operator must be in ["and", "&&", "or", "||"]
func (s *Search) Operation(operator, field, comparator string, value interface{}) *Search {
	op := strings.ToLower(operator)
	switch op {
	case "and", "&&":
		return s.And(field, comparator, value)
	case "or", "||":
		return s.Or(field, comparator, value)
	default:
		s.err = fmt.Errorf("%w %s", ErrUnknownOperator, op)
	}
	return s
}

// And performs a new Search while "ANDing" search results
func (s *Search) And(field, operator string, value interface{}) *Search {
	if s.Err() != nil {
		return s
	}
	return s.db.search(s.object, field, operator, value, s.fields)
}

// Or performs a new Search while "ORing" search results
func (s *Search) Or(field, operator string, value interface{}) *Search {
	if s.Err() != nil {
		return s
	}
	new := s.db.search(s.object, field, operator, value, nil)
	marked := make(map[uint64]bool)
	// we mark the fields of the new search
	for _, f := range new.fields {
		marked[f.ObjectId] = true
	}
	for _, f := range s.fields {
		// we concat the searches while deduplicating
		if _, ok := marked[f.ObjectId]; !ok {
			new.fields = append(new.fields, f)
		}
	}
	return new
}

// Len returns the number of data returned by the search
func (s *Search) Len() int {
	return len(s.fields)
}

// Iterator returns an Iterator convenient to iterate over
// the objects resulting from the search
func (s *Search) Iterator() (it *Iterator, err error) {
	var sch *Schema

	if sch, err = s.db.schema(s.object); err != nil {
		return
	}

	it = &Iterator{db: s.db, t: typeof(s.object)}
	it.uuids = make([]string, 0, len(s.fields))

	for _, f := range s.fields {
		it.uuids = append(it.uuids, sch.ObjectsIndex.ObjectIds[f.ObjectId])
	}

	return
}

// Delete deletes the objects found by the search
func (s *Search) Delete() (err error) {
	var it *Iterator

	if it, err = s.Iterator(); err != nil {
		return
	}

	return s.db.DeleteObjects(it)
}

// Reverse the order the results are collected by Collect function
func (s *Search) Reverse() *Search {
	s.reverse = true
	return s
}

// Limit the number of results collected by Collect function
func (s *Search) Limit(limit uint64) *Search {
	s.limit = limit
	return s
}

// One returns the first result found calling Collect function.
// If no Object is found, ErrNoObjectFound is returned
func (s *Search) One() (o Object, err error) {
	var sr []Object

	if s.Len() > 0 {
		if sr, err = s.Collect(); err != nil {
			return
		}
		o = sr[0]
		return
	}
	err = ErrNoObjectFound
	return
}

// AssignOne returns the first result found calling Collect function
// and assign the Object found to target. Target must be a *sod.Object
// otherwise the function panics. If no Object is found, ErrNoObjectFound
// is returned
func (s *Search) AssignOne(target interface{}) (err error) {
	var o Object

	if o, err = s.One(); err != nil {
		return err
	} else {
		v := reflect.ValueOf(target)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
			if _, ok := v.Interface().(Object); ok {
				ov := reflect.ValueOf(o)
				v.Set(ov)
				return
			}
		}
		panic("target must be a *sod.Object")
	}
}

// Collect all the objects resulting from the search.
// If a search has been made on an indexed field, results
// will be in descending order by default. If you want to change
// result order, call Reverse before.
// NB: only search on indexed field(s) will be garanteed to be
// ordered according to the last field searched.
func (s *Search) Collect() (out []Object, err error) {
	s.db.Lock()
	defer s.db.Unlock()

	var it *Iterator
	var o Object

	if s.Err() != nil {
		return nil, s.Err()
	}

	if it, err = s.Iterator(); err != nil {
		return
	}

	if s.reverse {
		it.Reverse()
	}

	out = make([]Object, 0, it.Len())
	for o, err = it.next(); err == nil && err != ErrEOI && s.limit > 0; o, err = it.next() {
		out = append(out, o)
		s.limit--
	}

	// normal end of iterator
	if err == ErrEOI {
		err = nil
	}

	return
}

// Err return any error encountered while searching
func (s *Search) Err() error {
	return s.err
}
