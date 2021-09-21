package sod

import "math"

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

// And performs a new Search while "ANDing" search results
func (s *Search) And(field, operator string, value interface{}) *Search {
	if s.Err() != nil {
		return s
	}
	return s.db.search(s.object, field, operator, value, s.fields)
}

// And performs a new Search while "ORing" search results
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

// Reverse the order the results are collected by Collect function
func (s *Search) Reverse() *Search {
	s.reverse = true
	return s
}

func (s *Search) Limit(limit uint64) *Search {
	s.limit = limit
	return s
}

// Collect all the objects resulting from the search.
// If a search has been made on an indexed field, results
// will be in descending order by default. If you want to change
// result order, call Reverse before.
// NB: only search on indexed field(s) will be garanteed to be
// ordered according to the last field searched.
func (s *Search) Collect() (out []Object, err error) {
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
	for o, err = it.Next(); err == nil && err != ErrEOI && s.limit > 0; o, err = it.Next() {
		//if !s.reverse {
		out = append(out, o)
		/*} else {
			// insert with no additional slice allocation
			out = append(out, nil)
			copy(out[1:], out)
			out[0] = o
		}*/
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
