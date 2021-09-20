package sod

// Search helper structure to easily build search queries on objects
// and retrieve the results
type Search struct {
	db     *DB
	object Object
	fields []*IndexedField
	err    error
}

func newSearch(db *DB, o Object, f []*IndexedField, err error) *Search {
	return &Search{db: db, object: o, fields: f, err: err}
}

// And performs a new Search while "ANDing" search results
func (r *Search) And(field, operator string, value interface{}) *Search {
	if r.Err() != nil {
		return r
	}
	return r.db.search(r.object, field, operator, value, r.fields)
}

// And performs a new Search while "ORing" search results
func (r *Search) Or(field, operator string, value interface{}) *Search {
	if r.Err() != nil {
		return r
	}
	new := r.db.search(r.object, field, operator, value, nil)
	marked := make(map[uint64]bool)
	// we mark the fields of the new search
	for _, f := range new.fields {
		marked[f.ObjectId] = true
	}
	for _, f := range r.fields {
		// we concat the searches while deduplicating
		if _, ok := marked[f.ObjectId]; !ok {
			new.fields = append(new.fields, f)
		}
	}
	return new
}

// Len returns the number of data returned by the search
func (r *Search) Len() int {
	return len(r.fields)
}

// Iterator returns an Iterator convenient to iterate over
// the objects resulting from the search
func (r *Search) Iterator() (it *Iterator, err error) {
	var s *Schema

	if s, err = r.db.schema(r.object); err != nil {
		return
	}

	it = &Iterator{db: r.db, t: typeof(r.object)}
	it.uuids = make([]string, 0, len(r.fields))

	for _, f := range r.fields {
		it.uuids = append(it.uuids, s.ObjectsIndex.ObjectIds[f.ObjectId])
	}

	return
}

// Collect all the objects resulting from the search
func (r *Search) Collect() (out []Object, err error) {
	var it *Iterator
	var o Object

	if r.Err() != nil {
		return nil, r.Err()
	}

	if it, err = r.Iterator(); err != nil {
		return
	}

	out = make([]Object, 0, it.Len())
	for o, err = it.Next(); err == nil && err != ErrEOI; o, err = it.Next() {
		out = append(out, o)
	}

	// normal end of iterator
	if err == ErrEOI {
		err = nil
	}

	return
}

// Err return any error encountered while searching
func (r *Search) Err() error {
	return r.err
}
