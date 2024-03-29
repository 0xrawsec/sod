package sod

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

var (
	ErrUnknownOperator           = errors.New("unknown logical operator")
	ErrNoObjectFound             = errors.New("no object found")
	ErrUnexpectedNumberOfResults = errors.New("unexpected number of results")
)

func IsNoObjectFound(err error) bool {
	return errors.Is(err, ErrNoObjectFound)
}

// Search helper structure to easily build search queries on objects
// and retrieve the results
type Search struct {
	db      *DB
	object  Object
	fields  []*indexedField
	limit   uint64
	reverse bool
	err     error
}

func newSearch(db *DB, o Object, f []*indexedField, err error) *Search {
	return &Search{db: db, object: o, fields: f, limit: math.MaxUint, err: err}
}

// ExpectsZeroOrN checks that the number of results is the one expected or zero.
// If not, next call to s.Err must return an error and any subsbequent
// attempt to collect results must fail
func (s *Search) ExpectsZeroOrN(n int) *Search {
	found := len(s.fields)

	if s.err != nil {
		return s
	}

	if found != 0 && found != n {
		s.err = fmt.Errorf("%w expected %d, found %d", ErrUnexpectedNumberOfResults, n, found)
	}

	return s
}

// Expects checks that the number of results is the one expected
// if not, next call to s.Err must return an error and any subsbequent
// attempt to collect results must fail
func (s *Search) Expects(n int) *Search {
	found := len(s.fields)

	if s.err != nil {
		return s
	}

	if found != n {
		s.err = fmt.Errorf("%w expected %d, found %d", ErrUnexpectedNumberOfResults, n, found)
	}

	return s
}

// Operation performs a new Search while ANDing or ORing the results
// operator must be in ["and", "&&", "or", "||"]
func (s *Search) Operation(operator, field, comparator string, value interface{}) *Search {

	op := strings.ToLower(operator)

	if s.err != nil {
		return s
	}

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
	if s.err != nil {
		return s
	}

	return s.db.search(s.object, field, operator, value, s.fields)
}

// Or performs a new Search while "ORing" search results
func (s *Search) Or(field, operator string, value interface{}) *Search {

	if s.err != nil {
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
func (s *Search) Iterator() (it *iterator, err error) {
	var sch *Schema

	if s.err != nil {
		return nil, s.err
	}

	if sch, err = s.db.schema(s.object); err != nil {
		return
	}

	// create a new iterator
	it = newIterator(s.db, s.object, make([]string, 0, len(s.fields)))

	for _, f := range s.fields {
		it.uuids = append(it.uuids, sch.ObjectIndex.ObjectIds[f.ObjectId])
	}

	return
}

// Delete deletes the objects found by the search
func (s *Search) Delete() (err error) {
	var it *iterator

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
	s.db.RLock()
	defer s.db.RUnlock()

	return s.one()
}

// AssignUnique checks there is only one result in search and assign it
// to target. If search retuns more than one result, ErrUnexpectedNumberOfResults
// is returned
func (s *Search) AssignUnique(target interface{}) (err error) {
	s.ExpectsZeroOrN(1)
	return s.AssignOne(target)
}

// AssignOne returns the first result found calling Collect function
// and assign the Object found to target. Target must be a *sod.Object
// otherwise the function panics. If no Object is found, ErrNoObjectFound
// is returned
func (s *Search) AssignOne(target interface{}) (err error) {
	s.db.RLock()
	defer s.db.RUnlock()

	var o Object

	if o, err = s.one(); err != nil {
		return err
	}

	AssignOne(o, target)

	return
}

// Assign returns results found calling Collect function
// and assign them to target. Target must be a *[]sod.Object
// otherwise the function panics. If no Object is found, ErrNoObjectFound
// is returned
func (s *Search) Assign(target interface{}) (err error) {
	s.db.RLock()
	defer s.db.RUnlock()

	var objs []Object

	if objs, err = s.collect(); err != nil {
		return err
	}

	return Assign(objs, target)
}

// Collect all the objects resulting from the search.
// If a search has been made on an indexed field, results
// will be in descending order by default. If you want to change
// result order, call Reverse before.
// NB: only search on indexed field(s) will be garanteed to be
// ordered according to the last field searched.
func (s *Search) Collect() (out []Object, err error) {
	s.db.RLock()
	defer s.db.RUnlock()

	return s.collect()
}

// Err return any error encountered while searching
func (s *Search) Err() error {
	return s.err
}

/************** Private Methods ******************/

func (s *Search) one() (o Object, err error) {
	var sr []Object

	if s.err != nil {
		err = s.err
		return
	}

	if s.Len() == 0 {
		err = ErrNoObjectFound
		return
	}

	// prevent collecting all results and using only one
	s.limit = 1
	if sr, err = s.collect(); err != nil {
		return
	}
	o = sr[0]
	return
}

func (s *Search) collect() (out []Object, err error) {
	var it *iterator
	var o Object

	if s.err != nil {
		return nil, s.err
	}

	if it, err = s.Iterator(); err != nil {
		return
	}

	if s.reverse {
		it.reversed()
	}

	out = make([]Object, 0, it.len())
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
