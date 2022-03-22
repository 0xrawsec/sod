package sod

import (
	"errors"
	"reflect"
)

var (
	ErrEOI = errors.New("end of iterator")
)

type iterator struct {
	db      *DB
	t       reflect.Type
	i       int
	reverse bool
	uuids   []string
}

// newIterator creates a new iterator to iterate over Objects from their uuids
func newIterator(db *DB, of Object, uuids []string) *iterator {
	return &iterator{db: db, i: 0, uuids: uuids, t: typeof(of)}
}

// reversed iterates over the iterator in reverse order
func (it *iterator) reversed() *iterator {
	it.reverse = true
	it.i = len(it.uuids) - 1
	return it
}

// len returns the length of the iterator
func (it *iterator) len() int {
	return len(it.uuids)
}

func (it *iterator) object() Object {
	return reflect.New(it.t).Interface().(Object)
}

// next return the next Object of Iterator. It returns
// ErrEOI when no more objects are available.
func (it *iterator) next() (o Object, err error) {
	if it.i < len(it.uuids) && it.i >= 0 {
		o = it.object()
		o.Initialize(it.uuids[it.i])
		o, err = it.db.get(o)
		if it.reverse {
			it.i--
		} else {
			it.i++
		}
		return
	}
	err = ErrEOI
	return
}
