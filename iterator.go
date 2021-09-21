package sod

import (
	"errors"
	"reflect"
)

var (
	ErrEOI = errors.New("end of iterator")
)

type Iterator struct {
	db      *DB
	t       reflect.Type
	i       int
	reverse bool
	uuids   []string
}

func (it *Iterator) Reverse() *Iterator {
	it.reverse = true
	it.i = len(it.uuids) - 1
	return it
}

func (it *Iterator) Next() (o Object, err error) {
	if it.i < len(it.uuids) && it.i >= 0 {
		o = reflect.New(it.t).Interface().(Object)
		o.Initialize(it.uuids[it.i])
		err = it.db.Get(o)
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

func (it *Iterator) Len() int {
	return len(it.uuids)
}
