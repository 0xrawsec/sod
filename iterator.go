package sod

import (
	"errors"
	"reflect"
)

var (
	ErrEOI = errors.New("end of iterator")
)

type Iterator struct {
	db    *DB
	t     reflect.Type
	i     int
	uuids []string
}

func (it *Iterator) Next() (o Object, err error) {
	if it.i < len(it.uuids) {
		o = reflect.New(it.t).Interface().(Object)
		o.Initialize(it.uuids[it.i])
		err = it.db.Get(o)
		it.i++
		return
	}
	err = ErrEOI
	return
}

func (it *Iterator) Len() int {
	return len(it.uuids)
}
