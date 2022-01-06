package sod

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidObject = errors.New("object is not valid")
)

func ValidationErr(o Object, err error) error {
	return fmt.Errorf("%s %w: %s", stype(o), ErrInvalidObject, err)
}

type Object interface {
	UUID() string
	Initialize(string)
	Validate() error
}

type Item struct {
	uuid string
}

func (o *Item) Initialize(uuid string) {
	o.uuid = uuid
}

func (o *Item) UUID() string {
	return o.uuid
}

func (o *Item) Validate() error {
	return nil
}
