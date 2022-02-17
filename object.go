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
	// UUID returns a unique identifier used to store the
	// Object in the database
	UUID() string

	// Initialize is called to initialize the UUID associated
	// to an Object
	Initialize(string)

	// Transform is called prior to Object insertion and
	// can be used to apply some transformation on the data
	// to insert.
	Transform()

	// Validate is called every time an Object is inserted
	// if an error is returned by this function the Object
	// will not be inserted.
	Validate() error
}

// Item is a base structure implementing Object interface
type Item struct {
	uuid string
}

func (o *Item) Initialize(uuid string) {
	o.uuid = uuid
}

func (o *Item) UUID() string {
	return o.uuid
}

func (o *Item) Transform() {}

func (o *Item) Validate() error {
	return nil
}
