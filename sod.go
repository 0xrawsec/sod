package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var (
	DefaultPermissions = fs.FileMode(0700)
	LowercaseNames     = true

	uuidRegexp = regexp.MustCompile(`(?i:^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$)`)
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

type DB struct {
	sync.RWMutex
	root    string
	schemas map[string]*Schema
}

func Open(root string) *DB {
	return &DB{root: root, schemas: map[string]*Schema{}}
}

func (db *DB) itemname(o Object) string {
	if LowercaseNames {
		return strings.ToLower(stype(o))
	}
	return stype(o)
}

func (db *DB) Create(o Object, s *Schema) (err error) {
	db.Lock()
	defer db.Unlock()

	var data []byte

	dir := filepath.Join(db.root, db.itemname(o))
	path := filepath.Join(dir, SchemaFilename)

	if err = os.MkdirAll(dir, DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(s); err != nil {
		return
	}

	if err = ioutil.WriteFile(path, data, DefaultPermissions); err != nil {
		return
	}

	db.schemas[stype(o)] = s

	return
}

func (db *DB) Schema(o Object) (s *Schema, err error) {
	var ok bool
	var stat os.FileInfo

	if s, ok = db.schemas[stype(o)]; ok {
		return
	}

	path := filepath.Join(db.root, db.itemname(o), SchemaFilename)
	if stat, err = os.Stat(path); err != nil {
		return
	}

	if stat.Mode().IsRegular() {
		if err = UnmarshalJsonFile(path, &s); err != nil {
			return
		}
		db.schemas[stype(o)] = s
		return
	}

	err = ErrBadSchema
	return
}

func (db *DB) Get(o Object) (err error) {
	db.RLock()
	defer db.RUnlock()

	var s *Schema

	if s, err = db.Schema(o); err != nil {
		return
	}

	path := filepath.Join(db.root, db.itemname(o), filename(o, s))
	return UnmarshalJsonFile(path, o)
}

func (db *DB) All(of Object) (out []Object, err error) {
	db.RLock()
	defer db.RUnlock()

	var it *Iterator

	if it, err = db.Iterator(of); err != nil {
		return
	}

	out = make([]Object, 0, it.Len())
	for o, err := it.Next(); err == nil || err != ErrEOI; o, err = it.Next() {
		out = append(out, o)
	}
	return
}

func (db *DB) Iterator(of Object) (it *Iterator, err error) {
	db.RLock()
	defer db.RUnlock()

	var s *Schema
	var entries []os.DirEntry

	dir := filepath.Join(db.root, db.itemname(of))

	if s, err = db.Schema(of); err != nil {
		return
	}

	if entries, err = os.ReadDir(dir); err != nil {
		err = fmt.Errorf("failed to read object directory: %w", err)
		return
	}

	uuids := make([]string, 0, len(entries))

	for _, e := range entries {
		if e.Type().IsRegular() {
			uuid, ext := uuidExt(e.Name())
			if ext == s.Extension {
				if uuidRegexp.MatchString(uuid) {
					uuids = append(uuids, uuid)
				}
			}
		}
	}

	// building up the iterator
	it = &Iterator{db: db, i: 0, uuids: uuids, t: typeof(of)}

	return
}

func (db *DB) Count(of Object) (n int, err error) {
	var it *Iterator

	if it, err = db.Iterator(of); err != nil {
		return
	}

	n = it.Len()
	return
}

// Drop drops all the database
func (db *DB) Drop() (err error) {
	db.Lock()
	defer db.Unlock()

	path := filepath.Join(db.root)
	return os.RemoveAll(path)
}

// DropObjects drops all Objects of the same type as from
func (db *DB) DropObjects(of Object) (err error) {
	var it *Iterator
	var o Object

	if it, err = db.Iterator(of); err != nil {
		return
	}

	for o, err = it.Next(); err == nil || err != ErrEOI; o, err = it.Next() {
		if err = db.DropObject(o); err != nil {
			return
		}
	}
	return
}

// DropObject deletes a single Object from the database
func (db *DB) DropObject(o Object) (err error) {
	db.Lock()
	defer db.Unlock()
	var s *Schema

	if s, err = db.Schema(o); err != nil {
		return
	}

	path := filepath.Join(db.root, db.itemname(o), filename(o, s))
	return os.Remove(path)
}

func (db *DB) exist(o Object) (ok bool, err error) {
	var s *Schema

	if s, err = db.Schema(o); err != nil {
		return
	}

	path := filepath.Join(db.root, db.itemname(o), filename(o, s))
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return stat.Mode().IsRegular() && err == nil, nil
}

func (db *DB) Exist(o Object) (ok bool, err error) {
	db.RLock()
	defer db.RUnlock()
	return db.exist(o)
}

// Update updates a single Object
func (db *DB) Update(o Object) (err error) {
	db.Lock()
	defer db.Unlock()

	var data []byte
	var s *Schema

	if s, err = db.Schema(o); err != nil {
		return
	}

	// this is a new object, we have to handle here
	// potential uuid duplicates (even though it is very unlikely)
	if o.UUID() == "" {
		for ok := true; ok; {
			o.Initialize(uuidOrPanic())
			ok, err = db.exist(o)
			if err != nil {
				return
			}
		}
	}

	dir := filepath.Join(db.root, db.itemname(o))
	path := filepath.Join(dir, filename(o, s))

	if err = os.MkdirAll(dir, DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(o); err != nil {
		return
	}

	return ioutil.WriteFile(path, data, DefaultPermissions)
}
