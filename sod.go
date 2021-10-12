package sod

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var (
	DefaultPermissions = fs.FileMode(0700)
	LowercaseNames     = true

	uuidRegexp = regexp.MustCompile(`(?i:^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$)`)
)

type DB struct {
	sync.RWMutex
	root    string
	schemas map[string]*Schema
}

/***** Private Methods ******/

func (db *DB) saveSchema(o Object, s *Schema, override bool) (err error) {
	var data []byte

	dir := db.oDir(o)
	path := filepath.Join(dir, SchemaFilename)

	if err = os.MkdirAll(dir, DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(s); err != nil {
		return
	}

	if override || !isFileAndExist(path) {
		if err = ioutil.WriteFile(path, data, DefaultPermissions); err != nil {
			return
		}
	}

	return
}

func (db *DB) loadSchema(of Object) (s *Schema, err error) {

	var stat os.FileInfo

	path := filepath.Join(db.oDir(of), SchemaFilename)

	if stat, err = os.Stat(path); err != nil {
		return
	}

	if stat.Mode().IsRegular() {
		if err = UnmarshalJsonFile(path, &s); err != nil {
			return
		}
		s.Initialize(of)
		db.schemas[stype(of)] = s
		return
	}

	err = ErrBadSchema
	return
}

func (db *DB) schema(of Object) (s *Schema, err error) {
	var ok bool

	if s, ok = db.schemas[stype(of)]; ok {
		return
	}

	return db.loadSchema(of)
}

func (db *DB) itemname(o Object) string {
	if LowercaseNames {
		return strings.ToLower(stype(o))
	}
	return stype(o)
}

func (db *DB) oDir(of Object) string {
	return filepath.Join(db.root, db.itemname(of))
}

func (db *DB) oPath(of Object) (path string, err error) {
	var s *Schema

	if s, err = db.schema(of); err != nil {
		return
	}

	return filepath.Join(db.oDir(of), filename(of, s)), nil
}

func (db *DB) exist(o Object) (ok bool, err error) {
	var path string

	if path, err = db.oPath(o); err != nil {
		return
	}

	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return stat.Mode().IsRegular() && err == nil, nil
}

func (db *DB) insertOrUpdate(o Object, commit bool) (err error) {
	var path string
	var data []byte
	var schema *Schema

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

	if schema, err = db.schema(o); err != nil {
		return
	}

	if err = schema.Index(o); err != nil {
		return
	}

	if path, err = db.oPath(o); err != nil {
		return
	}

	if err = os.MkdirAll(filepath.Dir(path), DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(o); err != nil {
		return
	}

	if err = ioutil.WriteFile(path, data, DefaultPermissions); err != nil {
		return
	}

	if commit {
		return db.commit(o)
	}

	return

}

func (db *DB) delete(o Object) (err error) {
	var s *Schema
	var path string

	if s, err = db.schema(o); err != nil {
		return
	}

	// Unindexing object
	s.Unindex(o)
	path = filepath.Join(db.oDir(o), filename(o, s))

	return os.Remove(path)
}

func (db *DB) search(o Object, field, operator string, value interface{}, constrain []*IndexedField) *Search {
	var s *Schema
	var f []*IndexedField
	var err error

	if s, err = db.schema(o); err != nil {
		return &Search{err: err}
	}

	if f, err = s.ObjectsIndex.Search(o, field, operator, value, constrain); err != nil {
		// if the field is not indexed we have to go through all the collection
		if errors.Is(err, ErrFieldNotIndexed) {
			return db.searchAll(o, field, operator, value, constrain)
		}
		return &Search{err: err}
	} else {
		return newSearch(db, o, f, err)
	}
}

/***** Public Methods ******/

// Open opens a Simple Object Database
func Open(root string) *DB {
	return &DB{root: root, schemas: map[string]*Schema{}}
}

// Create a schema for an Object
func (db *DB) Create(o Object, s Schema) (err error) {
	db.Lock()
	defer db.Unlock()

	// the schema is existing and we don't need to build a new one
	if _, err = db.schema(o); err == nil {
		return
	}

	// we need to create a new schema
	s.Initialize(o)

	if err = db.saveSchema(o, &s, false); err != nil {
		return
	}

	db.schemas[stype(o)] = &s

	return
}

// Schema retrieves the schema of an Object
func (db *DB) Schema(of Object) (s *Schema, err error) {
	db.RLock()
	defer db.RUnlock()

	return db.schema(of)
}

// Get gets a single Object from the DB
func (db *DB) Get(o Object) (err error) {
	db.RLock()
	defer db.RUnlock()

	var path string

	if path, err = db.oPath(o); err != nil {
		return err
	}

	return UnmarshalJsonFile(path, o)
}

// All returns all Objects in the DB
func (db *DB) All(of Object) (out []Object, err error) {
	db.RLock()
	defer db.RUnlock()

	var o Object
	var it *Iterator

	if it, err = db.Iterator(of); err != nil {
		return
	}

	out = make([]Object, 0, it.Len())
	for o, err = it.Next(); err == nil && err != ErrEOI; o, err = it.Next() {
		out = append(out, o)
	}

	if err == ErrEOI {
		err = nil
	}
	return
}

func (db *DB) searchAll(o Object, field, operator string, value interface{}, constrain []*IndexedField) *Search {
	var iter *Iterator
	var err error
	var s *Schema

	f := make([]*IndexedField, 0)
	search := searchField(value)

	if s, err = db.schema(o); err != nil {
		return &Search{err: err}
	}

	// building up the iterator out of constrain
	if constrain != nil {
		uuids := make([]string, 0, len(constrain))
		for _, c := range constrain {
			uuids = append(uuids, s.ObjectsIndex.ObjectIds[c.ObjectId])
		}
		iter = &Iterator{db: db, i: 0, uuids: uuids, t: typeof(o)}
	} else if iter, err = db.Iterator(o); err != nil {
		return &Search{err: err}
	}

	// we go through the iterator
	for obj, err := iter.Next(); err == nil && err != ErrEOI; obj, err = iter.Next() {
		var test *IndexedField
		var value interface{}

		if index, ok := s.ObjectsIndex.uuids[obj.UUID()]; ok {
			if value, ok = fieldByName(obj, field); !ok {
				return &Search{err: fmt.Errorf("%w %s", ErrUnkownField, field)}
			}
			if test, err = NewIndexedField(value, index); err != nil {
				return &Search{err: err}
			}
			if test.Evaluate(operator, search) {
				f = append(f, test)
			}
		} else {
			panic("index corrupted")
		}
	}
	if err == ErrEOI {
		err = nil
	}
	return newSearch(db, o, f, err)

}

// Search Object where field matches value according to an operator
func (db *DB) Search(o Object, field, operator string, value interface{}) *Search {
	db.RLock()
	defer db.RUnlock()

	return db.search(o, field, operator, value, nil)
}

// Iterator returns an Object Iterator
func (db *DB) Iterator(of Object) (it *Iterator, err error) {
	db.RLock()
	defer db.RUnlock()

	var s *Schema
	var uuids []string

	if s, err = db.schema(of); err != nil {
		return
	}

	if s.ObjectsIndex != nil {
		uuids = make([]string, 0, len(s.ObjectsIndex.uuids))
		for uuid := range s.ObjectsIndex.uuids {
			uuids = append(uuids, uuid)
		}
	} else {
		err = fmt.Errorf("%T %w", stype(of), ErrMissingObjIndex)
		return
	}

	// building up the iterator
	it = &Iterator{db: db, i: 0, uuids: uuids, t: typeof(of)}

	return
}

// Count the number of Object in the database
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

// DeleteAll deletes all Objects of the same type and commit changes
func (db *DB) DeleteAll(of Object) (err error) {
	var it *Iterator
	if it, err = db.Iterator(of); err != nil {
		return
	}
	return db.DeleteObjects(it)
}

// DeleteObjects deletes Objects from an Iterator and commit changes.
// This primitive can be used for bulk deletions.
func (db *DB) DeleteObjects(from *Iterator) (err error) {
	var o Object

	defer db.commit(from.object())

	for o, err = from.Next(); err == nil || err != ErrEOI; o, err = from.Next() {
		if err = db.delete(o); err != nil {
			return
		}
	}

	// end of iterator is not considered as an error to report
	if err == ErrEOI {
		err = nil
	}

	return
}

// Delete deletes a single Object from the database and commit changes
func (db *DB) Delete(o Object) (lastErr error) {
	db.Lock()
	defer db.Unlock()
	if err := db.delete(o); err != nil {
		lastErr = err
	}

	if err := db.commit(o); err != nil {
		lastErr = err
	}

	return
}

// Exist returns true if the object exist.
func (db *DB) Exist(o Object) (ok bool, err error) {
	db.RLock()
	defer db.RUnlock()
	return db.exist(o)
}

// InsertOrUpdateBulk inserts objects in bulk in the DB. This
// function is locking the DB and will terminate only when input
// channel is closed
func (db *DB) InsertOrUpdateBulk(in chan Object) (err error) {
	db.Lock()
	defer db.Unlock()
	var o Object

	for o = range in {
		if err == nil {
			err = db.insertOrUpdate(o, false)
		}
	}

	if o != nil && err == nil {
		return db.commit(o)
	}

	return
}

// InsertOrUpdate inserts or updates a single Object and commits
// changes. This method is not suited for bulk insertions as each
// insert will trigger a write overhead. For
// bulk insertion use InsertOrUpdateBulk function
func (db *DB) InsertOrUpdate(o Object) (err error) {
	db.Lock()
	defer db.Unlock()

	return db.insertOrUpdate(o, true)
}

func (db *DB) commit(o Object) (err error) {
	var schema *Schema

	if schema, err = db.schema(o); err != nil {
		return
	}

	if err = db.saveSchema(o, schema, true); err != nil {
		return
	}

	return
}

// Control controls checks for inconsistencies in DB
func (db *DB) Control() (err error) {
	db.Lock()
	defer db.Unlock()

	for _, s := range db.schemas {
		if err = s.control(); err != nil {
			return
		}
	}
	return
}

// Commit object schema on the disk. This method must
// be called after Insert/Delete operations.
func (db *DB) Commit(o Object) (err error) {
	db.Lock()
	defer db.Unlock()

	return db.commit(o)
}

func (db *DB) Close() (last error) {
	db.Lock()
	defer db.Unlock()

	for _, s := range db.schemas {
		if err := db.commit(s.object); err != nil {
			last = err
		}
	}

	return
}
