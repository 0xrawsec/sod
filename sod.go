package sod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var (
	DefaultPermissions = fs.FileMode(0700)
	LowercaseNames     = true

	uuidRegexp = regexp.MustCompile(`(?i:^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$)`)
)

type objectStore struct {
	m map[string]map[string]Object
}

func newObjectStore() *objectStore {
	return &objectStore{make(map[string]map[string]Object)}
}

func (c *objectStore) key(o Object) string {
	return stype(o)
}

func (c *objectStore) put(o Object) {
	k := stype(o)
	if _, ok := c.m[k]; !ok {
		c.m[k] = make(map[string]Object)
	}
	c.m[k][o.UUID()] = o
}

func (c *objectStore) get(in Object) (out Object, ok bool) {
	k := stype(in)
	if _, ok = c.m[k]; ok {
		out, ok = c.m[k][in.UUID()]
	}
	return
}

func (c *objectStore) delete(o Object) {
	k := stype(o)
	if _, ok := c.m[k]; ok {
		delete(c.m[k], o.UUID())
	}
}

func (c *objectStore) count(of Object) (n int) {
	k := stype(of)
	if _, ok := c.m[k]; ok {
		return len(c.m[k])
	}
	return
}

type DB struct {
	l       sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	root    string
	cache   *objectStore
	asyncw  *objectStore
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

func (db *DB) startAsyncWritesRoutine(s *Schema) {
	step := time.Millisecond * 100
	if s.asyncWritesEnabled() && !s.AsyncWrites.routineStarted {
		s.AsyncWrites.routineStarted = true
		go func() {
			for db.ctx.Err() == nil {
				for slept := time.Duration(0); ; slept += step {
					n := db.safeCountPendingAsyncW(s.object)
					if n >= s.AsyncWrites.Threshold || slept >= s.AsyncWrites.Timeout {
						if err := db.FlushAll(s.object); err != nil {
							panic(err)
						}
						break
					}
					time.Sleep(step)
				}
			}
		}()
	}
}

func (db *DB) safeCountPendingAsyncW(of Object) (n int) {
	db.RLock()
	defer db.RUnlock()
	return db.asyncw.count(of)
}

func (db *DB) schema(of Object) (s *Schema, err error) {
	var ok bool

	if s, ok = db.schemas[stype(of)]; ok {
		db.startAsyncWritesRoutine(s)
		return
	}

	return db.loadSchema(of)
}

func (db *DB) itemname(o Object) string {
	if LowercaseNames {
		return CamelToSnake(stype(o))
	}
	return stype(o)
}

func (db *DB) oDir(of Object) string {
	return filepath.Join(db.root, db.itemname(of))
}

func (db *DB) oPath(s *Schema, of Object) (path string) {
	return filepath.Join(db.oDir(of), filename(of, s))
}

func (db *DB) exist(o Object) (ok bool, err error) {
	var path string
	var s *Schema

	if s, err = db.schema(o); err != nil {
		return
	}

	path = db.oPath(s, o)
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return stat.Mode().IsRegular() && err == nil, nil
}

func (db *DB) writeObject(o Object, path string) (err error) {
	var data []byte

	if err = os.MkdirAll(filepath.Dir(path), DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(o); err != nil {
		return
	}

	if err = ioutil.WriteFile(path, data, DefaultPermissions); err != nil {
		return
	}
	return
}

// gets a single Object from the DB
func (db *DB) get(in Object) (out Object, err error) {
	var path string
	var ok bool
	var s *Schema

	if s, err = db.schema(in); err != nil {
		return
	}

	// we return object if cached
	if s.mustCache() {
		if out, ok = db.cache.get(in); ok {
			return
		}
	}

	path = filepath.Join(db.oDir(in), filename(in, s))
	err = UnmarshalJsonFile(path, in)
	out = in

	// we cache the object
	if s.mustCache() {
		db.cache.put(out)
	}
	return
}

func (db *DB) insertOrUpdate(o Object, commit bool) (err error) {
	var path string
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

	if schema.mustCache() {
		db.cache.put(o)
	}

	if err = schema.Index(o); err != nil {
		return
	}

	if schema.asyncWritesEnabled() {
		// we don't write object to disk but store
		// it in a structure for later saving
		db.asyncw.put(o)
	} else {
		// writing the object to disk
		path = db.oPath(schema, o)
		if err = db.writeObject(o, path); err != nil {
			return
		}

		// commiting schema and index to disk
		if commit {
			return db.commit(o)
		}
	}

	return
}

func (db *DB) delete(o Object) (err error) {
	var s *Schema
	var path string

	if s, err = db.schema(o); err != nil {
		return
	}

	// deleting from cache
	if s.mustCache() {
		db.cache.delete(o)
		db.asyncw.delete(o)
	}

	// unindexing object
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

func (db *DB) flushDB() (err error) {
	for key := range db.asyncw.m {
		var s *Schema
		for _, o := range db.asyncw.m[key] {
			// we get schema
			if s == nil {
				if s, err = db.schema(o); err != nil {
					return
				}
			}
			if e := db.writeObject(o, db.oPath(s, o)); e != nil {
				err = e
			}
			// we delete object from the list of objects to write
			db.asyncw.delete(o)
		}
	}
	return
}

/***** Public Methods ******/

// Open opens a Simple Object Database
func Open(root string) *DB {
	ctx, cancel := context.WithCancel(context.Background())
	return &DB{
		ctx:     ctx,
		cancel:  cancel,
		root:    root,
		cache:   newObjectStore(),
		asyncw:  newObjectStore(),
		schemas: map[string]*Schema{}}
}

func (db *DB) Lock() {
	//dbgLock("Lock")
	db.l.Lock()
}

func (db *DB) RLock() {
	//dbgLock("RLock")
	db.l.RLock()
}

func (db *DB) Unlock() {
	//dbgLock("Unlock")
	db.l.Unlock()
}

func (db *DB) RUnlock() {
	//dbgLock("RUnlock")
	db.l.RUnlock()
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
func (db *DB) Get(in Object) (out Object, err error) {
	db.RLock()
	defer db.RUnlock()

	return db.get(in)
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
	for o, err = it.next(); err == nil && err != ErrEOI; o, err = it.next() {
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
	db.Lock()
	defer db.Unlock()

	var o Object

	defer db.commit(from.object())

	for o, err = from.next(); err == nil || err != ErrEOI; o, err = from.next() {
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

// InsertOrUpdateBulk inserts objects in bulk in the DB. A chunk size needs to be
// provided to commit the DB at every chunk. The DB is locked at every chunk
// processed, so changing the chunk size impact other concurrent DB operations.
func (db *DB) InsertOrUpdateBulk(in chan Object, csize int) (err error) {
	var o Object
	chunk := make([]Object, 0, csize)
	for o = range in {
		chunk = append(chunk, o)
		if len(chunk) == csize {
			if err = db.InsertOrUpdateMany(chunk...); err != nil {
				return
			}
			chunk = make([]Object, 0, csize)
		}
	}

	return db.InsertOrUpdateMany(chunk...)
}

// InsertOrUpdateMany inserts several objects into the DB and
// commit schema after all insertions. It is faster than calling
// InsertOrUpdate for every objects separately.
func (db *DB) InsertOrUpdateMany(objects ...Object) (err error) {
	db.Lock()
	defer db.Unlock()

	// we validate all the objects prior to insertion
	for _, o := range objects {
		if err := o.Validate(); err != nil {
			return ValidationErr(o, err)
		}
	}

	for _, o := range objects {
		if e := db.insertOrUpdate(o, false); e != nil {
			err = e
			break
		}
	}

	if len(objects) > 0 {
		if e := db.commit(objects[0]); e != nil {
			return e
		}
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

	if err := o.Validate(); err != nil {
		return ValidationErr(o, err)
	}

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

// Flush a single object to disk. Flush does not commit schema
func (db *DB) Flush(o Object) (err error) {
	db.Lock()
	defer db.Unlock()

	var s *Schema

	if s, err = db.schema(o); err != nil {
		return
	}

	if e := db.writeObject(o, db.oPath(s, o)); e != nil {
		err = e
	}
	// we delete object from the list of objects to save
	db.asyncw.delete(o)

	return
}

// FlushAll objects of type to disk. As Flush this function
// does not commit schema to disk
func (db *DB) FlushAll(of Object) (err error) {
	db.Lock()
	defer db.Unlock()

	var s *Schema

	if s, err = db.schema(of); err != nil {
		return
	}

	key := db.asyncw.key(of)

	if _, ok := db.asyncw.m[key]; ok {
		for _, o := range db.asyncw.m[key] {
			if e := db.writeObject(o, db.oPath(s, o)); e != nil {
				err = e
			}
			// we delete object from the list of objects to save
			db.asyncw.delete(o)
		}
	}

	return
}

// Close closes gently the DB by flushing any pending async writes
// and by committing all the schemas to disk
func (db *DB) Close() (last error) {
	db.Lock()
	defer db.Unlock()

	// cancelling db context
	db.cancel()

	// flushing all the objects of all kinds on disk
	if err := db.flushDB(); err != nil {
		last = err
	}

	// committing all the schemas to disk
	for _, s := range db.schemas {
		if err := db.commit(s.object); err != nil {
			last = err
		}
	}

	return
}
