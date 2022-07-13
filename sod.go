package sod

import (
	"bytes"
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
	LowercaseNames     = false
	ErrWrongObjectType = errors.New("wrong objet type")

	uuidRegexp = regexp.MustCompile(`(?i:^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$)`)
)

type objectMap struct {
	sync.RWMutex
	m map[string]Object
}

func newObjectMap() *objectMap {
	return &objectMap{m: make(map[string]Object)}
}

func (m *objectMap) put(o Object) {
	m.Lock()
	defer m.Unlock()
	m.m[o.UUID()] = CloneObject(o)
}

func (m *objectMap) get(uuid string) (o Object, ok bool) {
	m.RLock()
	defer m.RUnlock()
	if o, ok = m.m[uuid]; ok {
		o = CloneObject(o)
	}
	return
}

func (m *objectMap) delete(uuid string) {
	delete(m.m, uuid)
}

func (m *objectMap) lockDelete(uuid string) {
	m.Lock()
	defer m.Unlock()
	m.delete(uuid)
}

func (m *objectMap) len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.m)
}

func (m *objectMap) flush(db *DB) (err error) {
	m.Lock()
	defer m.Unlock()

	for _, o := range m.m {
		if e := db.writeObject(o); e != nil {
			err = e
		}
		// we delete object from the list of objects to save
		m.delete(o.UUID())
	}
	return
}

type objectStore struct {
	sync.RWMutex
	m map[string]*objectMap
}

func newObjectStore() *objectStore {
	return &objectStore{m: make(map[string]*objectMap)}
}

func (s *objectStore) key(o Object) string {
	return stype(o)
}

func (s *objectStore) put(o Object) {
	s.Lock()
	defer s.Unlock()

	k := stype(o)
	if _, ok := s.m[k]; !ok {
		s.m[k] = newObjectMap()
	}
	s.m[k].put(o)
}

func (s *objectStore) get(in Object) (out Object, ok bool) {
	s.RLock()
	defer s.RUnlock()

	k := stype(in)
	if _, ok = s.m[k]; ok {
		out, ok = s.m[k].get(in.UUID())
	}
	return
}

func (s *objectStore) delete(o Object) {
	s.Lock()
	defer s.Unlock()

	k := stype(o)
	if _, ok := s.m[k]; ok {
		s.m[k].lockDelete(o.UUID())
	}
}

func (s *objectStore) count(of Object) (n int) {
	s.RLock()
	defer s.RUnlock()

	k := stype(of)
	if _, ok := s.m[k]; ok {
		return s.m[k].len()
	}
	return
}

func (s *objectStore) flush(db *DB) (err error) {
	s.Lock()
	defer s.Unlock()

	for key := range s.m {
		if e := s.m[key].flush(db); e != nil {
			err = e
		}
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

func (db *DB) deleteSchema(o Object) (err error) {
	var ok bool

	dir := db.oDir(o)
	path := filepath.Join(dir, SchemaFilename)
	skey := stype(o)

	if _, ok = db.schemas[skey]; ok {
		delete(db.schemas, skey)
	}

	return os.Remove(path)
}

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
		if err = unmarshalJsonFile(path, &s); err != nil {
			return
		}

		// we initialize schema from object
		if err = s.initialize(db, of); err != nil {
			return
		}

		// we control schema and if object struct did not change
		// we allow to cache schema if index is corrupted
		if err = s.control(); err != nil && !errors.Is(err, ErrIndexCorrupted) {
			return
		}

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
						// enter critical section
						db.Lock()
						// checking db.ctx not to race with db.Close function
						if db.ctx.Err() == nil {
							if err := db.flushAllAndCommit(s.object); err != nil {
								panic(err)
							}
						}
						db.Unlock()
						// leave critical section
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
		return camelToSnake(stype(o))
	}
	return stype(o)
}

func (db *DB) oDir(of Object) string {
	return filepath.Join(db.root, db.itemname(of))
}

func (db *DB) oPath(s *Schema, of Object) (path string) {
	return filepath.Join(db.oDir(of), s.filename(of))
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

func (db *DB) writeObject(o Object) (err error) {
	var data []byte
	var s *Schema

	if s, err = db.schema(o); err != nil {
		return
	}

	path := db.oPath(s, o)
	if err = os.MkdirAll(filepath.Dir(path), DefaultPermissions); err != nil {
		return
	}

	if data, err = json.Marshal(o); err != nil {
		return
	}

	if err = writeReader(path, bytes.NewBuffer(data), DefaultPermissions, s.Compress); err != nil {
		return
	}

	return
}

func (db *DB) getByUUID(in Object, uuid string) (out Object, err error) {
	in.Initialize(uuid)
	return db.get(in)
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

	path = filepath.Join(db.oDir(in), s.filename(in))
	err = unmarshalJsonFile(path, in)
	out = in

	// we cache the object
	if s.mustCache() {
		db.cache.put(out)
	}
	return
}

func (db *DB) initialize(o Object) (err error) {
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
	return
}

func (db *DB) insertOrUpdate(s *Schema, o Object, commit bool) (err error) {

	// initialize object first
	if err = db.initialize(o); err != nil {
		return
	}

	if s.mustCache() {
		db.cache.put(o)
	}

	if err = s.index(o); err != nil {
		return
	}

	if s.asyncWritesEnabled() {
		// we don't write object to disk but store
		// it in a structure for later saving
		db.asyncw.put(o)
	} else {
		// writing the object to disk
		if err = db.writeObject(o); err != nil {
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
	s.unindex(o)
	path = filepath.Join(db.oDir(o), s.filename(o))
	if isFileAndExist(path) {
		return os.Remove(path)
	}
	return
}

func (db *DB) search(o Object, field, operator string, value interface{}, constrain []*indexedField) *Search {
	var s *Schema
	var f []*indexedField
	var err error

	if s, err = db.schema(o); err != nil {
		return &Search{db: db, err: err}
	}

	// transform search value before searching
	s.prepare(field, &value)

	if f, err = s.ObjectIndex.search(o, field, operator, value, constrain); err != nil {
		// if the field is not indexed we have to go through all the collection
		if errors.Is(err, ErrFieldNotIndexed) {
			return db.searchAll(o, field, operator, value, constrain)
		}
		return &Search{db: db, err: err}
	} else {
		return newSearch(db, o, f, err)
	}
}

func (db *DB) flush(o Object) (err error) {

	if e := db.writeObject(o); e != nil {
		err = e
	}

	// we delete object from the list of objects to save
	db.asyncw.delete(o)

	return
}

func (db *DB) flushAll(of Object) (err error) {

	key := db.asyncw.key(of)
	if om, ok := db.asyncw.m[key]; ok {
		return om.flush(db)
	}

	return
}

func (db *DB) flushDB() (err error) {
	return db.asyncw.flush(db)
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
	var es *Schema

	es, err = db.schema(o)

	switch {
	case err == nil:
		s.initialize(db, o)

		// the schema is existing and we don't need to build a new one
		// update existing schema with changes
		if err = es.update(&s); err != nil {
			return
		}

		return db.saveSchema(o, es, true)

	case errors.Is(err, fs.ErrNotExist):
		// we need to create a new schema
		if err = s.initialize(db, o); err != nil {
			return
		}

		if err = db.saveSchema(o, &s, false); err != nil {
			return
		}

		if err = s.control(); err != nil {
			return
		}

		db.schemas[stype(o)] = &s

	default:
		return
	}

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

// GetByUUID gets a single Object from the DB its UUID
func (db *DB) GetByUUID(in Object, uuid string) (out Object, err error) {
	db.RLock()
	defer db.RUnlock()
	return db.getByUUID(in, uuid)
}

func (db *DB) all(of Object) (out []Object, err error) {
	var o Object
	var it *iterator

	if it, err = db.Iterator(of); err != nil {
		return
	}

	out = make([]Object, 0, it.len())
	for o, err = it.next(); err == nil && err != ErrEOI; o, err = it.next() {
		out = append(out, o)
	}

	if err == ErrEOI {
		err = nil
	}

	return
}

// All returns all Objects in the DB
func (db *DB) All(of Object) (out []Object, err error) {
	db.RLock()
	defer db.RUnlock()

	return db.all(of)
}

// AssignAll assigns all Objects in the DB to target
func (db *DB) AssignAll(of Object, target interface{}) (err error) {
	db.RLock()
	defer db.RUnlock()

	var objs []Object

	if objs, err = db.all(of); err != nil {
		return
	}

	return Assign(objs, target)
}

// AssignIndex assign indexed fields to target. It prevents from fetching objects from disk
// if the only thing we actually want to query is some indexed fields. As indexes are
// all in memory this call is fast. The function panics if target is not a slice pointer
// or if indexed values cannot be assigned to target elements.
func (db *DB) AssignIndex(of Object, field string, target interface{}) (err error) {
	db.RLock()
	defer db.RUnlock()

	var s *Schema

	if s, err = db.schema(of); err != nil {
		return
	}

	return s.assignIndex(of, field, target)
}

func (db *DB) searchAll(o Object, field, operator string, value interface{}, constrain []*indexedField) *Search {
	var iter *iterator
	var err error
	var s *Schema
	var search *indexedField

	f := make([]*indexedField, 0)

	if search, err = searchField(value); err != nil {
		return &Search{db: db, err: err}
	}

	if s, err = db.schema(o); err != nil {
		return &Search{db: db, err: err}
	}

	// building up the iterator out of constrain
	if constrain != nil {
		uuids := make([]string, 0, len(constrain))
		for _, c := range constrain {
			uuids = append(uuids, s.ObjectIndex.ObjectIds[c.ObjectId])
		}
		iter = newIterator(db, o, uuids)
	} else if iter, err = db.Iterator(o); err != nil {
		return &Search{db: db, err: err}
	}

	// we go through the iterator
	fp := fieldPath(field)
	searchType := search.valueTypeString()

	for obj, err := iter.next(); err == nil && err != ErrEOI; obj, err = iter.next() {
		var test *indexedField
		var value interface{}
		var ok bool
		var index uint64

		if index, ok = s.ObjectIndex.uuids[obj.UUID()]; !ok {
			return &Search{db: db, err: ErrIndexCorrupted}
		}

		if value, ok = fieldByName(obj, fp); !ok {
			return &Search{db: db, err: fmt.Errorf("%w %s", ErrUnkownField, field)}
		}

		if test, err = newIndexedField(value, index); err != nil {
			return &Search{db: db, err: err}
		}

		fieldType := test.valueTypeString()

		if fieldType != searchType {
			return &Search{db: db, err: fmt.Errorf("%w, cannot cast %T(%v) to %s", ErrCasting, search.Value, search.Value, fieldType)}
		}

		if test.evaluate(operator, search) {
			f = append(f, test)
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
func (db *DB) Iterator(of Object) (it *iterator, err error) {
	db.RLock()
	defer db.RUnlock()

	var s *Schema
	var uuids []string

	if s, err = db.schema(of); err != nil {
		return
	}

	if s.ObjectIndex != nil {
		uuids = make([]string, 0, len(s.ObjectIndex.uuids))
		for uuid := range s.ObjectIndex.uuids {
			uuids = append(uuids, uuid)
		}
	} else {
		err = fmt.Errorf("%T %w", stype(of), ErrMissingObjIndex)
		return
	}

	// building up the iterator
	return newIterator(db, of, uuids), nil
}

// Count the number of Object in the database
func (db *DB) Count(of Object) (n int, err error) {
	var it *iterator

	if it, err = db.Iterator(of); err != nil {
		return
	}

	n = it.len()
	return
}

// Drop drops all the database
func (db *DB) Drop() (err error) {
	db.Lock()
	defer db.Unlock()

	return os.RemoveAll(db.root)
}

// DeleteAll deletes all Objects of the same type and commit changes
func (db *DB) DeleteAll(of Object) (err error) {
	var it *iterator
	if it, err = db.Iterator(of); err != nil {
		return
	}
	return db.DeleteObjects(it)
}

// DeleteObjects deletes Objects from an Iterator and commit changes.
// This primitive can be used for bulk deletions.
func (db *DB) DeleteObjects(from *iterator) (err error) {
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
// n returns the number of Objects successfully inserted.
func (db *DB) InsertOrUpdateBulk(in chan Object, csize int) (n int, err error) {
	var o Object
	var insn int

	chunk := make([]Object, 0, csize)
	for o = range in {
		chunk = append(chunk, o)
		if len(chunk) == csize {
			insn, err = db.InsertOrUpdateMany(chunk...)
			n += insn
			if err != nil {
				return
			}
			chunk = make([]Object, 0, csize)
		}
	}

	// we process last chunk
	insn, err = db.InsertOrUpdateMany(chunk...)
	n += insn

	return
}

// InsertOrUpdateMany inserts several objects into the DB and
// commit schema after all insertions. It is faster than calling
// InsertOrUpdate for every objects separately. All objects must
// be of the same type. This method is atomic, so all objects
// must satisfy constraints and be valid according to their Validate
// method. If this method fails no object is inserted.
func (db *DB) InsertOrUpdateMany(objects ...Object) (n int, err error) {
	db.Lock()
	defer db.Unlock()
	var schema *Schema

	if len(objects) == 0 {
		return
	}

	if schema, err = db.schema(objects[0]); err != nil {
		return
	}

	expType := stype(objects[0])
	// we make a temporary index to validate constraints accross
	// objects to be inserted, because some objects we want to insert
	// might be conflicting
	tmpIndex := schema.makeTmpIndex()

	// we validate all the objects prior to insertion
	for _, o := range objects {

		otype := stype(o)

		// we have to initialize object before being able to make constraint checking
		if err = db.initialize(o); err != nil {
			return
		}

		// testing if all objects in parameters are of the same type
		if otype != expType {
			err = fmt.Errorf("%w expecting %s, got %s", ErrWrongObjectType, expType, otype)
			return
		}

		// making transformations prior to validation
		// Object transform
		o.Transform()
		// schema transformation superseeds Object transformation
		schema.transform(o)
		// validate object before insertion
		if err = o.Validate(); err != nil {
			err = validationErr(o, err)
			return
		}

		// check that temporary index made of objects to insert
		// validates object's constraints
		if err = tmpIndex.insertOrUpdate(o); err != nil {
			err = fmt.Errorf("%w > %s", err, jsonOrPanic(o))
			return
		}

		// check that current objects' index validate object's constraints
		if err = schema.ObjectIndex.satisfyAll(o); err != nil {
			err = fmt.Errorf("%w > %s", err, jsonOrPanic(o))
			return
		}
	}

	// inserting objects
	for _, o := range objects {
		if e := db.insertOrUpdate(schema, o, false); e != nil {
			err = fmt.Errorf("%w > %s", e, jsonOrPanic(o))
			break
		}
		n++
	}

	if e := db.commit(objects[0]); e != nil {
		err = e
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
	var schema *Schema

	if schema, err = db.schema(o); err != nil {
		return
	}

	// making transformations prior to validation
	// Object transform
	o.Transform()
	// schema transformation superseeds Object transformation
	schema.transform(o)
	if err := o.Validate(); err != nil {
		return validationErr(o, err)
	}

	return db.insertOrUpdate(schema, o, true)
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

	return db.flush(o)
}

// Flush a single object to disk and commit changes
func (db *DB) FlushAndCommit(o Object) (last error) {
	db.Lock()
	defer db.Unlock()

	if err := db.commit(o); err != nil {
		last = err
	}

	if err := db.flush(o); err != nil {
		last = err
	}

	return
}

// FlushAll objects of type to disk. As Flush this function
// does not commit schema to disk
func (db *DB) FlushAll(of Object) (err error) {
	db.Lock()
	defer db.Unlock()

	return db.flushAll(of)
}

func (db *DB) flushAllAndCommit(of Object) (last error) {

	if err := db.flushAll(of); err != nil {
		last = err
	}

	if err := db.commit(of); err != nil {
		last = err
	}

	return
}

// FlushAllAndCommit flushes objects of type to disk and commit schema
func (db *DB) FlushAllAndCommit(of Object) (last error) {
	db.Lock()
	defer db.Unlock()

	return db.flushAllAndCommit(of)
}

// Repair repairs database schema
func (db *DB) Repair(of Object) (err error) {
	db.Lock()
	defer db.Unlock()

	var uuids map[string]bool
	var s *Schema
	var o Object

	dir := db.oDir(of)

	// we get schema
	if s, err = db.schema(of); err != nil && !errors.Is(err, ErrIndexCorrupted) {
		return
	}

	// we re-index missing objects in index
	if uuids, err = uuidsFromDir(dir); err != nil {
		return
	}

	// we re-index missing uuids
	for uuid := range uuids {
		// we don't re-index already indexed objects
		if s.isUUIDIndexed(uuid) {
			continue
		}

		if o, err = db.getByUUID(of, uuid); err != nil {
			return
		}

		if err = s.index(o); err != nil {
			return
		}
	}

	// we de-index missing objects
	for uuid := range s.ObjectIndex.uuids {
		if !uuids[uuid] {
			// if object is not on disk and is in index
			s.unindexByUUID(uuid)
		}
	}

	return nil
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
