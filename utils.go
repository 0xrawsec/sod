package sod

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"

	"github.com/google/uuid"
)

func AssignOne(o Object, target interface{}) {
	v := reflect.ValueOf(target)
	if v.Kind() == reflect.Ptr && !v.IsZero() {
		v = v.Elem()
		if _, ok := v.Interface().(Object); ok {
			ov := reflect.ValueOf(o)
			v.Set(ov)
			return
		}
	}
	panic("target type must be a *sod.Object")
}

func Assign(objs []Object, target interface{}) (err error) {
	v := reflect.ValueOf(target)
	if v.Kind() == reflect.Ptr && !v.IsZero() {
		v = v.Elem()
		t := reflect.TypeOf(target)
		if v.Kind() == reflect.Slice {
			// making a new slice for value pointed by target
			v.Set(reflect.MakeSlice(t.Elem(), len(objs), len(objs)))
			for i := 0; i < len(objs); i++ {
				ov := reflect.ValueOf(objs[i])
				if _, ok := ov.Interface().(Object); ok {
					v.Index(i).Set(ov)
					continue
				}
				goto freakout
			}
			return
		}
	}
freakout:
	panic("target type must be *[]sod.Object")
}

// ToObjectSlice is a convenient function to pre-process arguments passed
// to InsertOrUpdateMany function.
func ToObjectSlice(slice interface{}) (objs []Object) {
	v := reflect.ValueOf(slice)
	if v.Kind() == reflect.Slice {
		objs = make([]Object, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			objs = append(objs, v.Index(i).Interface().(Object))
		}
	} else {
		objs = make([]Object, 0)
	}
	return
}

// ToObjectChan is a convenient function to pre-process arguments passed
// to InsertOrUpdateMany function.
func ToObjectChan(slice interface{}) (objs chan Object) {
	v := reflect.ValueOf(slice)
	objs = make(chan Object)
	if v.Kind() == reflect.Slice {
		go func() {
			defer close(objs)
			for i := 0; i < v.Len(); i++ {
				objs <- v.Index(i).Interface().(Object)
			}
		}()
	} else {
		close(objs)
	}
	return
}

func uuidOrPanic() string {
	if u, err := uuid.NewRandom(); err != nil {
		panic(err)
	} else {
		return u.String()
	}

}

func camelToSnake(camel string) string {
	var snake bytes.Buffer
	var prevLower bool
	var cur, next rune

	for i := range camel {
		var nextLower bool
		cur = rune(camel[i])
		isDigit := ('0' <= cur && cur <= '9')

		// check if next char is lower case
		if i < len(camel)-1 {
			next = rune(camel[i+1])
			if 'a' <= next && next <= 'z' {
				nextLower = true
			}
		}

		// if it is upper case or a digit
		if ('A' <= cur && cur <= 'Z') || isDigit {
			// just convert [A-Z] to _[a-z]
			if snake.Len() > 0 && (nextLower || prevLower) {
				snake.WriteRune('_')
			}

			if isDigit {
				// don't convert digit
				snake.WriteRune(cur)
			} else {
				// convert upper to lower
				snake.WriteRune(cur - 'A' + 'a')
			}

			prevLower = false
		} else {
			snake.WriteRune(cur)
			prevLower = true
		}
	}

	return snake.String()
}

func stype(i interface{}) string {
	return typeof(i).String()
}

func typeof(i interface{}) reflect.Type {
	if t := reflect.TypeOf(i); t.Kind() == reflect.Ptr {
		return t.Elem()
	} else {
		return t
	}
}

func uuidExt(name string) (uuid, ext string) {
	s := strings.SplitN(name, ".", 2)
	uuid = s[0]
	ext = fmt.Sprintf(".%s", s[1])
	return
}

func uuidsFromDir(dir string) (uuids map[string]bool, err error) {
	var entries []os.DirEntry

	// we read directory where objects are stored
	if entries, err = os.ReadDir(dir); err != nil {
		return
	}

	// we re-index missing objects in index
	uuids = make(map[string]bool)
	for _, entry := range entries {
		uuid, _ := uuidExt(entry.Name())

		if !uuidRegexp.MatchString(uuid) {
			continue
		}
		uuids[uuid] = true
	}

	return
}

func isFileAndExist(path string) bool {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return stat.Mode().IsRegular() && err == nil
}

func isDirAndExist(path string) bool {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return stat.Mode().IsDir() && err == nil
}

func dbgLock(lock string) {
	if pc, _, _, ok := runtime.Caller(2); ok {
		fn := runtime.FuncForPC(pc).Name()
		fmt.Fprintf(os.Stderr, "%s: %s\n", lock, fn)
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", lock)
	}
}

func fieldPath(path string) []string {
	return strings.Split(path, ".")
}

func unmarshalJsonFile(path string, i interface{}) (err error) {
	var data []byte
	var in *os.File
	var r io.Reader

	if in, err = os.Open(path); err != nil {
		return
	}
	defer in.Close()

	r = in
	if strings.HasSuffix(path, compressedExtension) {
		if r, err = gzip.NewReader(in); err != nil {
			return
		}
	}

	if data, err = ioutil.ReadAll(r); err != nil {
		return
	}

	if err = json.Unmarshal(data, i); err != nil {
		return
	}

	return
}

func writeReader(path string, r io.Reader, perms fs.FileMode, compress bool) (err error) {
	var out *os.File
	var w io.WriteCloser

	if compress && !strings.HasSuffix(path, compressedExtension) {
		path = fmt.Sprintf("%s%s", path, compressedExtension)
	}

	if out, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, perms); err != nil {
		return
	}
	defer out.Close()

	// default value for writer
	w = out
	if compress {
		if w, err = gzip.NewWriterLevel(out, gzip.BestSpeed); err != nil {
			return
		}
		defer w.Close()
	}

	if _, err = io.Copy(w, r); err != nil {
		return
	}

	return w.Close()

}
