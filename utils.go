package sod

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"

	"github.com/google/uuid"
)

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

func UnmarshalJsonFile(path string, i interface{}) (err error) {
	var data []byte

	if data, err = ioutil.ReadFile(path); err != nil {
		return
	}
	if err = json.Unmarshal(data, i); err != nil {
		return
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

func stype(i interface{}) string {
	return typeof(i).Name()
}

func typeof(i interface{}) reflect.Type {
	if t := reflect.TypeOf(i); t.Kind() == reflect.Ptr {
		return t.Elem()
	} else {
		return t
	}
}

func filename(o Object, s *Schema) string {
	return fmt.Sprintf("%s%s", o.UUID(), s.Extension)
}

func uuidExt(name string) (uuid, ext string) {
	s := strings.SplitN(name, ".", 2)
	uuid = s[0]
	ext = fmt.Sprintf(".%s", s[1])
	return
}

func isFileAndExist(path string) bool {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return stat.Mode().IsRegular() && err == nil
}

func dbgLock(lock string) {
	if pc, _, _, ok := runtime.Caller(2); ok {
		fn := runtime.FuncForPC(pc).Name()
		fmt.Fprintf(os.Stderr, "%s: %s\n", lock, fn)
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", lock)
	}
}
