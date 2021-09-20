package sod

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/google/uuid"
)

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
