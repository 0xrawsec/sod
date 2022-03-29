package sod

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

var (
	timeType = reflect.TypeOf(time.Time{})

	ErrFieldDescModif = errors.New("field descriptor changed")
)

type FieldDescriptor struct {
	Path        string      `json:"path"`
	Type        string      `json:"type"`
	Constraints Constraints `json:"constraints"`
}

func (d *FieldDescriptor) Transform(o interface{}) {
	switch i := o.(type) {
	case Object:
		d.Constraints.TransformField(d.Path, i)
	default:
		d.Constraints.Transform(i)
	}
}

func (d *FieldDescriptor) FieldEqual(other *FieldDescriptor) bool {
	return d.Path == other.Path && d.Type == other.Type

}

func (d *FieldDescriptor) DeepEqual(other *FieldDescriptor) bool {
	return reflect.DeepEqual(d, other)
}

func (d FieldDescriptor) String() string {
	return fmt.Sprintf("path=%s type=%s constraints=(%s)", d.Path, d.Type, d.Constraints)
}

type FieldDescMap map[string]FieldDescriptor

func FieldDescriptors(from Object) (desc FieldDescMap) {
	desc = make(FieldDescMap)
	sdesc := make([]FieldDescriptor, 0)
	recFieldDescriptors(reflect.ValueOf(from), "", &sdesc)
	for _, fd := range sdesc {
		desc[fd.Path] = fd
	}
	return
}

func (m FieldDescMap) CompatibleWith(target FieldDescMap) (err error) {

	for p, fd := range m {
		if ofd, ok := target[p]; !ok {
			return fmt.Errorf("target %w %s", ErrUnkownField, p)
		} else if !fd.DeepEqual(&ofd) {
			return fmt.Errorf("%w %s", ErrFieldDescModif, ofd)
		}
	}

	for p, ofd := range target {
		if fd, ok := m[p]; !ok {
			return fmt.Errorf("source %w %s", ErrUnkownField, p)
		} else if !ofd.DeepEqual(&fd) {
			return fmt.Errorf("%w %s", ErrFieldDescModif, fd)
		}
	}

	return
}

func (m FieldDescMap) FieldsCompatibleWith(target FieldDescMap) (err error) {

	for p, fd := range m {
		if ofd, ok := target[p]; !ok {
			return fmt.Errorf("target %w %s", ErrUnkownField, p)
		} else if !fd.FieldEqual(&ofd) {
			return fmt.Errorf("%w %s", ErrFieldDescModif, ofd)
		}
	}

	for p, ofd := range target {
		if fd, ok := m[p]; !ok {
			return fmt.Errorf("source %w %s", ErrUnkownField, p)
		} else if !ofd.FieldEqual(&fd) {
			return fmt.Errorf("%w %s", ErrFieldDescModif, fd)
		}
	}

	return
}

func (m FieldDescMap) Transformers() (t []FieldDescriptor) {
	t = make([]FieldDescriptor, 0)
	for _, fd := range m {
		if fd.Constraints.Transformer() {
			t = append(t, fd)
		}
	}
	return
}

func (m FieldDescMap) GetDescriptor(fpath string) (d FieldDescriptor, ok bool) {
	d, ok = m[fpath]
	return
}

func (m FieldDescMap) Constraint(fpath string, c Constraints) (err error) {
	if d, ok := m[fpath]; ok {
		d.Constraints = c
		m[fpath] = d
		return nil
	}
	return fmt.Errorf("%w %s", ErrUnkownField, fpath)
}

func fdFromType(path string, tag string, fieldType reflect.Type) FieldDescriptor {
	fd := FieldDescriptor{
		Path: path,
		Type: fieldType.String(),
	}

	for _, tv := range strings.Split(tag, ",") {
		switch tv {
		case "index":
			fd.Constraints.Index = true
		case "unique":
			fd.Constraints.Index = true
			fd.Constraints.Unique = true
		case "lower":
			fd.Constraints.Lower = true
		case "upper":
			fd.Constraints.Upper = true
		}
	}

	return fd

}

func joinFieldPath(path, fieldName string) string {
	if path == "" {
		return fieldName
	}
	return strings.Join([]string{path, fieldName}, ".")

}

func recFieldDescriptors(v reflect.Value, path string, fds *[]FieldDescriptor) {
	typ := v.Type()

	switch v.Kind() {
	default:
		*fds = append(*fds, fdFromType(path, "", typ))

	case reflect.Ptr:
		if v.Elem().Kind() == reflect.Struct {
			recFieldDescriptors(v.Elem(), path, fds)
		} else {
			*fds = append(*fds, fdFromType(path, "", typ))
		}

	case reflect.Struct:

		for i := 0; i < v.NumField(); i++ {
			fieldValue := v.Field(i)
			structField := typ.Field(i)

			switch fieldValue.Kind() {
			case reflect.Ptr:
				// create a new field
				fieldValue = reflect.New(structField.Type.Elem())
				recFieldDescriptors(fieldValue, joinFieldPath(path, structField.Name), fds)
				continue
			case reflect.Struct:
				// don't treat struct time.Time as a struct
				if !fieldValue.Type().AssignableTo(timeType) {
					recFieldDescriptors(fieldValue, joinFieldPath(path, structField.Name), fds)
					continue
				}
			}

			// process struct field
			tag, _ := structField.Tag.Lookup("sod")
			fdPath := structField.Name
			if !structField.IsExported() {
				continue
			}
			if path != "" {
				fdPath = fmt.Sprintf("%s.%s", path, fdPath)
			}

			*fds = append(*fds, fdFromType(fdPath, tag, fieldValue.Type()))
		}
	}
}
