package bigq

import (
	"fmt"
	"reflect"
)

// Iter is a structure to loop through query results.
type Iter interface {
	// Next fetches the next row and fills the fields of the given
	// struct pointer with the columns of the row in appearance order.
	// For example, given:
	//  struct {
	//          Foo int
	//          Bar int
	//  }
	//
	// The row 1, 2 would result in struct{Foo: 1, Bar:2}
	// This method returns a boolean reporting if the operation was
	// successful.
	Next(interface{}) bool

	// Err returns the latest error that happened.
	Err() error
}

type iter struct {
	q    Query
	rows [][]interface{}
	idx  int
	err  error
}

// Next fetches the next row and fills the fields of the given
// struct pointer with the columns of the row in appearance order.
// For example, given:
//  struct {
//          Foo int
//          Bar int
//  }
//
// The row 1, 2 would result in struct{Foo: 1, Bar:2}
// This method returns a boolean reporting if the operation was
// successful.
func (i *iter) Next(dst interface{}) bool {
	if i.idx >= len(i.rows) || len(i.rows) == 0 {
		if err := i.requestNextPage(); err != nil {
			i.err = err
			return false
		}

		if len(i.rows) == 0 {
			return false
		}
	}

	if err := i.scan(dst); err != nil {
		i.err = err
		return false
	}

	i.idx++
	return true
}

func (i *iter) requestNextPage() error {
	rows, err := i.q.(*query).nextPage()
	if err != nil {
		return err
	}

	i.rows = rows
	i.idx = 0
	return nil
}

func (i *iter) scan(dst interface{}) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("value of type %s is not a pointer", v.Type())
	}

	v = v.Elem()
	t := v.Type()
	row := i.rows[i.idx]

	var ignored int
	for i := 0; i < t.NumField() && i < len(row); i++ {
		field := t.Field(i)
		f := v.FieldByName(field.Name)
		if !f.CanSet() {
			ignored++
			continue
		}

		cell := reflect.ValueOf(row[i-ignored])
		if !cell.Type().AssignableTo(f.Type()) {
			return fmt.Errorf("value of type %q is not assignable to field %q of type %q", cell.Type(), field.Name, f.Type())
		}
		f.Set(cell)
	}

	return nil
}

// Err returns the latest error that happened.
func (i *iter) Err() error {
	return i.err
}
