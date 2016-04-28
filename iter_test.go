package bigq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScan(t *testing.T) {
	assert := assert.New(t)
	iter := iterWithRow()
	var row Row
	assert.Nil(iter.scan(&row))
	assert.Equal(row.Num, 1)
	assert.Equal(row.Float, 3.45)
	assert.Equal(row.String, "hi")
	assert.Equal(row.Bool, true)
}

type Row struct {
	Num    int
	Float  float64
	String string
	Bool   bool
}

func iterWithRow() *Iter {
	return &Iter{
		rows: [][]interface{}{
			[]interface{}{1, 3.45, "hi", true},
		},
	}
}
