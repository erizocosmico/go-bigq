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

func TestNext(t *testing.T) {
	expected := []string{
		"zwaggered", "zounds", "zone", "zodiacs", "zodiac",
		"zo", "zir", "zephyrs", "zenith", "zed",
		"zeals", "zealous", "zealous", "zealous", "zealous",
		"zealous", "zeal", "zeal", "zeal", "zeal",
	}

	assert := assert.New(t)
	service, err := New(WithConfigFile(tokenFile), Config{
		ProjectID: "go-bigq",
		DatasetID: "samples",
	})
	assert.Nil(err)

	q, err := service.Query(testQuery, 0, 5)
	assert.Nil(err)
	assert.NotNil(q)

	it := q.Iter()
	var word Word
	var i int
	for it.Next(&word) {
		assert.Equal(word.Word, expected[i])
		i++
	}
	assert.Nil(it.Err())
}

type Word struct {
	Word string
}

type Row struct {
	Num    int
	Float  float64
	String string
	Bool   bool
}

func iterWithRow() *iter {
	return &iter{
		rows: [][]interface{}{
			[]interface{}{1, 3.45, "hi", true},
		},
	}
}
