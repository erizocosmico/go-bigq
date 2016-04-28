package bigq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextPage(t *testing.T) {
	assert := assert.New(t)
	service, err := New(WithConfigFile(tokenFile), Config{
		ProjectID: "go-bigq",
		DatasetID: "samples",
	})
	assert.Nil(err)

	q, err := service.Query(testQuery, 0, 5)
	assert.Nil(err)
	assert.NotNil(q)

	expected := [][]string{
		[]string{"zwaggered", "zounds", "zone", "zodiacs", "zodiac"},
		[]string{"zo", "zir", "zephyrs", "zenith", "zed"},
		[]string{"zeals", "zealous", "zealous", "zealous", "zealous"},
		[]string{"zealous", "zeal", "zeal", "zeal", "zeal"},
	}

	for i := 0; true; i++ {
		rows, err := q.NextPage()
		if len(rows) == 0 {
			break
		}

		assert.Nil(err)
		assert.Equal(len(rows), 5)

		for j, r := range rows {
			assert.Equal(r[0], expected[i][j])
		}
	}
}
