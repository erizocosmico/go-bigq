package bigq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceNew(t *testing.T) {
	assert := assert.New(t)
	service, err := New(WithConfigFile(tokenFile), Config{
		ProjectID: "go-bigq",
		DatasetID: "samples",
	})
	assert.Nil(err)
	assert.NotNil(service)
}

const testQuery = `SELECT word
FROM [publicdata:samples.shakespeare]
ORDER BY word DESC
LIMIT 20`

func TestServiceQuery(t *testing.T) {
	assert := assert.New(t)
	service, err := New(WithConfigFile(tokenFile), Config{
		ProjectID: "go-bigq",
		DatasetID: "samples",
	})
	assert.Nil(err)

	q, err := service.Query(testQuery, 0, 5)
	assert.Nil(err)
	assert.NotNil(q)
}
