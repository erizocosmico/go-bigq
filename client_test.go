package bigq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenFileOptions(t *testing.T) {
	assert := assert.New(t)
	service, err := WithConfigFile(tokenFile).Service()
	assert.Nil(err)
	assert.NotNil(service)
}

func TestJWTTokenOptions(t *testing.T) {
	assert := assert.New(t)
	conf, err := NewJWTConfig(tokenFile)
	assert.Nil(err)
	assert.NotNil(conf)
	service, err := WithJWTConfig(conf).Service()
	assert.Nil(err)
	assert.NotNil(service)
}
