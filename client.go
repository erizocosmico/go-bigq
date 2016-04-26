package bigq

import (
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
)

// ClientOptions will construct the client service based
// on the options it was passed.
type ClientOptions interface {
	// Service returns the client service
	Service() (*bigquery.Service, error)
}

// WithConfigFile returns a ClientOptions that will construct the client service
// using a config file.
func WithConfigFile(path string) ClientOptions {
	return &tokenFileOptions{path: path}
}

type tokenFileOptions struct {
	path string
}

func (o *tokenFileOptions) Service() (*bigquery.Service, error) {
	conf, err := NewJWTConfig(o.path)
	if err != nil {
		return nil, err
	}

	return bigquery.New(conf.Client(oauth2.NoContext))
}

// WithJWTConfig returns a ClientOptions that will construct the client service
// using the given jwt config.
func WithJWTConfig(config *jwt.Config) ClientOptions {
	return &jwtTokenOptions{config: config}
}

type jwtTokenOptions struct {
	config *jwt.Config
}

func (o *jwtTokenOptions) Service() (*bigquery.Service, error) {
	return bigquery.New(o.config.Client(oauth2.NoContext))
}
