package bigq

import (
	"io/ioutil"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
)

type ClientOptions interface {
	Service() (*bigquery.Service, error)
}

func WithConfigFile(path string) ClientOptions {
	return &tokenFileOptions{path: path}
}

const bigqueryAuth = "https://www.googleapis.com/auth/bigquery"

type tokenFileOptions struct {
	path string
}

func (o *tokenFileOptions) Service() (*bigquery.Service, error) {
	data, err := ioutil.ReadFile(o.path)
	if err != nil {
		return nil, err
	}

	conf, err := google.JWTConfigFromJSON(data, bigqueryAuth)
	if err != nil {
		return nil, err
	}

	return bigquery.New(conf.Client(oauth2.NoContext))
}

func WithJWTConfig(config *jwt.Config) ClientOptions {
	return &jwtTokenOptions{config: config}
}

type jwtTokenOptions struct {
	config *jwt.Config
}

func (o *jwtTokenOptions) Service() (*bigquery.Service, error) {
	return bigquery.New(o.config.Client(oauth2.NoContext))
}
