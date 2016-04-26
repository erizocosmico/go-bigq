package bigq

import (
	"encoding/json"
	"io/ioutil"

	"golang.org/x/oauth2/jwt"
)

const bigqueryScope = "https://www.googleapis.com/auth/bigquery"

func NewJWTConfig(keyPath string) (*jwt.Config, error) {
	bytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	var token struct {
		PrivateKey string `json:"private_key"`
		Email      string `json:"client_email"`
	}
	if err := json.Unmarshal(bytes, &token); err != nil {
		return nil, err
	}

	return &jwt.Config{
		Email:      token.Email,
		PrivateKey: []byte(token.PrivateKey),
		Subject:    "",
		Scopes:     []string{bigqueryScope},
		TokenURL:   "https://accounts.google.com/o/oauth2/token",
		Expires:    0,
	}, nil
}
