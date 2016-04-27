package bigq

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

var (
	tokenDownloadURL = os.Getenv("TOKEN_DOWNLOAD_URL")
	tokenFile        string
)

func downloadToken() (string, error) {
	resp, err := http.Get(tokenDownloadURL)
	if err != nil {
		return "", err
	}

	f, err := ioutil.TempFile("", "bigq_tok_")
	if err != nil {
		return "", err
	}

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

func init() {
	path, err := downloadToken()
	if err != nil {
		panic(err)
	}

	tokenFile = path
}
