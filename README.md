# go-bigq [![Build Status](https://travis-ci.org/mvader/go-bigq.svg?branch=master)](https://travis-ci.org/mvader/go-bigq) [![GoDoc](https://godoc.org/github.com/mvader/go-bigq?status.svg)](http://godoc.org/github.com/mvader/go-bigq)
go-bigq is a wrapper to make querying BigQuery easier for the Go programming language.

Even though there is an official package to interact with BigQuery the API of that library is sort of arcane, so I ended up making a layer on top of it to make the experience of querying BigQuery more pleasant.

This package **only** performs reads, if you are looking for a way to write to BigQuery I recommend you [go-bqstreamer](https://github.com/rounds/go-bqstreamer).

## Usage

```go
// Create the service
service, err := bigq.New(bigq.WithConfigFile("/path/to/token.json"), bigq.Config{
	ProjectID: "my-project",
	DatasetID: "my-dataset",
})
handleErr(err)

// Perform the query starting at 0 index and with 100 results per page
q, err := service.Query("SELECT foo FROM bar WHERE baz", 0, 100)
handleErr(err)

// Get the next page of results
rows, err := q.NextPage()
handleErr(err)
doSomethingWith(rows)
```

## TODO

* `Service` method to perform queries with large results (queries whose results have to go to another table).
* Think of a nicer way to return the pages, rather than `[][]interface{}`.
* `Next` iterator-ish method to retrieve the results and fill a struct with them.
