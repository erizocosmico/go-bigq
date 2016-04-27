package bigq

import (
	"errors"

	"google.golang.org/api/bigquery/v2"
)

// Query contains all the context of a query execution and has methods to
// retrieve the rows in pages. The Query instance can be seen as a cursor,
// it can't go back and it can't re-read the same page again. It is not thread
// safe and it is not intended to be.
type Query struct {
	service     *bigquery.Service
	jobID       string
	projectID   string
	pageToken   string
	sentRows    uint64
	maxResults  uint64
	initialRows []*bigquery.TableRow
}

func newQuery(
	service *bigquery.Service,
	resp *bigquery.QueryResponse,
	projectID string,
	start uint64,
	maxResults uint64,
) *Query {
	var rows []*bigquery.TableRow
	if resp.JobComplete && start == 0 {
		rows = resp.Rows
	}

	return &Query{
		jobID:       resp.JobReference.JobId,
		projectID:   projectID,
		service:     service,
		sentRows:    start,
		initialRows: rows,
		maxResults:  maxResults,
	}
}

var errAlreadyReading = errors.New("can't use NextPage after calling All")

// NextPage returns the next page of rows in the query resultset. It returns up
// to the max results that were given to the query on its creation. Note that
// BigQuery has a limit of 10MB, that is, when your page reaches the 10MB limit
// it will yield and will not return the max number of results instead.
func (q *Query) NextPage() ([][]interface{}, error) {
	if q.sentRows == 0 && len(q.initialRows) > 0 {
		q.sentRows += uint64(len(q.initialRows))
		return transformRows(q.initialRows), nil
	}

	if len(q.initialRows) > 0 {
		// no need to hold the reference anymore
		q.initialRows = nil
	}

	call := q.service.Jobs.GetQueryResults(q.projectID, q.jobID)
	call.StartIndex(q.sentRows)

	if q.maxResults > 0 {
		call.MaxResults(int64(q.maxResults))
	}

	if q.pageToken != "" {
		call.PageToken(q.pageToken)
	}

	results, err := call.Do()
	if err != nil {
		return nil, err
	}

	q.sentRows += uint64(len(results.Rows))
	if q.sentRows > results.TotalRows {
		return nil, nil
	}

	q.pageToken = results.PageToken
	return transformRows(results.Rows), nil
}

func transformRows(rows []*bigquery.TableRow) [][]interface{} {
	var result [][]interface{}
	for _, r := range rows {
		result = append(result, transformRow(r.F))
	}
	return result
}

func transformRow(row []*bigquery.TableCell) []interface{} {
	var result []interface{}
	for _, c := range row {
		result = append(result, c.V)
	}
	return result
}
