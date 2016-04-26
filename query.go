package bigq

import "google.golang.org/api/bigquery/v2"

type Query struct {
	service     *bigquery.Service
	jobID       string
	projectID   string
	pageToken   string
	sentRows    uint64
	initialRows []*bigquery.TableRow
}

func newQuery(
	service *bigquery.Service,
	resp *bigquery.QueryResponse,
	projectID string,
	start uint64,
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
	}
}

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
