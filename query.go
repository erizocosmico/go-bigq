package bigq

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/api/bigquery/v2"
)

type Config struct {
	DatasetID string
	ProjectID string
}

type Service struct {
	config  Config
	service *bigquery.Service
}

var errInvalidConfig = errors.New("dataset and project can not be empty")

func NewService(clientOptions ClientOptions, config Config) (*Service, error) {
	bqService, err := clientOptions.Service()
	if err != nil {
		return nil, err
	}

	if config.DatasetID == "" || config.ProjectID == "" {
		return nil, errInvalidConfig
	}

	return &Service{config, bqService}, nil
}

type Query struct {
	service   *bigquery.Service
	jobID     string
	projectID string
	pageToken string
	gotRows   uint64
}

func (s *Service) Query(query string, args ...uint64) (*Query, error) {
	var start, maxResults uint64
	switch len(args) {
	case 0:
	case 2:
		maxResults = args[1]
		fallthrough
	case 1:
		start = args[0]
	default:
		return nil, fmt.Errorf("too many arguments given to query: %d", len(args))
	}

	req := &bigquery.QueryRequest{
		DefaultDataset: &bigquery.DatasetReference{
			DatasetId: s.config.DatasetID,
			ProjectId: s.config.ProjectID,
		},
		Query: query,
	}

	if maxResults > 0 {
		req.MaxResults = int64(maxResults)
	}

	resp, err := s.service.Jobs.Query(s.config.ProjectID, req).Do()
	if err != nil {
		return nil, err
	}

	if err := s.waitForJob(resp.JobReference.JobId); err != nil {
		return nil, err
	}

	return &Query{
		jobID:     resp.JobReference.JobId,
		projectID: s.config.ProjectID,
		service:   s.service,
		gotRows:   start,
	}, nil
}

func (s *Service) waitForJob(jobID string) error {
	for {
		job, err := s.service.Jobs.Get(s.config.ProjectID, jobID).Do()
		if err != nil {
			return err
		}

		if job.Status.State == "DONE" {
			if job.Status.ErrorResult != nil {
				return errors.New(job.Status.ErrorResult.Message)
			}

			break
		}
		<-time.After(300 * time.Millisecond)
	}
	return nil
}

func (q *Query) GetNextPage() ([]*bigquery.TableRow, error) {
	call := q.service.Jobs.GetQueryResults(q.projectID, q.jobID)
	call.StartIndex(q.gotRows)
	if q.pageToken != "" {
		call.PageToken(q.pageToken)
	}

	results, err := call.Do()
	if err != nil {
		return nil, err
	}

	q.gotRows += uint64(len(results.Rows))
	if q.gotRows > results.TotalRows {
		return nil, nil
	}

	q.pageToken = results.PageToken
	return results.Rows, nil
}
