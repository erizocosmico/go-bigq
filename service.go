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

func New(clientOptions ClientOptions, config Config) (*Service, error) {
	bqService, err := clientOptions.Service()
	if err != nil {
		return nil, err
	}

	if config.DatasetID == "" || config.ProjectID == "" {
		return nil, errInvalidConfig
	}

	return &Service{config, bqService}, nil
}

func (s *Service) Query(query string, args ...uint64) (*Query, error) {
	start, maxResults, err := queryArgs(args...)
	if err != nil {
		return nil, err
	}

	resp, err := s.requestQuery(query, maxResults)
	if err != nil {
		return nil, err
	}

	if !resp.JobComplete {
		if err := s.waitForJob(resp.JobReference.JobId); err != nil {
			return nil, err
		}
	}

	return newQuery(s.service, resp, s.config.ProjectID, start), nil
}

func (s *Service) requestQuery(query string, maxResults uint64) (*bigquery.QueryResponse, error) {
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

	return s.service.Jobs.Query(s.config.ProjectID, req).Do()
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

func queryArgs(args ...uint64) (uint64, uint64, error) {
	var start, maxResults uint64
	switch len(args) {
	case 0:
	case 2:
		maxResults = args[1]
		fallthrough
	case 1:
		start = args[0]
	default:
		return 0, 0, fmt.Errorf("too many arguments given to query: %d", len(args))
	}
	return start, maxResults, nil
}
