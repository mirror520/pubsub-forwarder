package router

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/endpoint"
)

type ReplayRequest struct {
	From  string    `json:"from"`
	To    string    `json:"to"`
	Topic string    `json:"topic"`
	Since time.Time `josn:"since"`
}

func ReplayEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(ReplayRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		return svc.Replay(req.Topic, req.Since, req.From, req.To)
	}
}

func CloseTaskEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		id, ok := request.(string)
		if !ok {
			return nil, errors.New("invalid request")
		}

		err := svc.CloseTask(id)
		return nil, err
	}
}
