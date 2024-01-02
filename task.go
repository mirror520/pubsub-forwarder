package router

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/mirror520/pubsub-forwarder/interfaces"
	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/persistence"
)

type Task interface {
	ID() string
	Start()
	Close() error
}

func NewReplayTask(ctx context.Context, it persistence.Iterator, endpoints []interfaces.Endpoint) (Task, error) {
	log, ok := ctx.Value(model.LOGGER).(*zap.Logger)
	if !ok {
		return nil, errors.New("logger not found")
	}

	id := "replay-" + ulid.Make().String()

	ctx, cancel := context.WithCancel(ctx)
	return &replayTask{
		log: log.With(
			zap.String("task", "replay"),
			zap.String("id", id),
		),
		id:        id,
		it:        it,
		endpoints: endpoints,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

type replayTask struct {
	log       *zap.Logger
	id        string
	it        persistence.Iterator
	endpoints []interfaces.Endpoint
	ctx       context.Context
	cancel    context.CancelFunc
}

func (task *replayTask) ID() string {
	return task.id
}

func (task *replayTask) Start() {
	task.log.Info("start replaying")

	go task.replaying(task.ctx)
}

func (task *replayTask) replaying(ctx context.Context) {
	log := task.log.With(zap.String("action", "replaying"))

	for {
		select {
		case <-ctx.Done():
			log.Info("done")
			return

		default:
			events, err := task.it.Fetch(20)
			if err != nil {
				if !errors.Is(err, persistence.ErrTimeout) {
					log.Error(err.Error())
				}
				continue
			}

			log.Debug("event fetched", zap.Int("size", len(events)))

			for _, e := range events {
				log := task.log.With(
					zap.String("topic", e.Topic),
				)

				bs, err := json.Marshal(e.Payload)
				if err != nil {
					log.Error(err.Error())
					continue
				}

				for _, endpoint := range task.endpoints {
					err := endpoint.Handle(e.Topic, bs, e.ID)
					if err != nil {
						log.Error(err.Error(), zap.String("to", endpoint.Name()))
					}
				}
			}
		}
	}
}

func (task *replayTask) Close() error {
	if task.cancel != nil {
		task.cancel()
	}

	task.cancel = nil
	return nil
}
