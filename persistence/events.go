package persistence

import (
	"encoding/json"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/mirror520/events"
	"github.com/mirror520/events/persistence"
	"github.com/mirror520/pubsub-forwarder/model"
)

func NewEventsStorage(cfg *model.Persistence) (Storage, error) {
	log := zap.L().With(
		zap.String("persistence", "events"),
		zap.String("driver", string(cfg.Driver)),
		zap.String("name", cfg.Name),
	)

	return &eventsStorage{
		log: log,
		cfg: cfg,
	}, nil
}

type eventsStorage struct {
	log *zap.Logger
	cfg *model.Persistence

	repo events.Repository
	svc  events.Service
}

func (s *eventsStorage) Name() string {
	return s.cfg.Name
}

func (s *eventsStorage) Handle(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	return s.Store(topic, payload, ids...)
}

func (s *eventsStorage) Connected() bool {
	return s.svc != nil
}

func (s *eventsStorage) Connect() error {
	if s.svc != nil {
		return nil
	}

	repo, err := persistence.NewEventRepository(s.cfg.Persistence)
	if err != nil {
		return err
	}

	svc := events.NewService(repo)
	svc = events.LoggingMiddleware(s.log)(svc)
	svc.Up()

	s.repo = repo
	s.svc = svc

	return nil
}

func (s *eventsStorage) Service() (events.Service, error) {
	if s.svc == nil {
		return nil, ErrInvalidStorage
	}

	return s.svc, nil
}

func (s *eventsStorage) Close() error {
	svc, err := s.Service()
	if err != nil {
		return err
	}

	svc.Down()
	return s.repo.Close()
}

func (s *eventsStorage) Store(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	svc, err := s.Service()
	if err != nil {
		return err
	}

	return svc.Store(topic, payload, ids...)
}

func (s *eventsStorage) Iterator(topic string, since time.Time) (Iterator, error) {
	svc, err := s.Service()
	if err != nil {
		return nil, err
	}

	id, err := svc.NewIterator(topic, since)
	if err != nil {
		return nil, err
	}

	return svc.Iterator(id)
}
