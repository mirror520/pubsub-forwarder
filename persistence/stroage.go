package persistence

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/mirror520/events"
	"github.com/mirror520/pubsub-forwarder/interfaces"
	"github.com/mirror520/pubsub-forwarder/model"
)

var (
	ErrInvalidStorage = errors.New("invalid storage")
	ErrTimeout        = events.ErrTimeout
)

type Storage interface {
	interfaces.Endpoint

	Connected() bool
	Connect() error
	Close() error
	Store(topic string, payload json.RawMessage, ids ...ulid.ULID) error
	Iterator(topic string, since time.Time) (Iterator, error)
}

func NewStorage(cfg *model.Persistence) (Storage, error) {
	switch cfg.Driver {
	case events.InMem, events.BadgerDB, events.InfluxDB, events.MongoDB:
		return NewEventsStorage(cfg)

	default:
		return nil, errors.New("driver unsupported")
	}
}

type Iterator interface {
	events.Iterator
}
