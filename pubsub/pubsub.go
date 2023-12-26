package pubsub

import (
	"encoding/json"
	"errors"

	"github.com/oklog/ulid/v2"

	"github.com/mirror520/pubsub-forwarder/interfaces"
	"github.com/mirror520/pubsub-forwarder/model"
)

var (
	ErrInvalidClient       = errors.New("invalid client")
	ErrClientDisconnected  = errors.New("client disconnected")
	ErrProtocolUnsupported = errors.New("protocol unsupported")
)

// topic wildcards:
// * (star) can substitute for exactly one word.
// # (hash) can substitute for zero or more words.

type PubSub interface {
	interfaces.Endpoint

	Connected() bool
	Connect() error
	Close() error
	Publish(topic string, payload json.RawMessage, ids ...ulid.ULID) error
	Subscribe(topic string, callback MessageHandler) error
	Unsubscribe(topic ...string) error
}

func NewPubSub(cfg *model.Transport) (PubSub, error) {
	switch cfg.Protocol {
	case model.MQTT:
		return NewMQTTPubSub(cfg)

	case model.NATS:
		return NewNATSPubSub(cfg)

	default:
		return nil, errors.New("protocol unsupported")
	}
}

type (
	MessageHandler    func(topic string, payload json.RawMessage, id ulid.ULID)
	MessageMiddleware func(next MessageHandler) MessageHandler
)
