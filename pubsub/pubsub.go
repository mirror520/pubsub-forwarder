package pubsub

import (
	"errors"

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
	Name() string
	Connected() bool
	Connect() error
	Close() error
	Publish(topic string, payload []byte) error
	Subscribe(topic string, callback MessageHandler) error
}

func NewPubSub(cfg model.Transport) (PubSub, error) {
	switch cfg.Protocol {
	case model.MQTT:
		return NewMQTTPubSub(cfg)

	case model.NATS:
		return NewNATSPubSub(cfg)

	default:
		return nil, errors.New("protocol unsupported")
	}
}

type MessageHandler func(topic string, payload []byte)

type SubscribedTopic struct {
	Topic    string
	Callback MessageHandler
}
