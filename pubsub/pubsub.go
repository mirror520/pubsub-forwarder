package pubsub

import (
	"errors"

	"github.com/mirror520/pubsub-forwarder/model"
)

// topic wildcards:
// * (star) can substitute for exactly one word.
// # (hash) can substitute for zero or more words.

type PubSub interface {
	Name() string
	Connect() error
	Close() error
	Publish(topic string, payload []byte) error
	Subscribe(topic string, callback MessageHandler) error
}

func NewPubSub(cfg model.Transport) (PubSub, error) {
	switch cfg.Protocol {
	case model.MQTT:
		return NewMQTTPubSub(cfg)

	default:
		return nil, errors.New("protocol not supported")
	}
}

type MessageHandler func(topic string, payload []byte)

type SubscribedTopic struct {
	Topic    string
	Callback MessageHandler
}
