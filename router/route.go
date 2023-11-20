package router

import (
	"errors"

	"go.uber.org/zap"

	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/pubsub"
)

type Route struct {
	id        string
	log       *zap.Logger
	profile   model.Route
	connector pubsub.PubSub
	endpoints []pubsub.PubSub

	subscribed map[string]struct{}
	binded     bool
}

func (r *Route) Bind() error {
	var err error
	for _, topic := range r.profile.Topics {
		_, ok := r.subscribed[topic]
		if ok {
			continue
		}

		e := r.connector.Subscribe(topic, r.Handler)
		if e != nil {
			err = errors.Join(err, e)
			continue
		}

		r.subscribed[topic] = struct{}{}
	}

	if err == nil {
		r.binded = true
	}

	return err
}

func (r *Route) Handler(topic string, payload []byte) {
	log := r.log.With(
		zap.String("action", "handler"),
		zap.String("topic", topic),
	)
	log.Debug("message arrived", zap.String("payload", string(payload)))

	for _, endpoint := range r.endpoints {
		log := log.With(zap.String("endpoint", endpoint.Name()))

		err := endpoint.Publish(topic, payload)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		log.Info("message published")
	}
}

func (r *Route) Binded() bool {
	return r.binded
}
