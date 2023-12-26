package router

import (
	"encoding/json"
	"errors"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/mirror520/pubsub-forwarder/interfaces"
	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/pubsub"
)

type Route struct {
	id        string
	log       *zap.Logger
	profile   *model.Route
	connector pubsub.PubSub
	endpoints []interfaces.Endpoint

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

		handler := r.Handler
		handler = MinifyMiddleware(JSON)(handler)

		e := r.connector.Subscribe(topic, handler)
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

func (r *Route) Unbind() error {
	err := r.connector.Unsubscribe(r.profile.Topics...)
	if err != nil {
		return err
	}

	r.subscribed = make(map[string]struct{})
	r.binded = false
	return nil
}

func (r *Route) Handler(topic string, payload json.RawMessage, id ulid.ULID) {
	log := r.log.With(
		zap.String("action", "handler"),
		zap.String("id", id.String()),
		zap.String("topic", topic),
	)

	log.Debug("message arrived", zap.String("payload", string(payload)))

	for _, endpoint := range r.endpoints {
		log := log.With(zap.String("endpoint", endpoint.Name()))

		err := endpoint.Handle(topic, payload, id)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		log.Info("message handled")
	}
}

func (r *Route) Binded() bool {
	return r.binded
}
