package router

import (
	"encoding/json"

	"github.com/oklog/ulid/v2"
	"github.com/tdewolff/minify/v2"

	jsonMIME "github.com/tdewolff/minify/v2/json"

	"github.com/mirror520/pubsub-forwarder/pubsub"
)

type MIME string

const (
	JSON MIME = "application/json"
	XML  MIME = "application/xml"
)

func (m MIME) Type() string {
	return string(m)
}

func MinifyMiddleware(mime ...MIME) pubsub.MessageMiddleware {
	m := minify.New()
	if len(mime) == 0 {
		m.AddFunc(JSON.Type(), jsonMIME.Minify)
	} else {
		for _, t := range mime {
			switch t {
			case JSON:
				m.AddFunc(JSON.Type(), jsonMIME.Minify)
			}
		}
	}

	return func(next pubsub.MessageHandler) pubsub.MessageHandler {
		return func(topic string, payload json.RawMessage, id ulid.ULID) {
			minifiedPayload, err := m.Bytes("application/json", payload)
			if err != nil {
				next(topic, payload, id)
				return
			}

			next(topic, minifiedPayload, id)
		}
	}
}
