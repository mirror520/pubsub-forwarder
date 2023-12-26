package interfaces

import (
	"encoding/json"

	"github.com/oklog/ulid/v2"
)

type Endpoint interface {
	Name() string
	Handle(topic string, payload json.RawMessage, ids ...ulid.ULID) error
}
