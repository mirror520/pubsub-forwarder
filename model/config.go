package model

import (
	"gopkg.in/yaml.v3"

	"github.com/mirror520/events"
)

type Config struct {
	Transports   []*Transport      `yaml:"transports"`
	Persistences []*Persistence    `yaml:"persistences"`
	Routes       map[string]*Route `yaml:"routes"`
	Path         string            `yaml:"-"`
}

func (cfg *Config) SetPath(path string) {
	cfg.Path = path

	for _, p := range cfg.Persistences {
		if p.Driver == events.BadgerDB && p.DSN == "" {
			p.Persistence.DSN = path + "/events"
		}
	}
}

type Protocol string

const (
	MQTT  Protocol = "mqtt"
	NATS  Protocol = "nats"
	Kafka Protocol = "kafka"
)

type Transport struct {
	Name     string
	Protocol Protocol
	Broker   Broker
}

type Broker struct {
	Address  string
	Username string
	Password string
	Options  yaml.Node `yaml:"opts"`
}

type Persistence struct {
	Name string
	events.Persistence
}

func (p *Persistence) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Name   string               `yaml:"name"`
		Driver events.StorageDriver `yaml:"driver"`
		DSN    string               `yaml:"dsn"`
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	p.Name = raw.Name
	p.Persistence = events.Persistence{
		Driver: raw.Driver,
		DSN:    raw.DSN,
	}

	return nil
}

type Route struct {
	Connector string
	Topics    []string
	Endpoints []string
}
