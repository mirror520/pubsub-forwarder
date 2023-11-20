package model

import "gopkg.in/yaml.v3"

type Config struct {
	Transports []Transport
	Routes     map[string]Route
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

type Route struct {
	Connector string
	Topics    []string
	Endpoints []Endpoint
}

type Endpoint struct {
	Transport string
	Rewrite   string
}
