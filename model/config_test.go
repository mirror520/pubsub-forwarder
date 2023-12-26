package model

import (
	"os"
	"testing"

	"github.com/mirror520/events"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestLoadConfig(t *testing.T) {
	assert := assert.New(t)

	f, err := os.Open("../config.example.yaml")
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	var cfg *Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		assert.Fail(err.Error())
		return
	}

	cfg.SetPath(".forwarder")

	assert.Equal("events", cfg.Persistences[0].Name)
	assert.Equal(events.BadgerDB, cfg.Persistences[0].Driver)
	assert.Equal(".forwarder/events", cfg.Persistences[0].DSN)
}
