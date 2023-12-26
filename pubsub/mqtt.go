package pubsub

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/mirror520/pubsub-forwarder/model"
)

const QoS byte = 1

func NewMQTTPubSub(cfg *model.Transport) (PubSub, error) {
	return &mqttPubSub{
		log: zap.L().With(
			zap.String("pubsub", "mqtt"),
			zap.String("name", cfg.Name),
		),
		cfg:              cfg,
		subscribedTopics: make(map[string]MessageHandler),
	}, nil
}

type mqttPubSub struct {
	log              *zap.Logger
	cfg              *model.Transport
	client           mqtt.Client
	subscribedTopics map[string]MessageHandler // map[Topic]MessageHandler
	sync.RWMutex
}

func (ps *mqttPubSub) Name() string {
	return ps.cfg.Name
}

func (ps *mqttPubSub) Handle(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	return ps.Publish(topic, payload, ids...)
}

func (ps *mqttPubSub) Connected() bool {
	if ps.client != nil && ps.client.IsConnected() {
		return true
	}

	return false
}

func (ps *mqttPubSub) Connect() error {
	if ps.Connected() {
		return nil
	}

	broker := ps.cfg.Broker
	opts := mqtt.NewClientOptions().
		SetClientID(ps.cfg.Name + "_" + strconv.Itoa(int(time.Now().UnixNano()))).
		AddBroker(broker.Address).
		SetUsername(broker.Username).
		SetPassword(broker.Password).
		SetOnConnectHandler(ps.ConnectHandler()).
		SetConnectionLostHandler(ps.ConnectionLostHandler())

	ps.client = mqtt.NewClient(opts)

	token := ps.client.Connect()
	token.Wait()

	return token.Error()
}

func (ps *mqttPubSub) Close() error {
	client, err := ps.Client()
	if err != nil {
		return err
	}

	client.Disconnect(250)
	return nil
}

func (ps *mqttPubSub) Publish(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	client, err := ps.Client()
	if err != nil {
		return err
	}

	topic = strings.ReplaceAll(topic, `.`, `/`)

	token := client.Publish(topic, QoS, false, payload)

	token.Wait()
	return token.Error()
}

func (ps *mqttPubSub) Subscribe(topic string, callback MessageHandler) error {
	client, err := ps.Client()
	if err != nil {
		return err
	}

	ps.Lock()
	defer ps.Unlock()

	_, ok := ps.subscribedTopics[topic]
	if ok {
		return errors.New("topic is already subscribed")
	}

	topic = strings.ReplaceAll(topic, `.`, `/`)
	topic = strings.ReplaceAll(topic, `*`, `+`)

	token := client.Subscribe(topic, QoS, func(client mqtt.Client, msg mqtt.Message) {
		msgTopic := msg.Topic()
		msgTopic = strings.ReplaceAll(msgTopic, `/`, `.`)

		callback(msgTopic, msg.Payload(), ulid.Make())
	})

	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}

	ps.subscribedTopics[topic] = callback

	return nil
}

func (ps *mqttPubSub) Unsubscribe(topic ...string) error {
	if len(topic) == 0 {
		return errors.New("empty topic")
	}

	client, err := ps.Client()
	if err != nil {
		return err
	}

	topics := make([]string, len(topic))

	ps.Lock()
	for i, t := range topic {
		delete(ps.subscribedTopics, t)

		t = strings.ReplaceAll(t, `.`, `/`)
		t = strings.ReplaceAll(t, `*`, `+`)
		topics[i] = t
	}
	ps.Unlock()

	token := client.Unsubscribe(topics...)

	token.Wait()
	return token.Error()
}

func (ps *mqttPubSub) Client() (mqtt.Client, error) {
	if ps.client == nil {
		return nil, ErrInvalidClient
	}

	if !ps.client.IsConnectionOpen() {
		return nil, ErrClientDisconnected
	}

	return ps.client, nil
}

func (ps *mqttPubSub) ConnectHandler() mqtt.OnConnectHandler {
	return func(client mqtt.Client) {
		log := ps.log.With(
			zap.String("handler", "connect"),
		)
		log.Info("client connected")

		ps.RLock()
		for topic, callback := range ps.subscribedTopics {
			log := log.With(
				zap.String("topic", topic),
			)

			token := client.Subscribe(topic, QoS, func(client mqtt.Client, msg mqtt.Message) {
				msgTopic := msg.Topic()
				msgTopic = strings.ReplaceAll(msgTopic, `/`, `.`)

				callback(msgTopic, msg.Payload(), ulid.Make())
			})

			token.Wait()
			if err := token.Error(); err != nil {
				log.Error(err.Error())
				continue
			}

			log.Info("topic re-subscribed successfully")
		}
		ps.RUnlock()
	}
}

func (ps *mqttPubSub) ConnectionLostHandler() mqtt.ConnectionLostHandler {
	return func(client mqtt.Client, err error) {
		ps.log.Error(err.Error(), zap.String("handler", "connection_lost"))
	}
}
