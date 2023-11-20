package pubsub

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/mirror520/pubsub-forwarder/model"
)

const QoS byte = 1

func NewMQTTPubSub(cfg model.Transport) (PubSub, error) {
	return &mqttPubSub{
		log: zap.L().With(
			zap.String("pubsub", "mqtt"),
			zap.String("name", cfg.Name),
		),
		cfg:              cfg,
		subscribedTopics: make([]*SubscribedTopic, 0),
	}, nil
}

type mqttPubSub struct {
	log              *zap.Logger
	cfg              model.Transport
	client           mqtt.Client
	subscribedTopics []*SubscribedTopic
	sync.RWMutex
}

func (ps *mqttPubSub) Name() string {
	return ps.cfg.Name
}

func (ps *mqttPubSub) Connected() bool {
	if ps.client != nil && ps.client.IsConnected() {
		return true
	}

	return false
}

func (ps *mqttPubSub) Connect() error {
	if ps.client != nil && ps.client.IsConnected() {
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

func (ps *mqttPubSub) Publish(topic string, payload []byte) error {
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

	topic = strings.ReplaceAll(topic, `.`, `/`)
	topic = strings.ReplaceAll(topic, `*`, `+`)

	token := client.Subscribe(topic, QoS, func(client mqtt.Client, msg mqtt.Message) {
		msgTopic := msg.Topic()
		msgTopic = strings.ReplaceAll(msgTopic, `/`, `.`)

		callback(msgTopic, msg.Payload())
	})

	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}

	subscribedTopic := &SubscribedTopic{
		Topic:    topic,
		Callback: callback,
	}

	ps.Lock()
	ps.subscribedTopics = append(ps.subscribedTopics, subscribedTopic)
	ps.Unlock()
	return nil
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
		for _, sub := range ps.subscribedTopics {
			log := log.With(
				zap.String("topic", sub.Topic),
			)

			token := client.Subscribe(sub.Topic, QoS, func(client mqtt.Client, msg mqtt.Message) {
				msgTopic := msg.Topic()
				msgTopic = strings.ReplaceAll(msgTopic, `/`, `.`)

				sub.Callback(msgTopic, msg.Payload())
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
