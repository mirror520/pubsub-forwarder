package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/mirror520/pubsub-forwarder/model"
)

func NewNATSPubSub(cfg *model.Transport) (PubSub, error) {
	var opts *NATSOptions
	err := cfg.Broker.Options.Decode(&opts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &natsPubSub{
		log: zap.L().With(
			zap.String("pubsub", "nats"),
			zap.String("name", cfg.Name),
		),
		cfg:              cfg,
		opts:             opts,
		events:           make(map[string]*Event),
		subscribedTopics: make([]*subscribedTopic, 0),
		ctx:              ctx,
		cancel:           cancel,
	}, nil
}

type natsPubSub struct {
	log              *zap.Logger
	cfg              *model.Transport
	opts             *NATSOptions
	nc               *nats.Conn
	js               nats.JetStreamContext
	events           map[string]*Event
	subscribedTopics []*subscribedTopic

	ctx    context.Context
	cancel context.CancelFunc
	sync.RWMutex
}

func (ps *natsPubSub) Name() string {
	return ps.cfg.Name
}

func (ps *natsPubSub) Handle(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	return ps.Publish(topic, payload, ids...)
}

func (ps *natsPubSub) Connected() bool {
	if ps.nc != nil && !ps.nc.IsClosed() {
		return true
	}

	return false
}

func (ps *natsPubSub) Connect() error {
	if ps.Connected() {
		return nil
	}

	nc, err := nats.Connect(ps.cfg.Broker.Address)
	if err != nil {
		return err
	}

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	ps.nc = nc
	ps.js = js

	opts := ps.opts
	if opts != nil && opts.PullEnabled {
		ps.Lock()
		for name, jetstream := range opts.JetStreams {
			stream := jetstream.Stream
			if err := ps.AddStream(stream.Name, stream.Config); err != nil {
				return err
			}

			consumer := jetstream.Consumer
			if err := ps.AddConsumer(consumer.Name, stream.Name, consumer.Config); err != nil {
				return err
			}

			ps.events[name] = jetstream
		}
		ps.Unlock()
	}

	return nil
}

func (ps *natsPubSub) Close() error {
	client, err := ps.Client()
	if err != nil {
		return err
	}

	if ps.cancel != nil {
		ps.cancel()
	}

	return client.Drain()
}

func (ps *natsPubSub) Publish(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	client, err := ps.Client()
	if err != nil {
		return err
	}

	var id ulid.ULID
	if len(ids) > 0 {
		id = ids[0]
	} else {
		id = ulid.Make()
	}

	msg := nats.NewMsg(topic)
	msg.Header.Add("id", id.String())
	msg.Data = payload

	return client.PublishMsg(msg)
}

var re = regexp.MustCompile(`^(?P<name>\w+)::(?P<topic>.+)`)

func (ps *natsPubSub) Subscribe(topic string, callback MessageHandler) error {
	if len(ps.events) > 0 {
		topic = strings.ReplaceAll(topic, `#`, `>`)

		matches := re.FindAllStringSubmatch(topic, -1)
		if len(matches) > 0 {
			match := matches[0]

			event, ok := ps.events[match[1]]
			if !ok {
				return errors.New("event not found")
			}

			if match[2] == "#" {
				return ps.PullSubscribe(event.Consumer.Name, event.Stream.Name, callback)
			} else {
				return ps.PullSubscribe(event.Consumer.Name, event.Stream.Name, callback, match[2])
			}
		}
	}

	client, err := ps.Client()
	if err != nil {
		return err
	}

	topic = strings.ReplaceAll(topic, `#`, `>`)

	sub, err := client.Subscribe(topic, func(msg *nats.Msg) {
		idStr := msg.Header.Get("id")
		id, err := ulid.Parse(idStr)
		if err != nil {
			id = ulid.Make()
		}

		callback(msg.Subject, msg.Data, id)
	})

	if err != nil {
		return err
	}

	subscribedTopic := &subscribedTopic{
		sub:      sub,
		topic:    topic,
		callback: callback,
	}

	ps.Lock()
	ps.subscribedTopics = append(ps.subscribedTopics, subscribedTopic)
	ps.Unlock()

	return nil
}

func (ps *natsPubSub) Unsubscribe(topic ...string) error {
	if len(topic) == 0 {
		return errors.New("empty topic")
	}

	ps.Lock()
	defer ps.Unlock()

	remainingTopics := make(map[string]struct{})
	for _, subscribedTopic := range ps.subscribedTopics {
		remainingTopics[subscribedTopic.topic] = struct{}{}
	}

	var errs error
	for _, t := range topic {
		t = strings.ReplaceAll(t, `#`, `>`)

		delete(remainingTopics, t)

		for _, subscribedTopic := range ps.subscribedTopics {
			if subscribedTopic.topic != t {
				continue
			}

			if subscribedTopic.cancel != nil {
				subscribedTopic.cancel()
				subscribedTopic.cancel = nil
			}

			err := subscribedTopic.sub.Unsubscribe()
			if err != nil {
				errs = errors.Join(errs, err)
			}
		}
	}

	subscribedTopics := make([]*subscribedTopic, 0)
	for _, subscribedTopic := range ps.subscribedTopics {
		_, ok := remainingTopics[subscribedTopic.topic]
		if !ok {
			continue
		}

		subscribedTopics = append(subscribedTopics, subscribedTopic)
	}

	ps.subscribedTopics = subscribedTopics

	return errs
}

func (ps *natsPubSub) Client() (*nats.Conn, error) {
	if ps.nc == nil {
		return nil, ErrInvalidClient
	}

	if !ps.nc.IsConnected() {
		return nil, ErrClientDisconnected
	}

	return ps.nc, nil
}

func (ps *natsPubSub) AddStream(name string, raw json.RawMessage) error {
	if ps.js == nil {
		return ErrInvalidClient
	}

	var cfg *nats.StreamConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}

	cfg.Name = name

	_, err := ps.js.AddStream(cfg)
	return err
}

func (ps *natsPubSub) AddConsumer(name string, stream string, raw json.RawMessage, filter ...string) error {
	if ps.js == nil {
		return ErrInvalidClient
	}

	var cfg *nats.ConsumerConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}

	cfg.Durable = name

	if len(filter) > 0 {
		cfg.FilterSubject = filter[0]
	}

	_, err := ps.js.AddConsumer(stream, cfg)
	if err != nil {
		if !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
			return err
		}
	}

	return nil
}

func (ps *natsPubSub) PullSubscribe(consumer string, stream string, callback MessageHandler, filter ...string) error {
	if ps.js == nil {
		return ErrInvalidClient
	}

	subj := ""
	if len(filter) > 0 {
		subj = filter[0]
	}

	sub, err := ps.js.PullSubscribe(subj, consumer, nats.BindStream(stream))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ps.ctx)

	subscribedTopic := &subscribedTopic{
		log: ps.log.With(
			zap.String("consumer", consumer),
			zap.String("stream", stream),
		),
		sub:      sub,
		topic:    stream + "_" + consumer,
		callback: callback,
		cancel:   cancel,
	}

	go subscribedTopic.pull(ctx)

	ps.Lock()
	ps.subscribedTopics = append(ps.subscribedTopics, subscribedTopic)
	ps.Unlock()

	return nil
}

type subscribedTopic struct {
	log      *zap.Logger
	sub      *nats.Subscription
	topic    string
	callback MessageHandler
	cancel   context.CancelFunc
}

func (subscribedTopic *subscribedTopic) pull(ctx context.Context) {
	var (
		log      = subscribedTopic.log.With(zap.String("action", "pull_subscribe"))
		sub      = subscribedTopic.sub
		callback = subscribedTopic.callback
	)

	for {
		select {
		case <-ctx.Done():
			log.Info("done")
			return

		default:
			msgs, err := sub.Fetch(100)
			if err != nil && !errors.Is(err, nats.ErrTimeout) {
				log.Error(err.Error())
				continue
			}

			for _, msg := range msgs {
				idStr := msg.Header.Get("id")
				id, err := ulid.Parse(idStr)
				if err != nil {
					id = ulid.Make()
				}

				callback(msg.Subject, msg.Data, id)

				msg.Ack()
			}
		}
	}
}

type NATSOptions struct {
	PullEnabled bool              `yaml:"pull"`
	JetStreams  map[string]*Event `yaml:"jetstream"`
}

type Event struct {
	Stream   Stream
	Consumer Consumer
}

type Stream struct {
	Name   string
	Config json.RawMessage
}

func (s *Stream) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Name   string
		Config string
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	s.Name = raw.Name
	s.Config = json.RawMessage(raw.Config)

	return nil
}

type Consumer struct {
	Name   string
	Stream string
	Config json.RawMessage
}

func (c *Consumer) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Name   string
		Stream string
		Config string
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Name = raw.Name
	c.Stream = raw.Stream
	c.Config = json.RawMessage(raw.Config)

	return nil
}
