package router

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mirror520/pubsub-forwarder/interfaces"
	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/persistence"
	"github.com/mirror520/pubsub-forwarder/pubsub"
)

type Service interface {
	Close()

	Replay(topic string, since time.Time, from string, to string) (string, error)
	CloseTask(id string) error
}

type ServiceMiddleware func(next Service) Service

type service struct {
	log *zap.Logger
	cfg *model.Config

	pubSubs   map[string]pubsub.PubSub       // map[Name]pubsub.PubSub
	storages  map[string]persistence.Storage // map[Name]persistence.Storage
	routesMap map[string][]*Route            // map[Connector][]*Route
	tasks     sync.Map                       // map[ID]*ReplayTask

	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(cfg *model.Config) Service {
	svc := new(service)
	svc.log = zap.L().With(
		zap.String("service", "router"),
	)
	svc.cfg = cfg

	pubSubs := make(map[string]pubsub.PubSub)
	for _, profile := range cfg.Transports {
		log := svc.log.With(
			zap.String("pubsub", profile.Name),
		)

		ps, err := pubsub.NewPubSub(profile)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		pubSubs[profile.Name] = ps
		log.Info("added pubsub")
	}

	storages := make(map[string]persistence.Storage)
	for _, profile := range cfg.Persistences {
		log := svc.log.With(
			zap.String("persistence", profile.Name),
		)

		s, err := persistence.NewStorage(profile)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		storages[profile.Name] = s
		log.Info("added storage")
	}

	routesMap := make(map[string][]*Route)
	for id, profile := range cfg.Routes {
		log := svc.log.With(zap.String("route", id))

		connector, ok := pubSubs[profile.Connector]
		if !ok {
			log.Error("connector not found", zap.String("connector", profile.Connector))
			continue
		}

		endpoints := make([]interfaces.Endpoint, 0)
		for _, ep := range profile.Endpoints {
			if ps, ok := pubSubs[ep]; ok {
				endpoints = append(endpoints, ps)
				log.Info("added endpoint to route", zap.String("pubsub", ep))
			}

			if s, ok := storages[ep]; ok {
				endpoints = append(endpoints, s)
				log.Info("added endpoint to route", zap.String("storage", ep))
			}
		}

		if count := len(endpoints); count < len(profile.Endpoints) {
			log.Error("insufficient endpoints", zap.Int("count", count))
			continue
		}

		routes, ok := routesMap[profile.Connector]
		if !ok {
			routes = make([]*Route, 0)
		}

		route := &Route{
			id:         id,
			log:        log,
			profile:    profile,
			connector:  connector,
			endpoints:  endpoints,
			subscribed: make(map[string]struct{}),
		}

		routesMap[profile.Connector] = append(routes, route)
	}

	svc.pubSubs = pubSubs
	svc.storages = storages
	svc.routesMap = routesMap

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel

	go svc.CheckConnAndRoutes(ctx, 30*time.Second)

	return svc
}

func (svc *service) CheckConnAndRoutes(ctx context.Context, duration time.Duration) {
	log := svc.log.With(
		zap.String("action", "check_conn_and_routes"),
	)

	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ctx.Done():
			log := log.With(
				zap.String("phase", "close"),
			)

			for _, routers := range svc.routesMap {
				for _, r := range routers {
					log := log.With(zap.String("router", r.id))

					if err := r.Unbind(); err != nil {
						log.Error(err.Error())
						continue
					}

					log.Info("router unbinded")
				}
			}

			for name, s := range svc.storages {
				log := log.With(zap.String("persistence", name))

				if err := s.Close(); err != nil {
					log.Error(err.Error())
					continue
				}

				log.Info("closed")
			}

			for name, ps := range svc.pubSubs {
				log := log.With(zap.String("pubsub", name))

				if err := ps.Close(); err != nil {
					log.Error(err.Error())
					continue
				}

				log.Info("closed")
			}

			log.Info("done")
			return

		case <-ticker.C:
			for name, ps := range svc.pubSubs {
				if ps.Connected() {
					continue
				}

				log := log.With(
					zap.String("phase", "check_conn"),
					zap.String("pubsub", name),
				)

				err := ps.Connect()
				if err != nil {
					log.Error(err.Error())
					continue
				}

				log.Info("connected")
			}

			for name, s := range svc.storages {
				if s.Connected() {
					continue
				}

				log := log.With(
					zap.String("phase", "check_conn"),
					zap.String("persistence", name),
				)

				err := s.Connect()
				if err != nil {
					log.Error(err.Error())
					continue
				}

				log.Info("connected")
			}

			for _, routes := range svc.routesMap {
				for _, route := range routes {
					if route.Binded() {
						continue
					}

					log := log.With(
						zap.String("phase", "check_route"),
						zap.String("route", route.id),
					)

					err := route.Bind()
					if err != nil {
						log.Error(err.Error())
						continue
					}

					log.Info("route binded")
				}
			}
		}
	}
}

func (svc *service) Close() {
	if svc.cancel != nil {
		svc.cancel()
	}

	svc.cancel = nil
}

func (svc *service) Replay(topic string, since time.Time, from string, to string) (string, error) {
	endpoints := make([]interfaces.Endpoint, 0)
	for name, ps := range svc.pubSubs {
		if name != to {
			continue
		}

		endpoints = append(endpoints, ps)
	}

	for name, s := range svc.storages {
		if name != to {
			continue
		}

		endpoints = append(endpoints, s)
	}

	if len(endpoints) == 0 {
		return "", errors.New("endpoint not found")
	}

	storage, ok := svc.storages[from]
	if !ok {
		return "", errors.New("storage not found")
	}

	it, err := storage.Iterator(topic, since)
	if err != nil {
		return "", err
	}

	log := svc.log.With(zap.String("from", from))
	ctx := context.WithValue(svc.ctx, model.LOGGER, log)

	task, err := NewReplayTask(ctx, it, endpoints)
	if err != nil {
		return "", err
	}

	svc.tasks.Store(task.ID(), task)

	go task.Start()

	return task.ID(), nil
}

func (svc *service) CloseTask(id string) error {
	val, ok := svc.tasks.LoadAndDelete(id)
	if !ok {
		return errors.New("task not found")
	}

	task, ok := val.(Task)
	if !ok {
		return errors.New("invalid task")
	}

	task.Close()
	return nil
}
