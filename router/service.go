package router

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/pubsub"
)

type Service interface {
	Close()
}

type service struct {
	log       *zap.Logger
	cfg       *model.Config
	pubSubs   map[string]pubsub.PubSub // map[Name]pubsub.PubSub
	routesMap map[string][]*Route      // map[Connector][]*Route
	cancel    context.CancelFunc
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
	svc.pubSubs = pubSubs

	routesMap := make(map[string][]*Route)
	for id, profile := range cfg.Routes {
		log := svc.log.With(zap.String("route", id))

		connector, ok := pubSubs[profile.Connector]
		if !ok {
			log.Error("connector not found", zap.String("connector", profile.Connector))
			continue
		}

		endpoints := make([]pubsub.PubSub, 0)
		for _, e := range profile.Endpoints {
			endpoint, ok := pubSubs[e.Transport]
			if !ok {
				log.Error("endpoint not found", zap.String("endpoint", e.Transport))
				continue
			}

			endpoints = append(endpoints, endpoint)
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
	svc.routesMap = routesMap

	ctx, cancel := context.WithCancel(context.Background())
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
}
