package router

import (
	"time"

	"go.uber.org/zap"
)

func LoggingMiddleware(log *zap.Logger) ServiceMiddleware {
	return func(next Service) Service {
		return &loggingMiddleware{
			log:  log.With(zap.String("service", "router")),
			next: next,
		}
	}
}

type loggingMiddleware struct {
	log  *zap.Logger
	next Service
}

func (mw *loggingMiddleware) Close() {
	mw.next.Close()
}

func (mw *loggingMiddleware) Replay(topic string, since time.Time, from string, to string) (string, error) {
	log := mw.log.With(
		zap.String("action", "replay"),
		zap.String("from", from),
		zap.String("to", to),
		zap.String("topic", topic),
		zap.Time("since", since),
	)

	id, err := mw.next.Replay(topic, since, from, to)
	if err != nil {
		log.Error(err.Error())
		return "", err
	}

	log.Info("replay created", zap.String("id", id))
	return id, nil
}

func (mw *loggingMiddleware) CloseTask(id string) error {
	log := mw.log.With(
		zap.String("action", "close_task"),
		zap.String("id", id),
	)

	err := mw.next.CloseTask(id)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("task closed")
	return nil
}
