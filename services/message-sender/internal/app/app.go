package app

import (
	"context"

	"github.com/yakirLE/message-system/nats"
)

type app struct {
	cfg        config
	grpcServer grpcServer
}

func New(cfg config) *app {
	natsQueue := nats.New(cfg, "message-queue", "message-type-1")

	return nil
}

func (a app) Start(ctx context.Context) error {
	return nil
}

func (a app) Stop(ctx context.Context) error {
	return nil
}

type config interface {
	GetString(key string) string
	GetInt(key string) int
}

type grpcServer interface {
	Start() error
	Stop() error
}
