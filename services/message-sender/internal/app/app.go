package app

import "context"

type app struct {
	cfg        config
	grpcServer grpcServer
}

func New(cfg config) *app {
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
}

type grpcServer interface {
	Start() error
	Stop() error
}
