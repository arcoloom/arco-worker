//go:build !linux

package runtime

import (
	"context"
	"errors"
	"log/slog"
)

type ContainerdEngine struct {
	logger *slog.Logger
}

var _ Engine = (*ContainerdEngine)(nil)
var _ LogEmitterAware = (*ContainerdEngine)(nil)

func NewContainerdEngine(logger *slog.Logger) *ContainerdEngine {
	if logger == nil {
		logger = slog.Default()
	}
	return &ContainerdEngine{logger: logger}
}

func (e *ContainerdEngine) SetLogEmitter(LogEmitter) {}

func (e *ContainerdEngine) Prepare(context.Context, []byte) error {
	return errors.New("container runtime is only supported on linux workers")
}

func (e *ContainerdEngine) Start(context.Context, []byte) error {
	return errors.New("container runtime is only supported on linux workers")
}

func (e *ContainerdEngine) Interrupt(context.Context) error {
	return errors.New("container runtime is only supported on linux workers")
}

func (e *ContainerdEngine) Stop(context.Context) error {
	return errors.New("container runtime is only supported on linux workers")
}

func (e *ContainerdEngine) Wait(context.Context) error {
	return errors.New("container runtime is only supported on linux workers")
}
