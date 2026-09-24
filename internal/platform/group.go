package platform

import (
	"context"
	"errors"
)

type Service func(context.Context) error

func RunServices(ctx context.Context, services ...Service) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(services))
	for _, service := range services {
		go func(run Service) { errCh <- run(runCtx) }(service)
	}
	var firstErr error
	for i := 0; i < len(services); i++ {
		err := <-errCh
		if i == 0 {
			cancel()
		}
		if err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
