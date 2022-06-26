package signal

import (
	"go.uber.org/zap"
)

func LogTransient(ctx Context, log *zap.SugaredLogger) {
	ctx.Go(func() error {
		for {
			select {
			case err := <-ctx.Transient():
				log.Error(err)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}
