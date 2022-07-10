package signal

import (
	"go.uber.org/zap"
)

func LogTransient(ctx Context, log *zap.SugaredLogger) {
	IterTransient(ctx, func(err error) { log.Error(err) })
}

func IterTransient(ctx Context, f func(err error)) {
	go func() {
		for {
			select {
			case err := <-ctx.Transient():
				f(err)
			case <-ctx.Stopped():
				return
			}
		}
	}()
}
