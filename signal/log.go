package signal

import (
	"go.uber.org/zap"
)

func LogTransient(ctx Context, log *zap.SugaredLogger) {
	go func() {
		for {
			select {
			case err := <-ctx.Transient():
				log.Error(err)
			case <-ctx.Stopped():
				return
			}
		}
	}()
}
