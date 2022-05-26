package pipe

import shut "github.com/arya-analytics/x/shutdown"

func Transform[V any](req <-chan V, sd shut.Shutdown, transform func(V) V) <-chan V {
	out := make(chan V)
	sd.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case r := <-req:
				out <- transform(r)
			}
		}
	})
	return out
}
