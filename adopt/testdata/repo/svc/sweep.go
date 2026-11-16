package svc

import (
	"context"
	"time"
)

// Sweep loops on a ticker on every instance.
func Sweep(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweepOnce(ctx)
		}
	}
}

func sweepOnce(context.Context) {}

// once reads a ticker once: not a polling loop.
func once() {
	t := time.NewTicker(time.Second)
	<-t.C
	t.Stop()
}
