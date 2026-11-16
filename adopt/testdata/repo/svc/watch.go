package svc

import (
	"context"
	"time"

	"github.com/go-apis/loom"

	"example.com/svc/loomgen"
)

type Watch struct{}

// OnWatchTicked re-arms the watch.
func (h *Watch) OnWatchTicked(ctx context.Context, evt *loom.Event, data *loomgen.WatchTicked) ([]loom.Command, error) {
	return []loom.Command{loom.After(&loomgen.TickWatch{}, time.Minute)}, nil
}

// OnOrderPlaced arms a one-shot timeout: what loom.After is for.
func (h *Watch) OnOrderPlaced(ctx context.Context, evt *loom.Event, data *loomgen.OrderPlaced) ([]loom.Command, error) {
	return []loom.Command{loom.After(&loomgen.CancelOrder{}, time.Hour)}, nil
}
