package clock

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// CoolAnvil implements loomgen.CoolAnvilReactions. Yours to edit.
type CoolAnvil struct{}

// HotFor scripts how many looks find the anvil still hot, and Looks
// counts the looks made.
var HotFor, Looks atomic.Int64

// Dawdle scripts how long, in milliseconds, each look takes.
var Dawdle atomic.Int64

// PlannedLooks counts the looks made while a plan was working out what
// the process would do.
var PlannedLooks atomic.Int64

func (h *CoolAnvil) OnAnvilStruck(ctx context.Context, evt *loom.Event, data *loomgen.AnvilStruck) ([]loom.Command, error) {
	time.Sleep(time.Duration(Dawdle.Load()) * time.Millisecond)
	if loom.Planning(ctx) {
		PlannedLooks.Add(1)
	}
	if Looks.Add(1) <= HotFor.Load() {
		return nil, loom.NotYet("the anvil is still hot")
	}
	return []loom.Command{&loomgen.RecordAnvilCool{CommandBase: loom.CommandBase{Namespace: evt.Namespace, AggregateID: evt.AggregateID}}}, nil
}
