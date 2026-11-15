package clock

import (
	"context"
	"sync"

	"github.com/go-apis/loom"
)

// Look implements loomgen.Look — each @loop aggregate's recurring look.
// Yours to edit.
type Look struct{}

// KilnLook scripts what a look at a kiln does; nil, the default, does
// nothing. Looks counts the looks made at each kiln, and Told keeps
// what the last look at each was told.
var (
	lookMu   sync.Mutex
	kilnLook func(ctx context.Context) ([]loom.Command, error)
	kilnSeen = map[string]int{}
	kilnTold = map[string]loom.Looking{}
)

// SetKilnLook scripts the look at a kiln.
func SetKilnLook(f func(ctx context.Context) ([]loom.Command, error)) {
	lookMu.Lock()
	defer lookMu.Unlock()
	kilnLook = f
}

// KilnLooks is how many looks have been made at the kiln, and what the
// last was told.
func KilnLooks(id string) (int, loom.Looking) {
	lookMu.Lock()
	defer lookMu.Unlock()
	return kilnSeen[id], kilnTold[id]
}

func (l *Look) LookKiln(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	lookMu.Lock()
	kilnSeen[told.ID.String()]++
	kilnTold[told.ID.String()] = told
	look := kilnLook
	lookMu.Unlock()
	if look == nil {
		return nil, nil
	}
	return look(ctx)
}
