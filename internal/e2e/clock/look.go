package clock

import (
	"context"
	"sync"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
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

// OvenLooks records each look at an oven: what it was told.
var (
	ovenMu   sync.Mutex
	ovenTold = map[string][]loom.Looking{}
)

// OvenToldOf is every look made at the oven, oldest first.
func OvenToldOf(id string) []loom.Looking {
	ovenMu.Lock()
	defer ovenMu.Unlock()
	return append([]loom.Looking(nil), ovenTold[id]...)
}

func (l *Look) LookOven(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	ovenMu.Lock()
	ovenTold[told.ID.String()] = append(ovenTold[told.ID.String()], told)
	script := ovenScript
	ovenMu.Unlock()
	if script != nil {
		return script(ctx)
	}
	return nil, nil
}

var (
	ovenScript func(ctx context.Context) ([]loom.Command, error)
	loafMu     sync.Mutex
	loafLook   func(ctx context.Context) ([]loom.Command, error)
	loafSeen   = map[string]int{}
)

// SetOvenScript scripts what an oven's look does beyond recording what
// it was told; nil, the default, does nothing more.
func SetOvenScript(f func(ctx context.Context) ([]loom.Command, error)) {
	ovenMu.Lock()
	defer ovenMu.Unlock()
	ovenScript = f
}

// SetLoafLook scripts the look at a loaf; nil does nothing.
func SetLoafLook(f func(ctx context.Context) ([]loom.Command, error)) {
	loafMu.Lock()
	defer loafMu.Unlock()
	loafLook = f
}

// LoafLooks is how many looks have been made at the loaf.
func LoafLooks(id string) int {
	loafMu.Lock()
	defer loafMu.Unlock()
	return loafSeen[id]
}

func (l *Look) LookLoaf(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	loafMu.Lock()
	loafSeen[told.ID.String()]++
	look := loafLook
	loafMu.Unlock()
	if look == nil {
		return nil, nil
	}
	return look(ctx)
}

// TicketLooks records each look at a ticket: what it was told.
var (
	ticketMu     sync.Mutex
	ticketTold   = map[string][]loom.Looking{}
	ticketScript func(ctx context.Context) ([]loom.Command, error)
)

// TicketToldOf is every look made at the ticket, oldest first.
func TicketToldOf(id string) []loom.Looking {
	ticketMu.Lock()
	defer ticketMu.Unlock()
	return append([]loom.Looking(nil), ticketTold[id]...)
}

// SetTicketScript scripts the look at a ticket in place of DecideTicket;
// nil, the default, decides with DecideTicket.
func SetTicketScript(f func(ctx context.Context) ([]loom.Command, error)) {
	ticketMu.Lock()
	defer ticketMu.Unlock()
	ticketScript = f
}

func (l *Look) LookTicket(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	ticketMu.Lock()
	ticketTold[told.ID.String()] = append(ticketTold[told.ID.String()], told)
	script := ticketScript
	ticketMu.Unlock()
	if script != nil {
		return script(ctx)
	}
	return DecideTicket(ctx)
}

// TriageResult is what the triage agent answers.
type TriageResult struct {
	Verdict string `json:"verdict"`
}

// DecideTicket is a ticket's next step: an open ticket asks the triage
// agent, and is closed with its verdict once it answers — "failed" when
// its job failed, "unanswered" when it ran out of time.
func DecideTicket(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	state, version, err := loom.Load(ctx, "Ticket", told.Namespace, told.ID)
	if err != nil {
		return nil, err
	}
	if version == 0 || state.(*loomgen.Ticket).Status != "open" {
		return nil, nil
	}
	ticket := loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}
	triage, asked := told.Agent(loomgen.TicketTriageAgent)
	switch {
	case !asked:
		return nil, loomgen.AskTicketTriage(ctx, loom.AgentAsk{})
	case triage.Status == loom.AgentFinished:
		var res TriageResult
		if err := triage.Result(&res); err != nil {
			return nil, err
		}
		return []loom.Command{&loomgen.CloseTicket{CommandBase: ticket, Verdict: res.Verdict}}, nil
	case triage.Status == loom.AgentFailed:
		return []loom.Command{&loomgen.CloseTicket{CommandBase: ticket, Verdict: "failed"}}, nil
	case triage.Status == loom.AgentExpired:
		return []loom.Command{&loomgen.CloseTicket{CommandBase: ticket, Verdict: "unanswered"}}, nil
	}
	// asked, and still waiting: ask again, which does nothing while open
	return nil, loomgen.AskTicketTriage(ctx, loom.AgentAsk{})
}
