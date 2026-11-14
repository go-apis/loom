package loom

import "errors"

// A reaction sometimes has nothing to do yet: the deploy it watches is
// still rolling out, the channel it posts to does not exist, the
// provider has not settled the payment. It is not a failure, and the
// answer is to look again later, a little less often each time, until
// it is done or it has been too long.
//
// A process that declares @retry already has that machinery (retry.go):
// a durable timer per (process, event), backing off between its bounds,
// that ends in a dead letter after its attempts. NotYet is how a
// reaction asks for it on purpose:
//
//	process awaitDeploy @retry(40, 5s..5m) {
//	  on DeployRequested -> RecordDeployOutcome
//	}
//
//	func (h *AwaitDeploy) OnDeployRequested(ctx context.Context, evt *loom.Event, d *DeployRequested) ([]loom.Command, error) {
//	    state, err := h.Runs.Get(ctx, d.RunID) // a read: call it directly
//	    if err != nil {
//	        return nil, err // a failure: the usual retries
//	    }
//	    if !state.Finished {
//	        return nil, loom.NotYet("the deploy is still rolling out")
//	    }
//	    return []loom.Command{&RecordDeployOutcome{...}}, nil
//	}
//
// What NotYet changes from an error: the reaction is not tried again at
// once (the three quick attempts are for failures), nothing is logged as
// a failure, and the durable timer starts straight away. What it does
// not change: the attempts and bounds are @retry's, and when they run
// out the event is parked — a thing that never became ready is for a
// person to look at.
//
// The look itself is a read, so make it directly rather than through
// loom.Once: Once remembers a result so that a call is made once, and a
// remembered "not ready" would be the answer every time.

// NotYet is the error a reaction returns when what it waits for has not
// happened yet. reason says what it is waiting for; it is what the dead
// letter reads if the wait runs out.
func NotYet(reason string) error { return &notYetError{reason: reason} }

// IsNotYet reports whether err is, or wraps, a NotYet.
func IsNotYet(err error) bool {
	var n *notYetError
	return errors.As(err, &n)
}

type notYetError struct{ reason string }

func (e *notYetError) Error() string { return "not yet: " + e.reason }
