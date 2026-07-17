package orders

import "encoding/json"

// Upcasts implements loomgen.EventUpcasts — migration for stored events
// written under older @v versions. Each hop takes the payload JSON of one
// version and returns the payload JSON of the next; loom chains hops at
// decode time, so folds and reactions only ever see the current shape.
// Keep hops deterministic: replays and rebuilds run them again.
// Yours to edit.
type Upcasts struct{}

// OrderCancelledFromV1 lifts OrderCancelled v1 → v2: rows written before
// `reason` existed read as "unspecified".
func (u *Upcasts) OrderCancelledFromV1(data []byte) ([]byte, error) {
	var old struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &old); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"status": old.Status, "reason": "unspecified"})
}
