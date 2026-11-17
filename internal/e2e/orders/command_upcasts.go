package orders

import "encoding/json"

// CommandUpcasts implements loomgen.CommandUpcasts — migration for commands
// loom stored to run later (timers, batch items, plans) under older @v
// versions. Each hop takes the command JSON of one version and returns the
// command JSON of the next; loom chains hops when the command is read to
// run, so handlers only ever see the current shape. Keep aggregate_id and
// namespace. Yours to edit.
type CommandUpcasts struct{}

// CancelOrderFromV1 lifts a stored CancelOrder v1 → v2: v1 called the
// reason `why`.
func (u *CommandUpcasts) CancelOrderFromV1(data []byte) ([]byte, error) {
	var cmd map[string]any
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	if why, ok := cmd["why"]; ok {
		cmd["reason"] = why
		delete(cmd, "why")
	}
	return json.Marshal(cmd)
}
