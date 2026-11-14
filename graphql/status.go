package graphql

import (
	"encoding/json"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// serviceStatus is the gateway's read of what each composed service's
// background work is doing (loom.Client.Status): who leads its
// processes, how far each projection and process is behind, and what is
// waiting. It is about the service, not about any tenant, so it takes
// no namespace — and under DefaultPolicy a read with no namespace passes
// only All access. A deployment that wants its operators to see it says
// so in its Policy (Field "serviceStatus").

const statusField = "serviceStatus"

var (
	runnerStatus = gql.NewObject(gql.ObjectConfig{
		Name:        "RunnerStatus",
		Description: "One projection or process against the event log.",
		Fields: gql.Fields{
			"runner":       {Type: gql.NewNonNull(gql.String), Resolve: mapField("runner")},
			"kind":         {Type: gql.NewNonNull(gql.String), Resolve: mapField("kind"), Description: "projection, process, or process(bus). A bus consumer keeps no position."},
			"seq":          {Type: gql.NewNonNull(scalarLong), Resolve: mapField("seq"), Description: "The last event it handled."},
			"lag":          {Type: gql.NewNonNull(scalarLong), Resolve: mapField("lag"), Description: "How many log positions it is behind the newest event."},
			"behindSince":  {Type: scalarTime, Resolve: mapField("behind_since"), Description: "When the oldest event it has not handled was written. Null when caught up."},
			"updatedAt":    {Type: scalarTime, Resolve: mapField("updated_at")},
			"stalled":      {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("stalled"), Description: "Halted on an event it cannot handle."},
			"failingSeq":   {Type: scalarLong, Resolve: mapField("failing_seq")},
			"attempts":     {Type: gql.Int, Resolve: mapField("attempts")},
			"lastError":    {Type: gql.String, Resolve: mapField("last_error")},
			"stalledSince": {Type: scalarTime, Resolve: mapField("stalled_since")},
		},
	})
	leaseStatus = gql.NewObject(gql.ObjectConfig{
		Name:        "LeaseStatus",
		Description: "Which instance runs the service's processes.",
		Fields: gql.Fields{
			"needed":         {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("needed"), Description: "Whether the service has processes that react to its own events. Without any it elects no leader, and held is false by design."},
			"held":           {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("held"), Description: "False when no instance leads. Where a lease is needed, nothing reacts to events until one does."},
			"generation":     {Type: scalarLong, Resolve: mapField("generation"), Description: "The leader's deploy generation."},
			"revision":       {Type: gql.String, Resolve: mapField("revision"), Description: "The leader's revision."},
			"yielding":       {Type: gql.Boolean, Resolve: mapField("yielding"), Description: "The leader is handing over to a newer deploy."},
			"here":           {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("here"), Description: "Whether the instance that answered is the leader."},
			"hereGeneration": {Type: scalarLong, Resolve: mapField("here_generation"), Description: "The answering instance's deploy generation. Newer than the leader's means a deploy whose hand-over has not happened."},
			"hereRevision":   {Type: gql.String, Resolve: mapField("here_revision")},
		},
	})
	timerStatus = gql.NewObject(gql.ObjectConfig{
		Name: "TimerStatus",
		Fields: gql.Fields{
			"pending":   {Type: gql.NewNonNull(scalarLong), Resolve: mapField("pending")},
			"overdue":   {Type: gql.NewNonNull(scalarLong), Resolve: mapField("overdue"), Description: "Timers whose time has passed and have not fired."},
			"oldestDue": {Type: scalarTime, Resolve: mapField("oldest_due")},
		},
	})
	instanceStatus = gql.NewObject(gql.ObjectConfig{
		Name:        "InstanceStatus",
		Description: "The live instances of one build of the service.",
		Fields: gql.Fields{
			"generation": {Type: gql.NewNonNull(scalarLong), Resolve: mapField("generation")},
			"revision":   {Type: gql.String, Resolve: mapField("revision")},
			"count":      {Type: gql.NewNonNull(gql.Int), Resolve: mapField("count")},
			"seenAt":     {Type: gql.NewNonNull(scalarTime), Resolve: mapField("seen_at")},
			"newest":     {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("newest"), Description: "The newest generation alive: the one whose instances fire timers, run batches and fold projections."},
		},
	})
	scheduleStatus = gql.NewObject(gql.ObjectConfig{
		Name:        "ScheduleStatus",
		Description: "A command the service dispatches on an interval.",
		Fields: gql.Fields{
			"name":         {Type: gql.NewNonNull(gql.String), Resolve: mapField("name")},
			"command":      {Type: gql.NewNonNull(gql.String), Resolve: mapField("command")},
			"everySeconds": {Type: gql.NewNonNull(gql.Float), Resolve: mapField("every_seconds")},
			"nextAt":       {Type: scalarTime, Resolve: mapField("next_at"), Description: "When it is next due. While a tick is being run this shows a few minutes out."},
		},
	})
	serviceStatus = gql.NewObject(gql.ObjectConfig{
		Name:        "ServiceStatus",
		Description: "What one service's background work is doing now.",
		Fields: gql.Fields{
			"service":        {Type: gql.NewNonNull(gql.String), Resolve: mapField("service")},
			"latest":         {Type: gql.NewNonNull(scalarLong), Resolve: mapField("latest"), Description: "The newest event's position in the log."},
			"lease":          {Type: gql.NewNonNull(leaseStatus), Resolve: mapField("lease")},
			"instances":      {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(instanceStatus))), Resolve: mapField("instances"), Description: "The builds with a live instance, newest first. More than one line after a deploy means the old revision is still running."},
			"hereNewest":     {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("here_newest"), Description: "Whether the instance that answered is of the newest generation alive. False on an old revision's instance, which leaves timers, batches and projections to the newer build."},
			"behind":         {Type: gql.NewNonNull(gql.Int), Resolve: mapField("behind"), Description: "How many runners have events still to handle."},
			"stalled":        {Type: gql.NewNonNull(gql.Int), Resolve: mapField("stalled"), Description: "How many runners are halted."},
			"timers":         {Type: gql.NewNonNull(timerStatus), Resolve: mapField("timers")},
			"schedules":      {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(scheduleStatus))), Resolve: mapField("schedules"), Description: "The commands dispatched on an interval, and when each is next due."},
			"effectsRunning": {Type: gql.NewNonNull(scalarLong), Resolve: mapField("effects_running")},
			"effectsFailed":  {Type: gql.NewNonNull(scalarLong), Resolve: mapField("effects_failed")},
			"outboxDepth":    {Type: gql.NewNonNull(scalarLong), Resolve: mapField("outbox_depth"), Description: "Events not yet published to the bus."},
			"outboxOldest":   {Type: scalarTime, Resolve: mapField("outbox_oldest")},
			"deadLetters":    {Type: gql.NewNonNull(scalarLong), Resolve: mapField("dead_letters")},
			"plans":          {Type: gql.NewNonNull(scalarLong), Resolve: mapField("plans"), Description: "Planned commands waiting to be run."},
			"runners": {
				Type:        gql.NewNonNull(gql.NewList(gql.NewNonNull(runnerStatus))),
				Description: "Every projection and process. onlyBehind keeps those with events still to handle or halted.",
				Args:        gql.FieldConfigArgument{"onlyBehind": {Type: gql.Boolean}},
				Resolve: func(p gql.ResolveParams) (any, error) {
					src, _ := p.Source.(map[string]any)
					all, _ := src["runners"].([]any)
					if only, _ := p.Args["onlyBehind"].(bool); !only {
						return all, nil
					}
					out := []any{}
					for _, r := range all {
						m, _ := r.(map[string]any)
						lag, _ := m["lag"].(float64)
						if stalled, _ := m["stalled"].(bool); lag > 0 || stalled {
							out = append(out, r)
						}
					}
					return out, nil
				},
			},
		},
	})
)

// statusQuery adds serviceStatus over the composed services.
func (b *builder) statusQuery(services []*loom.Client) error {
	b.gated("query", gate{}, false, statusField)
	return b.addQuery(statusField, &gql.Field{
		Description: "What each service's background work is doing now: which instance leads its processes, how far each projection and process is behind the event log, and what is waiting — timers, effects, the outbox, dead letters, planned commands. Ask for runners(onlyBehind: true) to see only what needs attention.",
		Type:        gql.NewNonNull(gql.NewList(gql.NewNonNull(serviceStatus))),
		Resolve: func(p gql.ResolveParams) (any, error) {
			if err := decide(p.Context, Decision{Kind: "query", Field: statusField, Fields: selectedFields(p)}); err != nil {
				return nil, err
			}
			out := make([]any, 0, len(services))
			for _, cli := range services {
				st, err := cli.Status(p.Context)
				if err != nil {
					return nil, err
				}
				raw, err := json.Marshal(st)
				if err != nil {
					return nil, err
				}
				doc := map[string]any{}
				if err := json.Unmarshal(raw, &doc); err != nil {
					return nil, err
				}
				out = append(out, doc)
			}
			return out, nil
		},
	})
}
