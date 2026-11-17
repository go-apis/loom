package loom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/go-apis/loom/mesh"
)

// An agent step is a @loop aggregate asking an AI agent, run by Mesh,
// for an answer (ADR 0017). The schema declares it on the aggregate:
//
//	agent triage { kind: "triage", deadline: 30m }
//
// A look asks with AskAgent (or the generated Ask<Aggregate><Agent>).
// The ask is recorded (loom_agents, status requested) and one Mesh job
// of the declared kind is offered for it, under an idempotency key that
// names the instance, the agent and the attempt, so an offer made again
// — after a restart, or a lost answer — converges on the same job. The
// instance waits on it (AgentWait(name), owned by AgentOwner) until the
// declared deadline.
//
// Mesh reports the job's end on its signed hook (Client.MeshHook): the
// record becomes finished or failed with Mesh's outcome, the wait ends
// and the instance is looked at at once. An agent that does not answer
// by the deadline is expired: the record says so, its job is cancelled,
// and the look is told the wait expired. Either way the look reads how
// it came out with LookingAt(ctx).Agent(name).
//
// A look is level-triggered and may ask again on every look: an ask
// while the agent's request is still open does nothing. Once settled,
// asking again asks afresh — a new attempt and a new job.

// Mesh is how a service reaches Mesh: mesh.HTTPClient against the real
// API, mesh.Fake in tests.
type Mesh interface {
	OfferJob(ctx context.Context, o mesh.Offer) (mesh.Offered, error)
	CancelJob(ctx context.Context, jobID, reason string) error
}

// AgentDef is an agent step a @loop aggregate declares: its name, the
// Mesh job kind it is offered as, and how long the instance waits.
type AgentDef struct {
	Name     string
	Kind     string
	Deadline time.Duration
}

// AgentAsk is what a look says about one ask, beyond the declared kind
// and deadline. All of it is optional.
type AgentAsk struct {
	// Tenant is the Mesh tenant the job runs for; empty takes
	// Config.MeshTenant.
	Tenant string `json:"tenant,omitempty"`
	// Subject is what the job is about; empty is the instance (its
	// aggregate type and id).
	Subject mesh.Subject `json:"subject,omitempty"`
	// Key, Priority, Group, Code, Git and Access are the offer's own
	// (mesh.Offer).
	Key      string          `json:"key,omitempty"`
	Priority string          `json:"priority,omitempty"`
	Group    string          `json:"group,omitempty"`
	Code     json.RawMessage `json:"code,omitempty"`
	Git      json.RawMessage `json:"git,omitempty"`
	Access   json.RawMessage `json:"access,omitempty"`
	// Refs is the product's own JSON object; loom adds its own "loom"
	// key, which names the request on the job's hooks.
	Refs json.RawMessage `json:"refs,omitempty"`
}

// How an agent's request stands (AgentRecord.Status).
const (
	AgentRequested = "requested"
	AgentFinished  = "finished"
	AgentFailed    = "failed"
	AgentExpired   = "expired"
)

// AgentOwner owns the waits on agents: Mesh is to answer.
const AgentOwner = "mesh"

// AgentWait is the name of the wait an agent's request keeps on its
// instance.
func AgentWait(name string) string { return agentPrefix + name }

const agentPrefix = "agent:"

// AgentRecord is one agent's latest request for an instance, and how
// it stands.
type AgentRecord struct {
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	// Attempt counts the requests made: each ask after the last settled
	// is a new attempt, and a new job.
	Attempt int      `json:"attempt"`
	Ask     AgentAsk `json:"ask"`
	// JobID is the Mesh job, once offered.
	JobID    string    `json:"job_id,omitempty"`
	Status   string    `json:"status"`
	Deadline time.Time `json:"deadline"`
	AskedAt  time.Time `json:"asked_at"`
	// EndedAt and Outcome are a settled request's: Mesh's outcome for a
	// finished or failed one; none for an expired one.
	EndedAt *time.Time    `json:"ended_at,omitempty"`
	Outcome *mesh.Outcome `json:"outcome,omitempty"`
}

// Settled reports whether the agent has answered, failed or run out of
// time.
func (r AgentRecord) Settled() bool { return r.Status != AgentRequested }

// Result decodes a finished agent's result into v.
func (r AgentRecord) Result(v any) error {
	if r.Status != AgentFinished || r.Outcome == nil || len(r.Outcome.Result) == 0 {
		return fmt.Errorf("loom: agent %s has no result: it is %s", r.Name, r.Status)
	}
	return json.Unmarshal(r.Outcome.Result, v)
}

// IdempotencyKey is the key the request's job is offered under: one per
// instance, agent and attempt, so offering it again converges on the
// same job.
func (r AgentRecord) IdempotencyKey(service string) string {
	k := strings.Join([]string{"loom", service, r.Aggregate, r.Namespace, r.ID.String(), r.Name, strconv.Itoa(r.Attempt)}, "/")
	if len(k) <= mesh.MaxIdempotencyKey {
		return k
	}
	sum := sha256.Sum256([]byte(k))
	return "loom/" + hex.EncodeToString(sum[:])
}

// agentRef is what loom puts in a job's refs to find its request again.
type agentRef struct {
	Service   string    `json:"service"`
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	Agent     string    `json:"agent"`
	Attempt   int       `json:"attempt"`
}

// MeshOffer is the offer of the request's job for service.
func (r AgentRecord) MeshOffer(service string) (mesh.Offer, error) {
	refs := map[string]json.RawMessage{}
	if len(r.Ask.Refs) > 0 {
		if err := json.Unmarshal(r.Ask.Refs, &refs); err != nil {
			return mesh.Offer{}, fmt.Errorf("loom: agent %s: refs is not a JSON object: %w", r.Name, err)
		}
	}
	own, _ := json.Marshal(agentRef{Service: service, Aggregate: r.Aggregate, Namespace: r.Namespace, ID: r.ID, Agent: r.Name, Attempt: r.Attempt})
	refs["loom"] = own
	raw, err := json.Marshal(refs)
	if err != nil {
		return mesh.Offer{}, err
	}
	if len(raw) > mesh.MaxRefs {
		return mesh.Offer{}, fmt.Errorf("loom: agent %s: refs is %d bytes, more than Mesh's %d", r.Name, len(raw), mesh.MaxRefs)
	}
	sub := r.Ask.Subject
	if sub.Type == "" && sub.ID == "" {
		sub = mesh.Subject{Type: r.Aggregate, ID: r.ID.String()}
	}
	return mesh.Offer{
		Kind: r.Kind, Tenant: r.Ask.Tenant, Subject: sub, IdempotencyKey: r.IdempotencyKey(service),
		Key: r.Ask.Key, Priority: r.Ask.Priority, Group: r.Ask.Group,
		Code: r.Ask.Code, Git: r.Ask.Git, Access: r.Ask.Access, Refs: raw,
	}, nil
}

// agentAsked is one ask a look made.
type agentAsked struct {
	def AgentDef
	ask AgentAsk
}

// AskAgent asks the instance being looked at's declared agent of that
// name. An ask while its last request is still open does nothing; one
// after it settled asks afresh. It is an error outside a look the
// runtime is making, for an agent the aggregate does not declare, with
// no Mesh configured, and with no tenant to run the job for.
func AskAgent(ctx context.Context, name string, ask AgentAsk) error {
	s, ok := ctx.Value(lookSaidKey{}).(*lookSaid)
	if !ok {
		return errors.New("loom: AskAgent outside a look: an agent is asked by a @loop aggregate's look")
	}
	var def *AgentDef
	for i := range s.agentDefs {
		if s.agentDefs[i].Name == name {
			def = &s.agentDefs[i]
		}
	}
	if def == nil {
		return fmt.Errorf("loom: AskAgent: the aggregate declares no agent %q", name)
	}
	if !s.meshOn {
		return fmt.Errorf("loom: AskAgent %s: no Mesh is configured (Config.Mesh)", name)
	}
	if ask.Tenant == "" {
		ask.Tenant = s.meshTenant
	}
	if ask.Tenant == "" {
		return fmt.Errorf("loom: AskAgent %s: no tenant: set AgentAsk.Tenant or Config.MeshTenant", name)
	}
	s.agents = append(s.agents, agentAsked{def: *def, ask: ask})
	return nil
}

// Agent is the instance's latest request of that agent as the look
// finds it, if it has asked: open, or settled with its outcome.
func (l Looking) Agent(name string) (AgentRecord, bool) {
	for _, a := range l.Agents {
		if a.Name == name {
			return a, true
		}
	}
	return AgentRecord{}, false
}

// AgentAsked is one AskAgent a look made.
type AgentAsked struct {
	Def AgentDef
	Ask AgentAsk
}

// --- the runtime ---

const agentColumns = `aggregate_type, namespace, aggregate_id, name, kind, attempt, ask, job_id, status, deadline, asked_at, ended_at, outcome`

func scanAgent(row pgx.Row) (*AgentRecord, error) {
	var (
		r            AgentRecord
		ask, outcome []byte
	)
	err := row.Scan(&r.Aggregate, &r.Namespace, &r.ID, &r.Name, &r.Kind, &r.Attempt, &ask, &r.JobID, &r.Status, &r.Deadline, &r.AskedAt, &r.EndedAt, &outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(ask, &r.Ask)
	if len(outcome) > 0 && string(outcome) != "null" {
		r.Outcome = &mesh.Outcome{}
		_ = json.Unmarshal(outcome, r.Outcome)
	}
	return &r, nil
}

// Agent reads an instance's latest request of one agent; nil when it
// has never asked.
func (c *Client) Agent(ctx context.Context, aggregate, namespace string, id uuid.UUID, name string) (*AgentRecord, error) {
	return scanAgent(c.db.QueryRow(ctx, `SELECT `+agentColumns+` FROM loom_agents
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5`,
		c.reg.Service, aggregate, namespace, id, name))
}

// agentsOf is every agent request of one instance, for its look.
func (c *Client) agentsOf(ctx context.Context, aggregate, namespace string, id uuid.UUID) ([]AgentRecord, error) {
	rows, err := c.db.Query(ctx, `SELECT `+agentColumns+` FROM loom_agents
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 ORDER BY name`,
		c.reg.Service, aggregate, namespace, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AgentRecord
	for rows.Next() {
		r, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// openAgents records the asks a look made: a new request for each
// agent whose last one is settled (or that never asked), deadline from
// start. An ask while the last request is open changes nothing. It
// answers the waits the new requests start.
func (c *Client) openAgents(ctx context.Context, aggregate, namespace string, id uuid.UUID, asked []agentAsked, start time.Time) ([]Wait, error) {
	var waits []Wait
	for _, a := range asked {
		ask, err := json.Marshal(a.ask)
		if err != nil {
			return nil, err
		}
		deadline := start.Add(a.def.Deadline).UTC()
		var attempt int
		err = c.db.QueryRow(ctx, `
			INSERT INTO loom_agents (service, aggregate_type, namespace, aggregate_id, name, kind, attempt, ask, deadline, asked_at)
			VALUES ($1,$2,$3,$4,$5,$6,1,$7,$8,$9)
			ON CONFLICT (service, aggregate_type, namespace, aggregate_id, name) DO UPDATE
			SET kind = EXCLUDED.kind, attempt = loom_agents.attempt + 1, ask = EXCLUDED.ask, deadline = EXCLUDED.deadline,
			    asked_at = EXCLUDED.asked_at, status = 'requested', job_id = '', ended_at = NULL, outcome = NULL
			WHERE loom_agents.status <> 'requested'
			RETURNING attempt`,
			c.reg.Service, aggregate, namespace, id, a.def.Name, a.def.Kind, ask, deadline, start).Scan(&attempt)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // still open: the ask is already in hand
		}
		if err != nil {
			return nil, err
		}
		waits = append(waits, Wait{Name: AgentWait(a.def.Name), Owner: AgentOwner, Deadline: deadline})
	}
	return waits, nil
}

// offerAgents offers a job for each of the instance's open requests
// that has none yet. The idempotency key is the request's, so an offer
// made twice — the first's answer lost, or the process restarted
// between recording and offering — is the same job. A failed offer is
// logged and tried again at the next look.
func (c *Client) offerAgents(ctx context.Context, aggregate, namespace string, id uuid.UUID) {
	if c.mesh == nil {
		return
	}
	rows, err := c.db.Query(ctx, `SELECT `+agentColumns+` FROM loom_agents
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND status='requested' AND job_id=''`,
		c.reg.Service, aggregate, namespace, id)
	if err != nil {
		c.log.ErrorContext(ctx, "reading agent requests to offer failed", "error", err)
		return
	}
	var open []AgentRecord
	for rows.Next() {
		r, err := scanAgent(rows)
		if err != nil {
			rows.Close()
			c.log.ErrorContext(ctx, "reading agent requests to offer failed", "error", err)
			return
		}
		open = append(open, *r)
	}
	rows.Close()
	for _, r := range open {
		o, err := r.MeshOffer(c.reg.Service)
		if err == nil {
			var got mesh.Offered
			if got, err = c.mesh.OfferJob(ctx, o); err == nil {
				_, err = c.db.Exec(ctx, `UPDATE loom_agents SET job_id=$7
					WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5 AND attempt=$6 AND job_id=''`,
					c.reg.Service, aggregate, namespace, id, r.Name, r.Attempt, got.JobID)
			}
		}
		if err != nil {
			c.log.ErrorContext(ctx, "offering an agent's job to Mesh failed: it is offered again at the next look", "aggregate", aggregate, "namespace", namespace, "id", id, "agent", r.Name, "error", err)
		}
	}
}

// expireAgents marks each open request whose wait has expired as
// expired, before the look that is told so, and cancels its job: an
// answer that comes later is not read.
func (c *Client) expireAgents(ctx context.Context, aggregate, namespace string, id uuid.UUID, expired []Wait) error {
	for _, w := range expired {
		name, ok := strings.CutPrefix(w.Name, agentPrefix)
		if !ok {
			continue
		}
		var jobID string
		err := c.db.QueryRow(ctx, `
			UPDATE loom_agents SET status='expired', ended_at=clock_timestamp()
			WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5
			  AND status='requested' AND deadline <= clock_timestamp()
			RETURNING job_id`,
			c.reg.Service, aggregate, namespace, id, name).Scan(&jobID)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if jobID != "" && c.mesh != nil {
			if err := c.mesh.CancelJob(ctx, jobID, "loom: the agent's deadline passed"); err != nil {
				c.log.WarnContext(ctx, "cancelling an expired agent's job failed", "agent", name, "job", jobID, "error", err)
			}
		}
	}
	return nil
}

// settleAgent records a job's end on the request it was offered for,
// if that request is still open: finished, or failed for any other
// end. The wait ends and the instance is looked at at once. It answers
// whether it settled anything; a hook heard twice settles once.
func (c *Client) settleAgent(ctx context.Context, ref agentRef, jobID string, o mesh.Outcome) (bool, error) {
	status := AgentFailed
	if o.End == mesh.EndFinished {
		status = AgentFinished
	}
	outcome, err := json.Marshal(o)
	if err != nil {
		return false, err
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE loom_agents SET status=$7, outcome=$8, ended_at=clock_timestamp(), job_id=CASE WHEN job_id='' THEN $9 ELSE job_id END
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5 AND attempt=$6 AND status='requested'`,
		c.reg.Service, ref.Aggregate, ref.Namespace, ref.ID, ref.Agent, ref.Attempt, status, outcome, jobID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	// an answer is news for the loop: look now
	if _, err := tx.Exec(ctx, `
		UPDATE loom_loop_due SET event_at = clock_timestamp(), next_look = LEAST(next_look, clock_timestamp())
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, ref.Aggregate, ref.Namespace, ref.ID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, c.EndWait(ctx, ref.Aggregate, ref.Namespace, ref.ID, AgentWait(ref.Agent))
}

// agentOpen reports whether the request a job was offered for is still
// open.
func (c *Client) agentOpen(ctx context.Context, ref agentRef) (bool, error) {
	var open bool
	err := c.db.QueryRow(ctx, `SELECT status='requested' FROM loom_agents
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5 AND attempt=$6`,
		c.reg.Service, ref.Aggregate, ref.Namespace, ref.ID, ref.Agent, ref.Attempt).Scan(&open)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return open, err
}

// refOf reads loom's ref from a job's refs; ok is false for a job loom
// did not offer.
func refOf(refs json.RawMessage) (agentRef, bool) {
	var outer struct {
		Loom *agentRef `json:"loom"`
	}
	if json.Unmarshal(refs, &outer) != nil || outer.Loom == nil || outer.Loom.Service == "" || outer.Loom.Agent == "" {
		return agentRef{}, false
	}
	return *outer.Loom, true
}

// MeshHookService is the loom service a Mesh hook body is for, read from
// the job's refs; empty for a job no loom service offered. A gateway
// routing hooks among services reads it (graphql.MeshHooks).
func MeshHookService(body []byte) string {
	var b struct {
		Job struct {
			Refs json.RawMessage `json:"refs"`
		} `json:"job"`
	}
	if json.Unmarshal(body, &b) != nil {
		return ""
	}
	ref, _ := refOf(b.Job.Refs)
	return ref.Service
}

// MeshHook receives Mesh's job hooks for the service's agents, at the
// URL its catalogue names for hooks.jobs: mount it where the gateway
// faces Mesh, e.g. mux.Handle("POST /hooks/mesh", cli.MeshHook()).
//
// A body not signed under Config.MeshHookSecret is 401 and one that is
// not a feed entry 400 — both final for Mesh. A job.ended entry for an
// agent's open request settles it (finished, or failed for any other
// end); every other entry, and one for a request already settled or not
// this service's, is answered 200 and changes nothing. A job.confirm
// ask is answered run while the request is open, cancel once it is not.
// A failure to record is 500, which Mesh retries.
func (c *Client) MeshHook() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "post a hook", http.StatusMethodNotAllowed)
			return
		}
		if len(c.meshSecret) == 0 {
			http.Error(w, "no Mesh hook secret is configured (Config.MeshHookSecret)", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mesh.MaxHookBody+1))
		if err != nil || len(body) > mesh.MaxHookBody {
			http.Error(w, "reading the body", http.StatusBadRequest)
			return
		}
		if !mesh.Verify(c.meshSecret, body, r.Header.Get(mesh.HeaderSignature)) {
			http.Error(w, "X-Mesh-Signature is not this product's", http.StatusUnauthorized)
			return
		}
		ctx := r.Context()
		var probe struct {
			Seq   int64  `json:"seq"`
			Event string `json:"event"`
		}
		if err := json.Unmarshal(body, &probe); err == nil && probe.Seq == 0 && probe.Event == mesh.EventJobConfirm {
			var ask mesh.ConfirmAsk
			if err := json.Unmarshal(body, &ask); err != nil {
				http.Error(w, "not a job.confirm ask", http.StatusBadRequest)
				return
			}
			reply := mesh.ConfirmReply{Answer: "run"}
			if ref, ok := refOf(ask.Job.Refs); ok && ref.Service == c.reg.Service {
				if open, err := c.agentOpen(ctx, ref); err == nil && !open {
					reply = mesh.ConfirmReply{Answer: "cancel", Reason: "loom: the agent's request is no longer open"}
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		var change mesh.JobChange
		if err := json.Unmarshal(body, &change); err != nil || change.Seq <= 0 || change.Event == "" {
			http.Error(w, "not a feed entry with a seq and an event", http.StatusBadRequest)
			return
		}
		if ev := r.Header.Get(mesh.HeaderEvent); ev != "" && ev != change.Event {
			http.Error(w, "X-Mesh-Event is not the body's event", http.StatusBadRequest)
			return
		}
		ref, ok := refOf(change.Job.Refs)
		if !ok || ref.Service != c.reg.Service || change.Event != mesh.EventJobEnded || change.Outcome == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		if _, err := c.settleAgent(ctx, ref, change.Job.ID, *change.Outcome); err != nil {
			c.log.ErrorContext(ctx, "recording an agent's outcome failed: Mesh retries the hook", "agent", ref.Agent, "job", change.Job.ID, "error", err)
			http.Error(w, "recording the outcome failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
