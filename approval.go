package loom

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// An approval is a wait on a person's yes or no. It belongs to one
// instance of a @loop aggregate and says four things: what is asked,
// the role that may answer, the deadline, and what happens at the
// deadline if nobody has answered. It is kept as a record
// (loom_approvals), so a product can list what is waiting on whom, and
// as a wait on its instance, so the loop is called when it is settled:
//
//   - answered in time (Client.AnswerApproval), by someone holding the
//     role: the record says who, yes or no, and why, and the instance is
//     looked at at once;
//   - not answered by the deadline: the record takes its declared
//     deadline action — approved, rejected or expired, by "deadline" —
//     and the instance is looked at, told the wait has expired.
//
// Either way the look reads how it came out with LookingAt(ctx).Approval
// and decides the next step. A look asks with AskApproval; code outside
// a look uses Client.AskApproval.

// Approval is what is asked, of whom, and until when.
type Approval struct {
	// Name is unique among its instance's approvals.
	Name string `json:"name"`
	// What says what is being approved, for whoever is asked.
	What string `json:"what"`
	// Role is the role that may answer.
	Role string `json:"role"`
	// Deadline is when the approval stops waiting for an answer.
	Deadline time.Time `json:"deadline"`
	// AtDeadline is what an unanswered approval becomes: one of the
	// AtDeadline constants.
	AtDeadline string `json:"at_deadline"`
}

// What an approval nobody answered becomes at its deadline
// (Approval.AtDeadline).
const (
	AtDeadlineApprove = "approve"
	AtDeadlineReject  = "reject"
	AtDeadlineExpire  = "expire"
)

// How an approval stands (ApprovalRecord.Status).
const (
	ApprovalOpen     = "open"
	ApprovalApproved = "approved"
	ApprovalRejected = "rejected"
	ApprovalExpired  = "expired"
)

// ApprovalByDeadline is who answered an approval that took its deadline
// action (ApprovalRecord.AnsweredBy).
const ApprovalByDeadline = "deadline"

// ApprovalRecord is an approval and how it stands.
type ApprovalRecord struct {
	Approval
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	Status    string    `json:"status"`
	// AnsweredBy is the actor who answered, or "deadline".
	AnsweredBy string     `json:"answered_by,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	AskedAt    time.Time  `json:"asked_at"`
	AnsweredAt *time.Time `json:"answered_at,omitempty"`
}

// Settled reports whether the approval has been answered or has taken
// its deadline action.
func (r ApprovalRecord) Settled() bool { return r.Status != ApprovalOpen }

// ErrNotYourApproval is an answer from someone who does not hold the
// approval's role.
var ErrNotYourApproval = errors.New("loom: this approval is not yours to answer: it asks for a role you do not hold")

func (a Approval) check() error {
	switch {
	case a.Name == "":
		return errors.New("loom: an approval needs a name")
	case a.What == "":
		return fmt.Errorf("loom: approval %q does not say what is asked", a.Name)
	case a.Role == "":
		return fmt.Errorf("loom: approval %q names no role: say who may answer", a.Name)
	case a.Deadline.IsZero():
		return fmt.Errorf("loom: approval %q has no deadline", a.Name)
	}
	switch a.AtDeadline {
	case AtDeadlineApprove, AtDeadlineReject, AtDeadlineExpire:
		return nil
	}
	return fmt.Errorf("loom: approval %q: at the deadline it is approve, reject or expire, not %q", a.Name, a.AtDeadline)
}

// approvalWait is the name of the wait an approval keeps on its instance.
func approvalWait(name string) string { return "approval:" + name }

func (a Approval) wait() Wait {
	return Wait{Name: approvalWait(a.Name), Owner: "role:" + a.Role, Deadline: a.Deadline.UTC()}
}

// AskApproval asks for an approval for the instance being looked at. An
// approval of the same name that is still open is replaced; one already
// settled is asked afresh. It is an error outside a look the runtime is
// making, and for an approval that leaves out what, who, when or what
// happens at the deadline.
func AskApproval(ctx context.Context, a Approval) error {
	s, ok := ctx.Value(lookSaidKey{}).(*lookSaid)
	if !ok {
		return errors.New("loom: AskApproval outside a look: use Client.AskApproval")
	}
	if err := a.check(); err != nil {
		return err
	}
	a.Deadline = a.Deadline.UTC()
	s.approvals = append(s.approvals, a)
	s.waits = append(s.waits, a.wait())
	return nil
}

// Approval is the instance's approval of that name as the look finds
// it, if there is one: open, or settled with who answered and why.
func (l Looking) Approval(name string) (ApprovalRecord, bool) {
	for _, a := range l.Approvals {
		if a.Name == name {
			return a, true
		}
	}
	return ApprovalRecord{}, false
}

// AskApproval asks for an approval for an instance of a @loop
// aggregate, from outside a look.
func (c *Client) AskApproval(ctx context.Context, aggregate, namespace string, id uuid.UUID, a Approval) error {
	if err := a.check(); err != nil {
		return err
	}
	if c.looping(aggregate) == nil {
		return fmt.Errorf("loom: %s declares no @loop: an approval belongs to a loop", aggregate)
	}
	a.Deadline = a.Deadline.UTC()
	if err := c.writeApprovals(ctx, c.db, aggregate, namespace, id, []Approval{a}); err != nil {
		return err
	}
	return c.Wait(ctx, aggregate, namespace, id, a.wait())
}

// writeApprovals records approvals as open.
func (c *Client) writeApprovals(ctx context.Context, q dbConn, aggregate, namespace string, id uuid.UUID, asked []Approval) error {
	for _, a := range asked {
		if _, err := q.Exec(ctx, `
			INSERT INTO loom_approvals (service, aggregate_type, namespace, aggregate_id, name, what, role, deadline, at_deadline)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (service, aggregate_type, namespace, aggregate_id, name) DO UPDATE
			SET what = EXCLUDED.what, role = EXCLUDED.role, deadline = EXCLUDED.deadline, at_deadline = EXCLUDED.at_deadline,
			    status = 'open', answered_by = '', reason = '', asked_at = now(), answered_at = NULL`,
			c.reg.Service, aggregate, namespace, id, a.Name, a.What, a.Role, a.Deadline, a.AtDeadline); err != nil {
			return err
		}
	}
	return nil
}

// ApprovalAnswer is a yes or a no.
type ApprovalAnswer struct {
	Approve bool
	// Reason says why; optional for a yes, and worth giving for a no.
	Reason string
	// Roles is the roles the answerer holds. The approval's role must
	// be among them.
	Roles []string
}

// AnswerApproval answers an open approval. The answerer (the context's
// Metadata.Actor) must hold the approval's role (ErrNotYourApproval). An
// approval that is not there, already settled, or past its deadline is
// refused: the deadline's action stands. On an answer the approval's
// wait ends and its instance is looked at at once.
func (c *Client) AnswerApproval(ctx context.Context, aggregate, namespace string, id uuid.UUID, name string, answer ApprovalAnswer) (*ApprovalRecord, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rec, err := c.approval(ctx, tx, aggregate, namespace, id, name, true)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, fmt.Errorf("loom: %s %s has no approval %q", aggregate, id, name)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	answered, err := rec.Answer(MetaFrom(ctx).Actor, answer, now)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE loom_approvals SET status=$6, answered_by=$7, reason=$8, answered_at=$9
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5`,
		c.reg.Service, aggregate, namespace, id, name, answered.Status, answered.AnsweredBy, answered.Reason, now); err != nil {
		return nil, err
	}
	// an answer is news for the loop: look now
	if _, err := tx.Exec(ctx, `
		UPDATE loom_loop_due SET event_at = clock_timestamp(), next_look = LEAST(next_look, clock_timestamp())
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, aggregate, namespace, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if err := c.EndWait(ctx, aggregate, namespace, id, approvalWait(name)); err != nil {
		return nil, err
	}
	return &answered, nil
}

// settleAtDeadline gives each approval whose wait has expired its
// declared deadline action, if it is still open.
func (c *Client) settleAtDeadline(ctx context.Context, aggregate, namespace string, id uuid.UUID, at time.Time, expired []Wait) error {
	for _, w := range expired {
		name, ok := approvalOfWait(w.Name)
		if !ok {
			continue
		}
		rec, err := c.approval(ctx, c.db, aggregate, namespace, id, name, false)
		if err != nil {
			return err
		}
		if rec == nil {
			continue
		}
		settled, changed := rec.PastDeadline(at)
		if !changed {
			continue
		}
		// an answer that landed since the read stands
		if _, err := c.db.Exec(ctx, `
			UPDATE loom_approvals SET status=$6, answered_by=$7, answered_at=$8
			WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5
			  AND status = 'open'`,
			c.reg.Service, aggregate, namespace, id, name, settled.Status, settled.AnsweredBy, at); err != nil {
			return err
		}
	}
	return nil
}

const approvalColumns = `aggregate_type, namespace, aggregate_id, name, what, role, deadline, at_deadline, status, answered_by, reason, asked_at, answered_at`

func scanApproval(row pgx.Row) (*ApprovalRecord, error) {
	var r ApprovalRecord
	err := row.Scan(&r.Aggregate, &r.Namespace, &r.ID, &r.Name, &r.What, &r.Role, &r.Deadline, &r.AtDeadline, &r.Status, &r.AnsweredBy, &r.Reason, &r.AskedAt, &r.AnsweredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *Client) approval(ctx context.Context, q querier, aggregate, namespace string, id uuid.UUID, name string, lock bool) (*ApprovalRecord, error) {
	sql := `SELECT ` + approvalColumns + ` FROM loom_approvals
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4 AND name=$5`
	if lock {
		sql += ` FOR UPDATE`
	}
	return scanApproval(q.QueryRow(ctx, sql, c.reg.Service, aggregate, namespace, id, name))
}

// Approval reads one approval of an instance; nil when it has none of
// that name.
func (c *Client) Approval(ctx context.Context, aggregate, namespace string, id uuid.UUID, name string) (*ApprovalRecord, error) {
	return c.approval(ctx, c.db, aggregate, namespace, id, name, false)
}

// ApprovalQuery says which approvals to list.
type ApprovalQuery struct {
	// Namespace is required.
	Namespace string
	// Status keeps one status; empty keeps the open ones.
	Status string
	// Role keeps the approvals one role may answer; empty keeps all.
	Role string
	// Aggregate and ID keep one aggregate type's, or one instance's.
	Aggregate string
	ID        uuid.UUID
	Limit     int
}

// Approvals lists approvals, those whose deadline is nearest first: what
// is waiting on whom.
func (c *Client) Approvals(ctx context.Context, q ApprovalQuery) ([]ApprovalRecord, error) {
	if q.Namespace == "" {
		return nil, errors.New("loom: approvals are listed for a namespace")
	}
	if q.Status == "" {
		q.Status = ApprovalOpen
	}
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	rows, err := c.db.Query(ctx, `
		SELECT `+approvalColumns+` FROM loom_approvals
		WHERE service=$1 AND namespace=$2 AND status=$3
		  AND ($4 = '' OR role = $4) AND ($5 = '' OR aggregate_type = $5)
		  AND ($6::uuid IS NULL OR aggregate_id = $6)
		ORDER BY deadline, aggregate_id, name
		LIMIT $7`,
		c.reg.Service, q.Namespace, q.Status, q.Role, q.Aggregate, nullUUID(q.ID), q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ApprovalRecord{}
	for rows.Next() {
		r, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

// approvalsOf is every approval of one instance, for its look.
func (c *Client) approvalsOf(ctx context.Context, aggregate, namespace string, id uuid.UUID) ([]ApprovalRecord, error) {
	rows, err := c.db.Query(ctx, `
		SELECT `+approvalColumns+` FROM loom_approvals
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
		ORDER BY name`,
		c.reg.Service, aggregate, namespace, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ApprovalRecord
	for rows.Next() {
		r, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}
