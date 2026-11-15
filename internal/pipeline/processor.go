// Package pipeline turns dispute events into triage decisions and moves them through Kafka.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mrodavis/dispute-triage-stream/internal/classify"
	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
	"github.com/mrodavis/dispute-triage-stream/internal/store"
)

// DeadLetter is published on the dead-letter topic with the original payload, so nothing is lost.
type DeadLetter struct {
	Error    string          `json:"error"`
	Attempts int             `json:"attempts"`
	Payload  json.RawMessage `json:"payload"`
	FailedAt time.Time       `json:"failed_at"`
}

// Outcome of one record: exactly one of Decision or DeadLetter is set.
type Outcome struct {
	Decision   *dispute.Decision
	DeadLetter *DeadLetter
	// Duplicate is true when the dispute was already decided (a redelivery); the stored decision is re-emitted.
	Duplicate bool
}

type Processor struct {
	Classifier  classify.Classifier
	Store       store.Store
	Policy      dispute.Policy
	MaxAttempts int
	Backoff     time.Duration
	Now         func() time.Time
	Sleep       func(context.Context, time.Duration) error
}

func NewProcessor(c classify.Classifier, s store.Store) *Processor {
	return &Processor{
		Classifier:  c,
		Store:       s,
		Policy:      dispute.DefaultPolicy,
		MaxAttempts: 3,
		Backoff:     500 * time.Millisecond,
		Now:         time.Now,
		Sleep: func(ctx context.Context, d time.Duration) error {
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

// Process handles one event. It returns an error only when the context is cancelled, in which case
// the offset must not be committed and the record will be redelivered.
func (p *Processor) Process(ctx context.Context, payload []byte) (Outcome, error) {
	dead := func(msg string, attempts int) (Outcome, error) {
		return Outcome{DeadLetter: &DeadLetter{Error: msg, Attempts: attempts, Payload: safeJSON(payload), FailedAt: p.Now().UTC()}}, nil
	}

	var e dispute.Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return dead("invalid JSON: "+err.Error(), 0)
	}
	if err := e.Validate(); err != nil {
		return dead("invalid event: "+err.Error(), 0)
	}

	// Idempotency: Kafka delivers at least once, so a redelivered dispute reuses its first decision
	// instead of calling the model again or making a second, possibly different, decision.
	if d, ok := p.Store.Get(e.DisputeID); ok {
		return Outcome{Decision: &d, Duplicate: true}, nil
	}

	var (
		c   dispute.Classification
		err error
	)
	attempt := 0
	for attempt = 1; attempt <= p.MaxAttempts; attempt++ {
		c, err = p.Classifier.Classify(ctx, e)
		if err == nil || !classify.IsTransient(err) {
			break
		}
		if attempt < p.MaxAttempts {
			if serr := p.Sleep(ctx, p.Backoff*time.Duration(1<<(attempt-1))); serr != nil {
				return Outcome{}, serr
			}
		}
	}
	if ctx.Err() != nil {
		return Outcome{}, ctx.Err()
	}
	if err != nil {
		return dead(fmt.Sprintf("classification failed: %v", err), min(attempt, p.MaxAttempts))
	}

	history := dispute.History{DisputesLast90Days: p.Store.DisputesSince(e.AccountID, e.OpenedAt.AddDate(0, 0, -90))}
	d := p.Policy.Decide(e, c, history, p.Classifier.Model(), p.Classifier.PromptHash(), p.Now())
	stored, created := p.Store.Save(d)
	return Outcome{Decision: &stored, Duplicate: !created}, nil
}

func safeJSON(b []byte) json.RawMessage {
	if json.Valid(b) {
		return b
	}
	quoted, _ := json.Marshal(string(b))
	return quoted
}
