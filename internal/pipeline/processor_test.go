package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrodavis/dispute-triage-stream/internal/classify"
	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
	"github.com/mrodavis/dispute-triage-stream/internal/store"
)

const goodEvent = `{"dispute_id":"dsp_1","account_id":"acct_1","merchant":"Northwind","mcc":"5651","amount_cents":8999,"currency":"USD","transaction_date":"2026-10-28","statement_date":"2026-11-02","opened_at":"2026-11-16T14:00:00Z","channel":"app","cardmember_statement":"The boots never arrived."}`

// scripted returns the given errors in order, then delegates to the mock.
type scripted struct {
	errs  []error
	calls int
}

func (s *scripted) Model() string      { return "scripted" }
func (s *scripted) PromptHash() string { return "hash" }
func (s *scripted) Classify(ctx context.Context, e dispute.Event) (dispute.Classification, error) {
	s.calls++
	if s.calls <= len(s.errs) {
		return dispute.Classification{}, s.errs[s.calls-1]
	}
	return classify.Mock{}.Classify(ctx, e)
}

func newTestProcessor(c classify.Classifier) *Processor {
	p := NewProcessor(c, store.NewMemory())
	p.Now = func() time.Time { return time.Date(2026, 11, 16, 15, 0, 0, 0, time.UTC) }
	p.Sleep = func(context.Context, time.Duration) error { return nil }
	return p
}

func TestRedeliveryReusesTheFirstDecision(t *testing.T) {
	c := &scripted{}
	p := newTestProcessor(c)
	first, _ := p.Process(context.Background(), []byte(goodEvent))
	again, _ := p.Process(context.Background(), []byte(goodEvent))
	if c.calls != 1 {
		t.Fatalf("model called %d times for one dispute", c.calls)
	}
	if !again.Duplicate || again.Decision.DecidedAt != first.Decision.DecidedAt || first.Decision.Route != dispute.RouteChargeback {
		t.Fatalf("redelivery did not reuse the stored decision: %+v", again)
	}
}

func TestTransientErrorsAreRetriedWithBackoff(t *testing.T) {
	c := &scripted{errs: []error{classify.TransientError{Err: errors.New("529")}, classify.TransientError{Err: errors.New("429")}}}
	p := newTestProcessor(c)
	var waits []time.Duration
	p.Sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
	o, err := p.Process(context.Background(), []byte(goodEvent))
	if err != nil || o.Decision == nil || c.calls != 3 {
		t.Fatalf("got %+v, %v after %d calls", o, err, c.calls)
	}
	if len(waits) != 2 || waits[1] != 2*waits[0] {
		t.Fatalf("expected exponential backoff, got %v", waits)
	}
}

func TestExhaustedRetriesGoToTheDeadLetterTopic(t *testing.T) {
	transient := classify.TransientError{Err: errors.New("overloaded")}
	c := &scripted{errs: []error{transient, transient, transient}}
	o, _ := newTestProcessor(c).Process(context.Background(), []byte(goodEvent))
	if o.DeadLetter == nil || o.DeadLetter.Attempts != 3 || string(o.DeadLetter.Payload) != goodEvent {
		t.Fatalf("got %+v", o)
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	c := &scripted{errs: []error{errors.New("400 bad request")}}
	o, _ := newTestProcessor(c).Process(context.Background(), []byte(goodEvent))
	if o.DeadLetter == nil || c.calls != 1 {
		t.Fatalf("got %+v after %d calls", o, c.calls)
	}
}

func TestMalformedEventsAreDeadLettered(t *testing.T) {
	for _, payload := range []string{`not json`, `{"dispute_id":"x","amount_cents":0}`} {
		o, _ := newTestProcessor(classify.Mock{}).Process(context.Background(), []byte(payload))
		if o.DeadLetter == nil {
			t.Fatalf("%s: expected a dead letter", payload)
		}
	}
}

func TestCancellationReturnsAnErrorSoNothingIsCommitted(t *testing.T) {
	c := &scripted{errs: []error{classify.TransientError{Err: errors.New("429")}}}
	p := newTestProcessor(c)
	ctx, cancel := context.WithCancel(context.Background())
	p.Sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	if _, err := p.Process(ctx, []byte(goodEvent)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
