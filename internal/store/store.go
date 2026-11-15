// Package store keeps decisions (for idempotency and the review API) and per-account dispute history.
// In-memory here; a production version uses Postgres with the same interface.
package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
)

var (
	ErrNotFound      = errors.New("dispute not found")
	ErrNotReviewable = errors.New("dispute is not awaiting review")
)

type Store interface {
	Get(disputeID string) (dispute.Decision, bool)
	// Save stores the decision once. A second Save for the same dispute keeps the first.
	Save(d dispute.Decision) (stored dispute.Decision, created bool)
	DisputesSince(accountID string, since time.Time) int
	ReviewQueue(route dispute.Route, limit int) []dispute.Decision
	Resolve(disputeID string, r dispute.Resolution) (dispute.Decision, error)
}

type Memory struct {
	mu        sync.Mutex
	decisions map[string]dispute.Decision
	opened    map[string][]time.Time
}

func NewMemory() *Memory {
	return &Memory{decisions: map[string]dispute.Decision{}, opened: map[string][]time.Time{}}
}

func (m *Memory) Get(id string) (dispute.Decision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.decisions[id]
	return d, ok
}

func (m *Memory) Save(d dispute.Decision) (dispute.Decision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.decisions[d.DisputeID]; ok {
		return existing, false
	}
	m.decisions[d.DisputeID] = d
	m.opened[d.AccountID] = append(m.opened[d.AccountID], d.OpenedAt)
	return d, true
}

func (m *Memory) DisputesSince(accountID string, since time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, t := range m.opened[accountID] {
		if !t.Before(since) {
			n++
		}
	}
	return n
}

// ReviewQueue lists unresolved decisions on human routes, oldest first. Empty route means review and fraud.
func (m *Memory) ReviewQueue(route dispute.Route, limit int) []dispute.Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []dispute.Decision
	for _, d := range m.decisions {
		human := d.Route == dispute.RouteReview || d.Route == dispute.RouteFraud
		if d.Resolution == nil && human && (route == "" || d.Route == route) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.Before(out[j].OpenedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (m *Memory) Resolve(id string, r dispute.Resolution) (dispute.Decision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.decisions[id]
	if !ok {
		return d, ErrNotFound
	}
	if d.Resolution != nil || (d.Route != dispute.RouteReview && d.Route != dispute.RouteFraud) {
		return d, ErrNotReviewable
	}
	d.Resolution = &r
	m.decisions[id] = d
	return d, nil
}
