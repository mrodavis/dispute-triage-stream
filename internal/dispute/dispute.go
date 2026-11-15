// Package dispute holds the domain types for card disputes and the deterministic triage policy.
// Everything here is pure: no I/O, no clock reads, no model calls.
package dispute

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Event is a dispute opened by a cardmember, as published on the disputes.received topic.
// The topic is keyed by AccountID so one account's disputes stay ordered on one partition.
type Event struct {
	DisputeID       string    `json:"dispute_id"`
	AccountID       string    `json:"account_id"`
	CardLast4       string    `json:"card_last4"`
	Merchant        string    `json:"merchant"`
	MCC             string    `json:"mcc"`
	AmountCents     int64     `json:"amount_cents"`
	Currency        string    `json:"currency"`
	TransactionDate string    `json:"transaction_date"`
	StatementDate   string    `json:"statement_date"`
	OpenedAt        time.Time `json:"opened_at"`
	Channel         string    `json:"channel"`
	// Statement is the cardmember's own words. Untrusted input.
	Statement string `json:"cardmember_statement"`
}

const DateLayout = "2006-01-02"

// Validate rejects events that can never be processed. These go to the dead-letter topic, not retried.
func (e Event) Validate() error {
	var problems []string
	if e.DisputeID == "" {
		problems = append(problems, "dispute_id is required")
	}
	if e.AccountID == "" {
		problems = append(problems, "account_id is required")
	}
	if e.AmountCents <= 0 {
		problems = append(problems, "amount_cents must be positive")
	}
	if e.Currency != "USD" {
		problems = append(problems, "only USD is supported")
	}
	if _, err := time.Parse(DateLayout, e.StatementDate); err != nil {
		problems = append(problems, "statement_date must be YYYY-MM-DD")
	}
	if e.OpenedAt.IsZero() {
		problems = append(problems, "opened_at is required")
	}
	if len(e.Statement) > 4000 {
		problems = append(problems, "cardmember_statement is too long")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// Category is what the model decides the dispute is about.
type Category string

const (
	Fraud                Category = "fraud_not_recognized"
	NotReceived          Category = "goods_not_received"
	NotAsDescribed       Category = "not_as_described"
	DuplicateCharge      Category = "duplicate_charge"
	IncorrectAmount      Category = "incorrect_amount"
	CanceledSubscription Category = "canceled_subscription"
	CreditNotProcessed   Category = "credit_not_processed"
	Other                Category = "other"
)

var Categories = []Category{Fraud, NotReceived, NotAsDescribed, DuplicateCharge, IncorrectAmount, CanceledSubscription, CreditNotProcessed, Other}

var EvidenceTypes = []string{"receipt", "tracking_number", "cancellation_confirmation", "merchant_correspondence", "photos", "refund_confirmation"}

// Classification is the model's structured output. It never decides money; Decide does.
type Classification struct {
	Category           Category `json:"category"`
	Confidence         float64  `json:"confidence"`
	Summary            string   `json:"summary"`
	EvidenceToRequest  []string `json:"evidence_to_request"`
	InjectionSuspected bool     `json:"injection_suspected"`
}

// Validate is the schema check on model output: enums, ranges and lengths.
func (c Classification) Validate() error {
	var problems []string
	if !contains(Categories, c.Category) {
		problems = append(problems, fmt.Sprintf("category %q is not one of %v", c.Category, Categories))
	}
	if c.Confidence < 0 || c.Confidence > 1 {
		problems = append(problems, "confidence must be between 0 and 1")
	}
	if strings.TrimSpace(c.Summary) == "" || len(c.Summary) > 300 {
		problems = append(problems, "summary must be 1 to 300 characters")
	}
	for _, ev := range c.EvidenceToRequest {
		if !contains(EvidenceTypes, ev) {
			problems = append(problems, fmt.Sprintf("evidence %q is not one of %v", ev, EvidenceTypes))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func contains[T comparable](list []T, v T) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
