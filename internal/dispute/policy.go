package dispute

import "time"

// Route is what happens to a dispute next.
type Route string

const (
	RouteAutoCredit Route = "auto_credit" // small amount: credit the cardmember, no investigation
	RouteChargeback Route = "chargeback"  // open a chargeback with the merchant
	RouteReview     Route = "review"      // a dispute analyst decides
	RouteFraud      Route = "fraud"       // transfer to the fraud team; the card is closed
)

// Decision is published on disputes.triaged and served by the gRPC review API.
type Decision struct {
	DisputeID           string      `json:"dispute_id"`
	AccountID           string      `json:"account_id"`
	Route               Route       `json:"route"`
	Reasons             []string    `json:"reasons"`
	ReasonCode          string      `json:"reason_code,omitempty"`
	Category            Category    `json:"category"`
	Confidence          float64     `json:"confidence"`
	Summary             string      `json:"summary"`
	EvidenceToRequest   []string    `json:"evidence_to_request,omitempty"`
	AmountCents         int64       `json:"amount_cents"`
	OpenedAt            time.Time   `json:"opened_at"`
	TemporaryCreditDue  string      `json:"temporary_credit_due,omitempty"`
	MerchantResponseDue string      `json:"merchant_response_due,omitempty"`
	Model               string      `json:"model"`
	PromptHash          string      `json:"prompt_hash"`
	DecidedAt           time.Time   `json:"decided_at"`
	Resolution          *Resolution `json:"resolution,omitempty"`
}

// Resolution is a person's final call on a dispute routed to review.
type Resolution struct {
	Outcome    string    `json:"outcome"` // approve_chargeback | credit | deny
	Reviewer   string    `json:"reviewer"`
	Note       string    `json:"note,omitempty"`
	ResolvedAt time.Time `json:"resolved_at"`
}

// History is what the policy needs to know about the account beyond this event.
type History struct {
	DisputesLast90Days int
}

// Policy thresholds. Changing them is a reviewed code change, not a prompt edit.
type Policy struct {
	DisputeWindowDays     int
	AutoCreditMaxCents    int64
	HighAmountCents       int64
	MinConfidence         float64
	FrequentDisputerCount int
	TempCreditBizDays     int
	MerchantResponseDays  int
}

var DefaultPolicy = Policy{
	DisputeWindowDays:     60,
	AutoCreditMaxCents:    2500,
	HighAmountCents:       500000,
	MinConfidence:         0.6,
	FrequentDisputerCount: 3,
	TempCreditBizDays:     2,
	MerchantResponseDays:  20,
}

// Fictional network reason codes for the chargeback route.
var reasonCodes = map[Category]string{
	NotReceived:          "HL-13.1",
	CanceledSubscription: "HL-13.2",
	NotAsDescribed:       "HL-13.3",
	CreditNotProcessed:   "HL-13.6",
	IncorrectAmount:      "HL-12.5",
}

// Decide is the triage policy. The model's classification is one input; the rules decide.
// Rules are checked in order and the first that applies wins.
func (p Policy) Decide(e Event, c Classification, h History, model, promptHash string, now time.Time) Decision {
	d := Decision{
		DisputeID:         e.DisputeID,
		AccountID:         e.AccountID,
		Category:          c.Category,
		Confidence:        c.Confidence,
		Summary:           c.Summary,
		EvidenceToRequest: c.EvidenceToRequest,
		AmountCents:       e.AmountCents,
		OpenedAt:          e.OpenedAt,
		Model:             model,
		PromptHash:        promptHash,
		DecidedAt:         now.UTC(),
	}
	route := func(r Route, reasons ...string) Decision {
		d.Route, d.Reasons = r, reasons
		return d
	}

	statement, _ := time.Parse(DateLayout, e.StatementDate)
	if e.OpenedAt.After(statement.AddDate(0, 0, p.DisputeWindowDays)) {
		return route(RouteReview, "outside_dispute_window")
	}
	if c.Category == Fraud {
		// Never auto-credited or charged back from a model label: fraud has its own process.
		return route(RouteFraud, "cardmember_does_not_recognize_charge")
	}
	if c.InjectionSuspected {
		return route(RouteReview, "injection_suspected")
	}
	if c.Confidence < p.MinConfidence {
		return route(RouteReview, "low_confidence")
	}
	if c.Category == Other {
		return route(RouteReview, "uncategorized")
	}
	if c.Category == DuplicateCharge {
		return route(RouteReview, "needs_ledger_match")
	}
	if h.DisputesLast90Days >= p.FrequentDisputerCount {
		return route(RouteReview, "frequent_disputes_90d")
	}
	if e.AmountCents <= p.AutoCreditMaxCents {
		return route(RouteAutoCredit, "small_amount_write_off")
	}
	if e.AmountCents >= p.HighAmountCents {
		return route(RouteReview, "high_amount")
	}
	d.ReasonCode = reasonCodes[c.Category]
	d.TemporaryCreditDue = AddBusinessDays(e.OpenedAt, p.TempCreditBizDays).Format(DateLayout)
	d.MerchantResponseDue = e.OpenedAt.AddDate(0, 0, p.MerchantResponseDays).Format(DateLayout)
	return route(RouteChargeback, "eligible_for_chargeback")
}

// AddBusinessDays skips Saturdays and Sundays. (A production version also skips bank holidays.)
func AddBusinessDays(t time.Time, n int) time.Time {
	d := t
	for added := 0; added < n; {
		d = d.AddDate(0, 0, 1)
		if wd := d.Weekday(); wd != time.Saturday && wd != time.Sunday {
			added++
		}
	}
	return d
}
