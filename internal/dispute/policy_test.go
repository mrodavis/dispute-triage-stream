package dispute

import (
	"slices"
	"testing"
	"time"
)

var opened = time.Date(2026, 11, 13, 15, 0, 0, 0, time.UTC) // a Friday

func event(cents int64) Event {
	return Event{DisputeID: "dsp_1", AccountID: "acct_1", AmountCents: cents, Currency: "USD", StatementDate: "2026-11-02", OpenedAt: opened, Statement: "x"}
}

func class(cat Category, conf float64) Classification {
	return Classification{Category: cat, Confidence: conf, Summary: "s"}
}

func TestDecideRules(t *testing.T) {
	now := opened.Add(time.Minute)
	cases := []struct {
		name   string
		e      Event
		c      Classification
		h      History
		route  Route
		reason string
		code   string
	}{
		{"chargeback", event(8999), class(NotReceived, 0.9), History{}, RouteChargeback, "eligible_for_chargeback", "HL-13.1"},
		{"small amount is written off", event(2500), class(NotReceived, 0.9), History{}, RouteAutoCredit, "small_amount_write_off", ""},
		{"fraud is never auto-credited, even when small", event(500), class(Fraud, 0.99), History{}, RouteFraud, "cardmember_does_not_recognize_charge", ""},
		{"injection goes to a person", event(500), Classification{Category: NotReceived, Confidence: 0.99, Summary: "s", InjectionSuspected: true}, History{}, RouteReview, "injection_suspected", ""},
		{"low confidence", event(8999), class(NotReceived, 0.59), History{}, RouteReview, "low_confidence", ""},
		{"uncategorized", event(8999), class(Other, 0.9), History{}, RouteReview, "uncategorized", ""},
		{"duplicates need the ledger", event(8999), class(DuplicateCharge, 0.9), History{}, RouteReview, "needs_ledger_match", ""},
		{"frequent disputer", event(1000), class(NotReceived, 0.9), History{DisputesLast90Days: 3}, RouteReview, "frequent_disputes_90d", ""},
		{"high amount", event(500000), class(NotAsDescribed, 0.9), History{}, RouteReview, "high_amount", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := DefaultPolicy.Decide(tc.e, tc.c, tc.h, "m", "h", now)
			if d.Route != tc.route || !slices.Contains(d.Reasons, tc.reason) || d.ReasonCode != tc.code {
				t.Fatalf("got %s %v %q, want %s %s %q", d.Route, d.Reasons, d.ReasonCode, tc.route, tc.reason, tc.code)
			}
		})
	}
}

func TestDisputeWindowIsCheckedFirst(t *testing.T) {
	e := event(8999)
	e.StatementDate = "2026-08-01" // more than 60 days before opened
	d := DefaultPolicy.Decide(e, class(Fraud, 0.99), History{}, "m", "h", opened)
	if d.Route != RouteReview || d.Reasons[0] != "outside_dispute_window" {
		t.Fatalf("got %s %v", d.Route, d.Reasons)
	}
}

func TestChargebackDeadlines(t *testing.T) {
	d := DefaultPolicy.Decide(event(8999), class(NotReceived, 0.9), History{}, "m", "h", opened)
	if d.TemporaryCreditDue != "2026-11-17" {
		t.Fatalf("temporary credit due %s, want Tuesday 2026-11-17 (2 business days after Friday)", d.TemporaryCreditDue)
	}
	if d.MerchantResponseDue != "2026-12-03" {
		t.Fatalf("merchant response due %s", d.MerchantResponseDue)
	}
}

func TestValidation(t *testing.T) {
	if err := (Event{Currency: "EUR"}).Validate(); err == nil {
		t.Fatal("expected an error")
	}
	if err := event(100).Validate(); err != nil {
		t.Fatal(err)
	}
	bad := Classification{Category: "refund_now", Confidence: 2, Summary: "", EvidenceToRequest: []string{"selfie"}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected schema errors")
	}
}
