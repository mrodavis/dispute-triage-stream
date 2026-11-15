package classify

import (
	"context"
	"regexp"

	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
)

// Mock is an offline keyword classifier so tests, CI and the eval harness run without an API key.
// It is tuned to the labeled set in testdata/, so its scores only show that the harness works.
type Mock struct{}

func (Mock) Model() string      { return "mock-keywords-v1" }
func (Mock) PromptHash() string { return PromptHash() }

var rules = []struct {
	cat dispute.Category
	re  *regexp.Regexp
}{
	{dispute.Fraud, regexp.MustCompile(`(?i)\b(never (made|authorized)|didn'?t (make|authorize)|don'?t recognize|not me|stolen)\b`)},
	{dispute.CreditNotProcessed, regexp.MustCompile(`(?i)\b(refund (never|hasn'?t|did not)|promised (a )?refund|credit (never|hasn'?t))\b`)},
	{dispute.CanceledSubscription, regexp.MustCompile(`(?i)\b(cancel+ed|unsubscribed).*\b(still|again|charged)\b`)},
	{dispute.DuplicateCharge, regexp.MustCompile(`(?i)\b(twice|two times|double charged|duplicate|charged again for the same)\b`)},
	{dispute.IncorrectAmount, regexp.MustCompile(`(?i)\b(wrong amount|more than (the )?(quoted|agreed)|overcharged|was quoted)\b`)},
	{dispute.NotReceived, regexp.MustCompile(`(?i)\b(never (arrived|came|received|showed)|not (delivered|received)|didn'?t (arrive|show up))\b`)},
	{dispute.NotAsDescribed, regexp.MustCompile(`(?i)\b(broken|damaged|defective|not as described|counterfeit|different (from|than))\b`)},
}

var injection = regexp.MustCompile(`(?i)(ignore (all |your |previous )|system:|you are now|approve this|issue (a|the) refund immediately)`)

func (Mock) Classify(_ context.Context, e dispute.Event) (dispute.Classification, error) {
	c := dispute.Classification{Category: dispute.Other, Confidence: 0.4, Summary: "unclear dispute", EvidenceToRequest: []string{}}
	for _, r := range rules {
		if r.re.MatchString(e.Statement) {
			c.Category, c.Confidence, c.Summary = r.cat, 0.85, string(r.cat)
			break
		}
	}
	// The mock reports injection itself; the real model is asked to flag it too.
	c.InjectionSuspected = injection.MatchString(e.Statement)
	switch c.Category {
	case dispute.NotReceived:
		c.EvidenceToRequest = []string{"tracking_number", "merchant_correspondence"}
	case dispute.CanceledSubscription:
		c.EvidenceToRequest = []string{"cancellation_confirmation"}
	case dispute.CreditNotProcessed:
		c.EvidenceToRequest = []string{"refund_confirmation"}
	case dispute.NotAsDescribed:
		c.EvidenceToRequest = []string{"photos", "merchant_correspondence"}
	}
	return c, nil
}
