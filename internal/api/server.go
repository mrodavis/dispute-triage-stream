// Package api is the gRPC ReviewService that dispute analysts use.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	triagev1 "github.com/mrodavis/dispute-triage-stream/gen/triage/v1"
	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
	"github.com/mrodavis/dispute-triage-stream/internal/store"
)

// Publisher sends resolutions to Kafka (disputes.resolved) so downstream systems and the Spark job see them.
type Publisher interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type Server struct {
	triagev1.UnimplementedReviewServiceServer
	Store         store.Store
	Publisher     Publisher
	ResolvedTopic string
	Now           func() time.Time
}

type reviewerKey struct{}

// AuthInterceptor requires "authorization: Bearer <token>" and maps tokens to reviewer names.
// In production this is the company's identity provider; a token map keeps the demo self-contained.
func AuthInterceptor(tokens map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		auth := strings.TrimPrefix(first(md.Get("authorization")), "Bearer ")
		for token, reviewer := range tokens {
			if auth != "" && subtle.ConstantTimeCompare([]byte(auth), []byte(token)) == 1 {
				return handler(context.WithValue(ctx, reviewerKey{}, reviewer), req)
			}
		}
		return nil, status.Error(codes.Unauthenticated, "missing or invalid bearer token")
	}
}

func (s *Server) GetDecision(_ context.Context, req *triagev1.GetDecisionRequest) (*triagev1.Decision, error) {
	d, ok := s.Store.Get(req.GetDisputeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "dispute not found")
	}
	return toProto(d), nil
}

func (s *Server) ListReviewQueue(_ context.Context, req *triagev1.ListReviewQueueRequest) (*triagev1.ListReviewQueueResponse, error) {
	size := int(req.GetPageSize())
	if size <= 0 || size > 100 {
		size = 50
	}
	var route dispute.Route
	switch req.GetRoute() {
	case triagev1.Route_ROUTE_UNSPECIFIED:
	case triagev1.Route_ROUTE_REVIEW:
		route = dispute.RouteReview
	case triagev1.Route_ROUTE_FRAUD:
		route = dispute.RouteFraud
	default:
		return nil, status.Error(codes.InvalidArgument, "only review and fraud routes have a queue")
	}
	resp := &triagev1.ListReviewQueueResponse{}
	for _, d := range s.Store.ReviewQueue(route, size) {
		resp.Decisions = append(resp.Decisions, toProto(d))
	}
	return resp, nil
}

func (s *Server) ResolveReview(ctx context.Context, req *triagev1.ResolveReviewRequest) (*triagev1.Decision, error) {
	reviewer, _ := ctx.Value(reviewerKey{}).(string)
	if reviewer == "" {
		return nil, status.Error(codes.Unauthenticated, "no reviewer identity")
	}
	outcome, ok := outcomes[req.GetOutcome()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "outcome is required")
	}
	d, err := s.Store.Resolve(req.GetDisputeId(), dispute.Resolution{Outcome: outcome, Reviewer: reviewer, Note: req.GetNote(), ResolvedAt: s.Now().UTC()})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrNotReviewable):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		return nil, status.Error(codes.Internal, "resolve failed")
	}
	if s.Publisher != nil {
		v, _ := json.Marshal(d)
		if err := s.Publisher.ProduceSync(ctx, &kgo.Record{Topic: s.ResolvedTopic, Key: []byte(d.AccountID), Value: v}).FirstErr(); err != nil {
			return nil, status.Error(codes.Unavailable, "resolution saved but not published; retry")
		}
	}
	return toProto(d), nil
}

var outcomes = map[triagev1.Outcome]string{
	triagev1.Outcome_OUTCOME_APPROVE_CHARGEBACK: "approve_chargeback",
	triagev1.Outcome_OUTCOME_CREDIT:             "credit",
	triagev1.Outcome_OUTCOME_DENY:               "deny",
}

var routes = map[dispute.Route]triagev1.Route{
	dispute.RouteAutoCredit: triagev1.Route_ROUTE_AUTO_CREDIT,
	dispute.RouteChargeback: triagev1.Route_ROUTE_CHARGEBACK,
	dispute.RouteReview:     triagev1.Route_ROUTE_REVIEW,
	dispute.RouteFraud:      triagev1.Route_ROUTE_FRAUD,
}

func toProto(d dispute.Decision) *triagev1.Decision {
	p := &triagev1.Decision{
		DisputeId: d.DisputeID, AccountId: d.AccountID, Route: routes[d.Route], Reasons: d.Reasons,
		ReasonCode: d.ReasonCode, Category: string(d.Category), Confidence: d.Confidence, Summary: d.Summary,
		AmountCents: d.AmountCents, TemporaryCreditDue: d.TemporaryCreditDue, MerchantResponseDue: d.MerchantResponseDue,
		Model: d.Model, PromptHash: d.PromptHash, DecidedAt: d.DecidedAt.Format(time.RFC3339),
	}
	if r := d.Resolution; r != nil {
		var o triagev1.Outcome
		for k, v := range outcomes {
			if v == r.Outcome {
				o = k
			}
		}
		p.Resolution = &triagev1.Resolution{Outcome: o, Reviewer: r.Reviewer, Note: r.Note, ResolvedAt: r.ResolvedAt.Format(time.RFC3339)}
	}
	return p
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}
