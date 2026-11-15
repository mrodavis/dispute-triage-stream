package api

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	triagev1 "github.com/mrodavis/dispute-triage-stream/gen/triage/v1"
	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
	"github.com/mrodavis/dispute-triage-stream/internal/store"
)

type fakePublisher struct{ records []*kgo.Record }

func (f *fakePublisher) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, rs...)
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r}
	}
	return out
}

func setup(t *testing.T) (triagev1.ReviewServiceClient, *store.Memory, *fakePublisher) {
	st := store.NewMemory()
	opened := time.Date(2026, 11, 16, 14, 0, 0, 0, time.UTC)
	st.Save(dispute.Decision{DisputeID: "dsp_review", AccountID: "acct_1", Route: dispute.RouteReview, Reasons: []string{"low_confidence"}, OpenedAt: opened})
	st.Save(dispute.Decision{DisputeID: "dsp_fraud", AccountID: "acct_2", Route: dispute.RouteFraud, OpenedAt: opened.Add(time.Minute)})
	st.Save(dispute.Decision{DisputeID: "dsp_auto", AccountID: "acct_3", Route: dispute.RouteAutoCredit, OpenedAt: opened})

	pub := &fakePublisher{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(AuthInterceptor(map[string]string{"t-alice": "alice"})))
	triagev1.RegisterReviewServiceServer(srv, &Server{Store: st, Publisher: pub, ResolvedTopic: "disputes.resolved", Now: func() time.Time { return opened.Add(time.Hour) }})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return triagev1.NewReviewServiceClient(conn), st, pub
}

func authed(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

func TestRequiresAToken(t *testing.T) {
	c, _, _ := setup(t)
	_, err := c.GetDecision(context.Background(), &triagev1.GetDecisionRequest{DisputeId: "dsp_review"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v", err)
	}
	_, err = c.GetDecision(authed("wrong"), &triagev1.GetDecisionRequest{DisputeId: "dsp_review"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v", err)
	}
}

func TestReviewQueueListsOnlyHumanRoutesOldestFirst(t *testing.T) {
	c, _, _ := setup(t)
	res, err := c.ListReviewQueue(authed("t-alice"), &triagev1.ListReviewQueueRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Decisions) != 2 || res.Decisions[0].DisputeId != "dsp_review" || res.Decisions[1].Route != triagev1.Route_ROUTE_FRAUD {
		t.Fatalf("got %v", res.Decisions)
	}
}

func TestResolveRecordsTheReviewerAndPublishes(t *testing.T) {
	c, st, pub := setup(t)
	d, err := c.ResolveReview(authed("t-alice"), &triagev1.ResolveReviewRequest{DisputeId: "dsp_review", Outcome: triagev1.Outcome_OUTCOME_APPROVE_CHARGEBACK, Note: "receipt checks out"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Resolution.Reviewer != "alice" || d.Resolution.Outcome != triagev1.Outcome_OUTCOME_APPROVE_CHARGEBACK {
		t.Fatalf("got %v", d.Resolution)
	}
	if len(pub.records) != 1 || pub.records[0].Topic != "disputes.resolved" || string(pub.records[0].Key) != "acct_1" {
		t.Fatalf("resolution not published: %v", pub.records)
	}
	if len(st.ReviewQueue("", 10)) != 1 {
		t.Fatal("resolved dispute should leave the queue")
	}

	_, err = c.ResolveReview(authed("t-alice"), &triagev1.ResolveReviewRequest{DisputeId: "dsp_review", Outcome: triagev1.Outcome_OUTCOME_DENY})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second resolve: got %v", err)
	}
	_, err = c.ResolveReview(authed("t-alice"), &triagev1.ResolveReviewRequest{DisputeId: "dsp_auto", Outcome: triagev1.Outcome_OUTCOME_DENY})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("auto-credited disputes are not reviewable: got %v", err)
	}
	_, err = c.ResolveReview(authed("t-alice"), &triagev1.ResolveReviewRequest{DisputeId: "dsp_fraud"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing outcome: got %v", err)
	}
}

func TestGetDecisionNotFound(t *testing.T) {
	c, _, _ := setup(t)
	_, err := c.GetDecision(authed("t-alice"), &triagev1.GetDecisionRequest{DisputeId: "nope"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got %v", err)
	}
}
