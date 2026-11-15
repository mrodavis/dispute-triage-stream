// Command reviewctl is a small gRPC client for the review API.
//
//	reviewctl -token T queue
//	reviewctl -token T get dsp_0011
//	reviewctl -token T resolve dsp_0011 deny "statement contained instructions; no evidence of non-delivery"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	triagev1 "github.com/mrodavis/dispute-triage-stream/gen/triage/v1"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "review API address")
	token := flag.String("token", os.Getenv("REVIEW_TOKEN"), "bearer token")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fail("usage: reviewctl [queue | get ID | resolve ID approve_chargeback|credit|deny [note]]")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err.Error())
	}
	defer conn.Close()
	client := triagev1.NewReviewServiceClient(conn)
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+*token), 5*time.Second)
	defer cancel()

	var res proto.Message
	switch {
	case args[0] == "queue":
		res, err = client.ListReviewQueue(ctx, &triagev1.ListReviewQueueRequest{})
	case args[0] == "get" && len(args) == 2:
		res, err = client.GetDecision(ctx, &triagev1.GetDecisionRequest{DisputeId: args[1]})
	case args[0] == "resolve" && len(args) >= 3:
		outcome := map[string]triagev1.Outcome{"approve_chargeback": triagev1.Outcome_OUTCOME_APPROVE_CHARGEBACK, "credit": triagev1.Outcome_OUTCOME_CREDIT, "deny": triagev1.Outcome_OUTCOME_DENY}[args[2]]
		note := ""
		if len(args) > 3 {
			note = args[3]
		}
		res, err = client.ResolveReview(ctx, &triagev1.ResolveReviewRequest{DisputeId: args[1], Outcome: outcome, Note: note})
	default:
		fail("unknown command")
	}
	if err != nil {
		fail(err.Error())
	}
	fmt.Println(protojson.Format(res))
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
