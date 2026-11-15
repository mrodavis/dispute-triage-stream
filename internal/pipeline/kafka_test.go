package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
)

// These tests run the real franz-go client against kfake, an in-process cluster that speaks the Kafka protocol.

func cluster(t *testing.T) []string {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, DefaultTopics.Received, DefaultTopics.Triaged, DefaultTopics.DLQ, DefaultTopics.Resolved))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func publishTestdata(t *testing.T, brokers []string) int {
	t.Helper()
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers...))
	defer cl.Close()
	f, _ := os.Open("../../testdata/labeled.jsonl")
	defer f.Close()
	var recs []*kgo.Record
	s := bufio.NewScanner(f)
	for s.Scan() {
		var row struct {
			Event json.RawMessage `json:"event"`
		}
		_ = json.Unmarshal(s.Bytes(), &row)
		var k struct {
			AccountID string `json:"account_id"`
		}
		_ = json.Unmarshal(row.Event, &k)
		recs = append(recs, &kgo.Record{Topic: DefaultTopics.Received, Key: []byte(k.AccountID), Value: row.Event})
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return len(recs)
}

// drain runs the runner's batch handler until n records have been processed.
func drain(t *testing.T, r *Runner, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for done := 0; done < n; {
		fetches := r.Client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d of %d records", done, n)
		}
		recs := fetches.Records()
		if err := r.HandleBatch(ctx, recs); err != nil {
			t.Fatal(err)
		}
		r.Client.AllowRebalance()
		done += len(recs)
	}
}

func readAll(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d from %s", len(out), n, topic)
		}
		out = append(out, f.Records()...)
	}
	return out
}

func newRunner(t *testing.T, brokers []string, p *Processor) *Runner {
	cl, err := kgo.NewClient(ConsumerOpts(brokers, "triage-test", DefaultTopics)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return &Runner{Client: cl, Processor: p, Topics: DefaultTopics, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestEndToEndOverKafka(t *testing.T) {
	brokers := cluster(t)
	n := publishTestdata(t, brokers)
	c := &scripted{}
	p := newTestProcessor(c)
	r := newRunner(t, brokers, p)
	drain(t, r, n)

	triaged := readAll(t, brokers, DefaultTopics.Triaged, n-1)
	dlq := readAll(t, brokers, DefaultTopics.DLQ, 1)
	routes := map[dispute.Route]int{}
	for _, rec := range triaged {
		var d dispute.Decision
		if err := json.Unmarshal(rec.Value, &d); err != nil {
			t.Fatal(err)
		}
		routes[d.Route]++
		if string(rec.Key) != d.AccountID {
			t.Fatalf("decision keyed by %q, want account %q", rec.Key, d.AccountID)
		}
	}
	if routes[dispute.RouteFraud] != 2 || routes[dispute.RouteAutoCredit] != 5 || len(dlq) != 1 {
		t.Fatalf("routes %v, dead letters %d", routes, len(dlq))
	}

	// Offsets were committed: a restarted consumer in the same group has nothing left to read.
	r2 := newRunner(t, brokers, p)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	recs := r2.Client.PollFetches(ctx).Records()
	r2.Client.AllowRebalance() // BlockRebalanceOnPoll: release, or leaving the group would wait forever
	if len(recs) != 0 {
		t.Fatalf("restarted consumer re-read %d committed records", len(recs))
	}
}

func TestRedeliveredDisputeIsNotDecidedTwice(t *testing.T) {
	brokers := cluster(t)
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers...))
	defer cl.Close()
	rec := func() *kgo.Record {
		return &kgo.Record{Topic: DefaultTopics.Received, Key: []byte("acct_1"), Value: []byte(goodEvent)}
	}
	if err := cl.ProduceSync(context.Background(), rec(), rec()).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c := &scripted{}
	drain(t, newRunner(t, brokers, newTestProcessor(c)), 2)

	out := readAll(t, brokers, DefaultTopics.Triaged, 2)
	if c.calls != 1 {
		t.Fatalf("model called %d times for one dispute delivered twice", c.calls)
	}
	var redelivery string
	for _, h := range out[1].Headers {
		if h.Key == "redelivery" {
			redelivery = string(h.Value)
		}
	}
	if redelivery != "true" || string(out[0].Value) != string(out[1].Value) {
		t.Fatalf("second delivery should re-emit the same decision, flagged as a redelivery")
	}
}
