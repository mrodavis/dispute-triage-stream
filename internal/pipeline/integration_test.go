//go:build integration

package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
)

// TestAgainstRealKafka runs the same flow as the kfake test against a real broker.
// CI starts apache/kafka as a service container and sets KAFKA_BROKERS:
//
//	KAFKA_BROKERS=localhost:9092 go test -tags=integration -run Real ./internal/pipeline
//
// It leaves the topics populated so the Spark job can read them in the next CI step.
func TestAgainstRealKafka(t *testing.T) {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	t0 := DefaultTopics
	if _, err := kadm.NewClient(admin).CreateTopics(ctx, 3, 1, nil, t0.Received, t0.Triaged, t0.DLQ, t0.Resolved); err != nil {
		t.Fatal(err)
	}

	n := publishTestdata(t, brokers)
	p := newTestProcessor(&scripted{})
	r := newRunner(t, brokers, p)
	drain(t, r, n)

	triaged := readAll(t, brokers, t0.Triaged, n-1)
	if dlq := readAll(t, brokers, t0.DLQ, 1); len(dlq) != 1 {
		t.Fatalf("dead letters: %d", len(dlq))
	}
	fraud := 0
	for _, rec := range triaged {
		var d dispute.Decision
		_ = json.Unmarshal(rec.Value, &d)
		if d.Route == dispute.RouteFraud {
			fraud++
		}
	}
	if fraud != 2 {
		t.Fatalf("fraud decisions: %d", fraud)
	}

	// Publish one resolution so the Spark job has review outcomes to read.
	d, _ := p.Store.Get("dsp_0013")
	d.Resolution = &dispute.Resolution{Outcome: "approve_chargeback", Reviewer: "ci", ResolvedAt: time.Now().UTC()}
	v, _ := json.Marshal(d)
	if err := admin.ProduceSync(ctx, &kgo.Record{Topic: t0.Resolved, Key: []byte(d.AccountID), Value: v}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
