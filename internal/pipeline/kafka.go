package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Topics struct {
	Received string
	Triaged  string
	DLQ      string
	Resolved string
}

var DefaultTopics = Topics{Received: "disputes.received", Triaged: "disputes.triaged", DLQ: "disputes.dlq", Resolved: "disputes.resolved"}

// Runner consumes disputes.received in a consumer group and produces decisions or dead letters.
//
// Delivery: offsets are committed only after every output for the polled batch is acknowledged
// by the brokers, so a crash replays the batch (at least once). Processing is idempotent by
// dispute_id, so a replay re-emits the stored decision instead of deciding twice: effectively once.
type Runner struct {
	Client    *kgo.Client
	Processor *Processor
	Topics    Topics
	Log       *slog.Logger
}

// ConsumerOpts are the client options the runner relies on.
func ConsumerOpts(brokers []string, group string, topics Topics) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics.Received),
		kgo.DisableAutoCommit(),
		// Do not let a rebalance revoke partitions between producing outputs and committing offsets.
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotent producer is on by default in franz-go
		kgo.AllowAutoTopicCreation(),
	}
}

// Run polls until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	for {
		fetches := r.Client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			r.Client.AllowRebalance() // otherwise Close would block leaving the group
			return ctx.Err()
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			r.Log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})
		if err := r.handle(ctx, fetches.Records()); err != nil {
			r.Client.AllowRebalance()
			if errors.Is(err, context.Canceled) {
				return err
			}
			r.Log.Error("batch failed; will be redelivered", "err", err)
			continue
		}
		r.Client.AllowRebalance()
	}
}

// HandleBatch is exported for tests: process, produce, then commit.
func (r *Runner) HandleBatch(ctx context.Context, records []*kgo.Record) error {
	return r.handle(ctx, records)
}

func (r *Runner) handle(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	out := make([]*kgo.Record, 0, len(records))
	for _, rec := range records {
		o, err := r.Processor.Process(ctx, rec.Value)
		if err != nil {
			return err // context cancelled: nothing is committed, the batch is redelivered
		}
		next, err := r.output(rec, o)
		if err != nil {
			return err
		}
		out = append(out, next)
	}
	if err := r.Client.ProduceSync(ctx, out...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	if err := r.Client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (r *Runner) output(in *kgo.Record, o Outcome) (*kgo.Record, error) {
	if o.DeadLetter != nil {
		v, _ := json.Marshal(o.DeadLetter)
		r.Log.Warn("dead-lettered", "key", string(in.Key), "error", o.DeadLetter.Error)
		return &kgo.Record{Topic: r.Topics.DLQ, Key: in.Key, Value: v, Headers: []kgo.RecordHeader{
			{Key: "source-topic", Value: []byte(in.Topic)},
			{Key: "source-offset", Value: []byte(fmt.Sprint(in.Offset))},
		}}, nil
	}
	v, err := json.Marshal(o.Decision)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: r.Topics.Triaged, Key: []byte(o.Decision.AccountID), Value: v, Headers: []kgo.RecordHeader{
		{Key: "route", Value: []byte(o.Decision.Route)},
		{Key: "model", Value: []byte(o.Decision.Model)},
		{Key: "prompt-hash", Value: []byte(o.Decision.PromptHash)},
		{Key: "redelivery", Value: []byte(fmt.Sprint(o.Duplicate))},
	}}, nil
}
