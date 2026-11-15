// Command producer publishes the synthetic disputes in testdata/labeled.jsonl to disputes.received.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	file := flag.String("file", "testdata/labeled.jsonl", "labeled events file")
	topic := flag.String("topic", "disputes.received", "topic")
	flag.Parse()

	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.AllowAutoTopicCreation())
	if err != nil {
		fail(err)
	}
	defer client.Close()

	f, err := os.Open(*file)
	if err != nil {
		fail(err)
	}
	defer f.Close()

	var records []*kgo.Record
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row struct {
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			fail(err)
		}
		var key struct {
			AccountID string `json:"account_id"`
		}
		_ = json.Unmarshal(row.Event, &key)
		records = append(records, &kgo.Record{Topic: *topic, Key: []byte(key.AccountID), Value: row.Event})
	}
	if err := client.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		fail(err)
	}
	fmt.Printf("published %d disputes to %s\n", len(records), *topic)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
