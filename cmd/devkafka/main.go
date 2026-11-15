// Command devkafka starts an in-process, Kafka-protocol-compatible broker (franz-go's kfake) for
// local demos without Docker. Development only: no persistence, no security.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/mrodavis/dispute-triage-stream/internal/pipeline"
)

func main() {
	port := flag.Int("port", 9092, "listen port")
	flag.Parse()
	t := pipeline.DefaultTopics
	c, err := kfake.NewCluster(kfake.Ports(*port), kfake.SeedTopics(3, t.Received, t.Triaged, t.DLQ, t.Resolved))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("dev kafka listening on", c.ListenAddrs())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
