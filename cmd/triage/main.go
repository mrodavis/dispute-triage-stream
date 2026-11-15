// Command triage consumes disputes.received, triages each dispute, and serves the gRPC review API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	triagev1 "github.com/mrodavis/dispute-triage-stream/gen/triage/v1"
	"github.com/mrodavis/dispute-triage-stream/internal/api"
	"github.com/mrodavis/dispute-triage-stream/internal/classify"
	"github.com/mrodavis/dispute-triage-stream/internal/pipeline"
	"github.com/mrodavis/dispute-triage-stream/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var classifier classify.Classifier = classify.Mock{}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		classifier = classify.NewAnthropic(key, os.Getenv("TRIAGE_MODEL"))
	}
	st := store.NewMemory()
	topics := pipeline.DefaultTopics

	client, err := kgo.NewClient(pipeline.ConsumerOpts(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","), env("KAFKA_GROUP", "dispute-triage"), topics)...)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	tokens := parseTokens(os.Getenv("REVIEW_TOKENS"))
	if len(tokens) == 0 {
		t := randomToken()
		tokens = map[string]string{t: "dev-reviewer"}
		log.Warn("REVIEW_TOKENS not set; generated a token for this run", "token", t)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(api.AuthInterceptor(tokens)))
	triagev1.RegisterReviewServiceServer(grpcServer, &api.Server{Store: st, Publisher: client, ResolvedTopic: topics.Resolved, Now: time.Now})
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hs)

	lis, err := net.Listen("tcp", env("GRPC_ADDR", ":9090"))
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	go func() {
		log.Info("grpc listening", "addr", lis.Addr().String(), "model", classifier.Model())
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc", "err", err)
		}
	}()

	runner := &pipeline.Runner{Client: client, Processor: pipeline.NewProcessor(classifier, st), Topics: topics, Log: log}
	if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("runner stopped", "err", err)
	}
	hs.Shutdown()
	grpcServer.GracefulStop()
	log.Info("stopped")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parseTokens reads "token1:alice,token2:bob".
func parseTokens(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		if token, name, ok := strings.Cut(strings.TrimSpace(pair), ":"); ok && token != "" && name != "" {
			out[token] = name
		}
	}
	return out
}

func randomToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
