.PHONY: test integration eval proto spark-test

test:            ## unit tests, including Kafka tests against an in-process broker
	go test -race ./...

integration:     ## against a real broker: KAFKA_BROKERS=localhost:9092 make integration
	go test -tags=integration -run Real -count=1 ./internal/pipeline

eval:            ## labeled disputes through the real processor; fails on a safety gate
	go run ./cmd/eval

proto:           ## needs protoc, protoc-gen-go and protoc-gen-go-grpc on PATH
	protoc -I proto --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/triage/v1/triage.proto

spark-test:
	cd analytics/spark && python -m pytest -q
