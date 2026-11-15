# syntax=docker/dockerfile:1
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/triage ./cmd/triage

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/triage /triage
EXPOSE 9090
USER nonroot
ENTRYPOINT ["/triage"]
