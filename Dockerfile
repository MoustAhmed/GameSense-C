# One image per service: docker build --build-arg SERVICE=ingest .
# TAGS=nokafka builds without the Kafka client (Phase 1 needs no go.sum).
FROM golang:1.26 AS build
WORKDIR /src
COPY go.* ./
RUN go mod download
COPY . .
ARG SERVICE
ARG TAGS=""
RUN CGO_ENABLED=0 go build -tags "${TAGS}" -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot
ENTRYPOINT ["/app"]
