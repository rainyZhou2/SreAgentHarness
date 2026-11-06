# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/harness ./cmd/harness

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/harness /harness
USER nonroot:nonroot
ENV HARNESS_HTTP_ADDR=0.0.0.0:8080 HARNESS_ARTIFACT_DIR=/data/artifacts
EXPOSE 8080 9090
ENTRYPOINT ["/harness"]
CMD ["all"]
