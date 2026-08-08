FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kv-server ./cmd/kv-server

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/kv-server /kv-server
EXPOSE 6380
ENTRYPOINT ["/kv-server", "-host", "0.0.0.0"]
