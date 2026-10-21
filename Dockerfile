FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /emberkv ./cmd/emberkv

FROM gcr.io/distroless/static
COPY --from=build /emberkv /emberkv
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["/emberkv", "-addr", "0.0.0.0:6380", "-dir", "/data"]
