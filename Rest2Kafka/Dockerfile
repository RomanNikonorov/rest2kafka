FROM golang:alpine AS builder
WORKDIR /build
ADD go.mod .
COPY . .
RUN go build -o rest2kafka rest2kafka.go

FROM alpine
WORKDIR /build
COPY --from=builder /build/rest2kafka /build/rest2kafka
ENTRYPOINT ["./rest2kafka"]