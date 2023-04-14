FROM golang:alpine AS builder
WORKDIR /build
ENV CGO_ENABLED 0
ENV GOOS linux
ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o rest2kafka .

FROM alpine
WORKDIR /build
COPY --from=builder /build/rest2kafka /build/rest2kafka
ENTRYPOINT ["./rest2kafka"]