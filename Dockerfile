FROM golang:1.21 AS builder
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin v1.59.1

WORKDIR /app
COPY . .

RUN golangci-lint run ./...

RUN go test -race -tags fast ./...
