.PHONY: all

all: build test install

test:
	go test -v ./...

build:
	go build ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

bench:
	go test -v -run=ZZZ -bench=. ./...

install:
	go install ./...
