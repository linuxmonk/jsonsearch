.PHONY: all

all: build test install

test:
	go test -v ./...

build:
	go build ./...

coverage:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out

install:
	go install ./...
