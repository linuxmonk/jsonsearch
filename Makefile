.PHONY: all

all: build test install

test:
	go test -v ./...

build:
	go build ./...

install:
	go install ./...
