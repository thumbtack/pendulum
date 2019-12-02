root_dir:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SRC = $(shell find . -name '*.go' -maxdepth 1)
SRC_TEST = $(shell find . -name '*_test.go')
BIN = pendulum

VERSION = 0.9.0

all:
	+$(MAKE) pendulum

pendulum: $(SRC)
	env GOOS=linux GOARCH=amd64 go build -o pendulum

clean: clean
	rm -fr $(BIN)

.PHONY: fmt
fmt: $(SRC)
	$(foreach file, $^, go fmt $(file);)

.PHONY: test
test: $(SRC_TEST)
	go test -coverprofile=coverage.out ./...

.PHONY: coverage
coverage: test
	go tool cover -html=coverage.out
