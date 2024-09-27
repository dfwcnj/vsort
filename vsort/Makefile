.DEFAULT_GOAL := build

.PHONY:fmt vet build

tidy:
	go mod tidy

fmt: tidy
	go fmt

vet: fmt
	go vet

build: vet
	go build

clean: vsort
	go clean

