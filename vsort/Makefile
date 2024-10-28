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

clean:
	go clean -modcache
	git restore go.mod go.sum
	/bin/rm -rf /tmp/[mSsv]*[0-9] 


