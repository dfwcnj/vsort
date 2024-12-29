.DEFAULT_GOAL := test

.PHONY:fmt vet build

tidy:
	go mod tidy

fmt: tidy
	go fmt

vet: fmt
	go vet

test: vet
	go test -timeout 20m

check: vet
	staticcheck .

clean:
	go clean -modcache
	git restore go.mod go.sum
	/bin/rm -rf /tmp/[fmSsv]*[0-9] 
