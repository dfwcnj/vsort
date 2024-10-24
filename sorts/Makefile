.DEFAULT_GOAL := test

.PHONY:fmt vet build

tidy:
	go mod tidy

fmt: tidy
	go fmt

vet: fmt
	go vet

test: vet
	go test

clean:
	go clean -modcache
	/bin/rm -rf /tmp/[mSsv]*[0-9] 
	/bin/rm -rf /tmp/chsort*
