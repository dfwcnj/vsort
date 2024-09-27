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
	/bin/rm -r /tmp/[mSsv]*[0-9]
