export GO111MODULE=on

all: deps build
install: 
	go install
build: 
	go build
clean: 
	go clean
deps: 
	go mod download
test:
	echo "" > coverage.txt
	for d in $(shell go list ./...); do \
		go test -race -v -coverprofile=profile.out -covermode=atomic $$d || exit 1; \
		[ -f profile.out ] && cat profile.out >> coverage.txt && rm profile.out; \
	done
