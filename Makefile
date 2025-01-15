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
	go test -race -cover ./...
