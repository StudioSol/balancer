dependency:
	go get github.com/smartystreets/goconvey/convey
	go get gopkg.in/DATA-DOG/go-sqlmock.v1

test:
	echo "" > coverage.txt
	for d in $(shell go list ./...); do \
		go test -race -v -coverprofile=profile.out -covermode=atomic $$d || exit 1; \
		[ -f profile.out ] && cat profile.out >> coverage.txt && rm profile.out; \
	done