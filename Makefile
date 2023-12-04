lint:
	gofmt -w .
	golangci-lint run

test:
	go test -v ./... -race

bench:
	go test -v ./... -run=XXX -bench=. -benchmem
