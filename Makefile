.PHONY: build test bench run lint clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

run: build
	./bin/emberkv -dir data

# Load-test a server started with `make run`.
bench: build
	./bin/emberbench -t set,get,incr,mixed
	./bin/emberbench -t set,get -P 16 -n 1000000

lint:
	gofmt -l . | (! grep .)
	go vet ./...

clean:
	rm -rf bin data
