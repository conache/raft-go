.PHONY: build vet test test-v check fmt clean

build:  ## Compile all packages
	go build ./...

vet:  ## Static checks
	go vet ./...

test:  ## Run all tests with the race detector, bypass cache
	go test -race ./... -count=1

test-v:  ## Same as test, but print each test as it runs
	go test -race -v ./... -count=1

check: build vet test  ## Build + vet + test (use this before committing)

fmt:  ## Format all Go files in place
	gofmt -w .

clean:  ## Remove Go's build cache artifacts
	go clean ./...
