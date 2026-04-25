.PHONY: build vet test test-v test-rsm test-consensus test-storage test-transport test-pkg check fmt clean

build:  ## Compile all packages
	go build ./...

vet:  ## Static checks
	go vet ./...

test:  ## Run all tests with the race detector, bypass cache
	go test -race ./... -count=1

test-v:  ## Same as test, but print each test as it runs
	go test -race -v ./... -count=1

test-rsm:  ## Run only the rsm package tests
	go test -race -count=1 ./rsm/...

test-consensus:  ## Run only the consensus package tests
	go test -race -count=1 ./internal/consensus/...

test-storage:  ## Run only the storage package tests
	go test -race -count=1 ./storage/...

test-transport:  ## Run only the transport package tests
	go test -race -count=1 ./transport/...

test-pkg:  ## Run a specific package; usage: make test-pkg PKG=./rsm/... [RUN=TestBasic]
	go test -race -count=1 $(if $(RUN),-run $(RUN),) -v $(PKG)

check: build vet test  ## Build + vet + test (use this before committing)

fmt:  ## Format all Go files in place
	gofmt -w .

clean:  ## Remove Go's build cache artifacts
	go clean ./...
