.PHONY: build vet test test-v test-rsm test-consensus test-storage test-transport test-pkg check fmt clean proto run-local-0 run-local-1 run-local-2 cli cli-docker

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

proto:  ## Regenerate gRPC code from proto/raft/v1/raft.proto
	cd proto && buf generate

# Local 3-node cluster — run each in its own terminal
LOCAL_RAFT  = localhost:9001,localhost:9002,localhost:9003

run-local-0:  ## Run peer 0 locally (terminal 1)
	go run ./cmd/raftd --id=0 --raft=$(LOCAL_RAFT) --http=localhost:9101

run-local-1:  ## Run peer 1 locally (terminal 2)
	go run ./cmd/raftd --id=1 --raft=$(LOCAL_RAFT) --http=localhost:9102

run-local-2:  ## Run peer 2 locally (terminal 3)
	go run ./cmd/raftd --id=2 --raft=$(LOCAL_RAFT) --http=localhost:9103

LOCAL_CLI  = http://localhost:9101,http://localhost:9102,http://localhost:9103
DOCKER_CLI = http://localhost:18001,http://localhost:18002,http://localhost:18003

cli:  ## Run raftcli against the local 3-node cluster
	go run ./cmd/raftcli --peers=$(LOCAL_CLI)

cli-docker:  ## Run raftcli against the docker-compose 3-node cluster
	go run ./cmd/raftcli --peers=$(DOCKER_CLI)
