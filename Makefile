COVER         = .coverage

.PHONY: all test

help: ## List the available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: cover-init ## run a comprehensive go test
	go test -v ./... -cover -coverprofile=$(COVER)/coverage-report

fmt:  ## run go fmt on .go files tracked by git w/o vendor
	git ls-files | grep '.go$$' | grep -v vendor/ |xargs -I% sh -c 'go fmt %'

lint:  ## lint with golangci-lint
	golangci-lint run

cover: cover-init test ## run tests and generate coverage report as HTML
	go tool cover -html=$(COVER)/coverage-report

vet:  ## golang vet
	go vet ./...

vet-shadow:  ## run vet for shadow vars
	go vet -vettool=$$GOPATH/bin/shadow ./...

coverage:   cover-init ## generate the coverage report from file to STDOUT
	go tool cover -func=$(COVER)/.coverage-report

coverage-html:  cover-init ## generate HTML file from coverage-report
	go tool cover -html=$(COVER)/coverage-report -o $(COVER)/coverage.html

race:  ## go race testing
	go test -race -short $(go list ./... | grep -v /vendor/)

check: lint test fmt  ## helpful checks while writing go code

cover-init:
	[ -d $(COVER) ] || mkdir -p $(COVER)
