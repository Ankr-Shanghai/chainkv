GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_DATE=$(shell git log -n1 --pretty='format:%cd' --date=format:'%Y%m%d')

.PHONY: build
build:
	CGO_ENABLED=0 go build -o ./bin/chainkv -ldflags "-X main.GitCommit=$(GIT_COMMIT) -X main.GitDate=$(GIT_DATE)" .
	@echo "Done building."