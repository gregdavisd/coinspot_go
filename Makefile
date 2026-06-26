.PHONY: build clean run test tidy fmt build-public run-public clean-public build-grid run-grid clean-grid

build: build-public

run: run-public

clean: clean-public
	go clean

build-public:
	$(MAKE) -f Makefile.public build

run-public:
	$(MAKE) -f Makefile.public run

clean-public:
	$(MAKE) -f Makefile.public clean

build-grid:
	$(MAKE) -f Makefile.grid build

run-grid:
	$(MAKE) -f Makefile.grid run

clean-grid:
	$(MAKE) -f Makefile.grid clean

test:
	go test -v ./...

tidy:
	go mod tidy

fmt:
	go fmt ./...

