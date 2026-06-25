.PHONY: build clean run test

build:
	go build -o coinspot_go.exe .

clean:
	go clean
	rm -f coinspot_go.exe

run: build
	./coinspot_go.exe

test:
	go test -v ./...

tidy:
	go mod tidy

fmt:
	go fmt ./...
