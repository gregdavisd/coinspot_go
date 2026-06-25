# CoinSpot Go

A Go client for the CoinSpot API with CSV output streaming capabilities.

## Building

```bash
go build -o coinspot_go.exe .
```

Or using Make:

```bash
make build
```

## Running

```bash
./coinspot_go.exe
```

Or:

```bash
make run
```

## Development

- **Format code**: `make fmt` or `gofmt -s -w .`
- **Tidy dependencies**: `make tidy` or `go mod tidy`
- **Run tests**: `make test` or `go test -v ./...`
- **Clean build artifacts**: `make clean`

## Project Structure

- `main.go` - Application entry point and CSV stream abstractions
- `coinspot/api.go` - CoinSpot API client implementation
- `go.mod` - Go module definition
- `Makefile` - Build automation

## Requirements

- Go 1.21 or later
