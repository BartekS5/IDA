# IDA

Project for Internet Database Applications utilizing Go, SQL and Mongo

## Create .env (copy from .env.example and fill in actual values)

```bash
bashcp .env.example .env
```

## Usefull commands

### Build the project

```bash
go build -o ida ./cmd/main
```

### Run the application

```bash
go run ./cmd/main
```

### Format code

```bash
go fmt ./...
```

### Lint code (install golangci-lint first)

```bash
golangci-lint run
```

### Run tests

```bash
go test ./...
```

### Update dependencies

```bash
go get -u
```
