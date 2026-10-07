# Contributor instructions

## Rules

- Never return a Statespace error from `Assign`, `Value`, or a function returned by
  `Function`. Return the default, log a warning, and record `statespace.error`.
- Keep assignment identical to the Python and TypeScript SDKs. `statespace_test.go` holds
  the shared vectors.

## Checks

```shell
gofmt -l .
go vet ./...
go test -race ./...
```

## Commits

Use Conventional Commits, such as `fix(value): decode integers into float defaults`.
