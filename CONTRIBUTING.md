# Contributing

Open an issue before a substantial change to the public API. Report security issues
privately, as [SECURITY.md](SECURITY.md) describes.

Install Go 1.25 or later and run the checks that CI runs.

```shell
gofmt -l .
go vet ./...
go test -race ./...
```

Use Conventional Commits and add a `CHANGELOG.md` entry for user-visible changes. By
contributing, you agree that your contribution is licensed under the Apache License 2.0.
