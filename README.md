# Statespace for Go

[![CI](https://github.com/statespace-tech/go-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/statespace-tech/go-sdk/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/statespace-tech/go-sdk.svg)](https://pkg.go.dev/github.com/statespace-tech/go-sdk)
[![License](https://img.shields.io/badge/license-Apache--2.0-007ec6?style=flat-square)](LICENSE)

Run Statespace A/B tests on functions and values in Go services. Each subject is assigned
to a group, reads that group's parameters, and falls back to your current code
everywhere else.

## Install

Add the module to your service.

```shell
go get github.com/statespace-tech/go-sdk
```

Set an API key from `ssp key create --preset runtime`. Locally, the SDK uses your `ssp login` session.

```shell
export SSP_API_KEY=ssp_key_...
```

## Quickstart

Load an experiment once and keep it. The SDK refreshes its configuration in the background.

```go
ranking, err := statespace.Load(ctx, "ranking")
```

Assign a subject. The same subject always gets the same group.

```go
group := ranking.Assign("user-42", map[string]any{"country": "US"})
```

Read a value. The last argument is your current value, which control receives.

```go
topK := statespace.Value(group, "top_k", 20)
```

Read a function. It runs in a local sandbox and falls back to your function if it fails.

```go
rank := statespace.Function(group, "ranker", rerank)
ranked, err := rank(ctx, items)
```

Log outcomes for the subject, from this process or any other.

```go
ranking.Log("user-42", "click", map[string]any{"position": 3})
```

Compare groups from the command line.

```shell
ssp experiment results ranking --outcome click
```

## Groups

`group.Name` is the group's name, `"control"`, or empty when the subject is not in the experiment.

```go
if group.Name != "" {
	log.Printf("user-42 is in %s", group.Name)
}
```

A value decodes into the type of its default, including structs. If it does not fit, you
get the default, and the SDK records a `statespace.error` outcome.

```go
temperature := statespace.Value(group, "temperature", 0.7)
sampling := statespace.Value(group, "sampling", Sampling{TopP: 1})
```

A function takes one JSON value and returns one. Its context carries the deadline.

```go
score := statespace.Function(group, "scorer", scoreDefault, statespace.WithTimeout(50*time.Millisecond))
result, err := score(ctx, Query{Text: text, Items: items})
```

An error from a returned function is always your default's own. A failing variant falls back silently.

```go
func scoreDefault(query Query) (Result, error) { ... }
```

## Delivery

Events are sent in the background in batches. Flush before the process exits.

```go
defer statespace.Flush(context.Background())
```

Create a client to configure credentials in code.

```go
client, err := statespace.NewClient(statespace.Options{APIKey: "ssp_key_..."})
defer client.Close(context.Background())
ranking, err := client.Load(ctx, "ranking")
```

## Guarantees

- Reads never fail because of Statespace. They return the default and log a warning.
  Only `Load` returns an error, for an unknown experiment or an invalid key.
- Assignment is computed locally from a cached configuration, with no network call.
- Functions run in a pure-Go WebAssembly interpreter without file, network, or environment
  access, with 256 MiB of memory by default (`STATESPACE_MAX_MEMORY_BYTES`).
- Python, TypeScript, and Go assign every subject to the same group.

## License

Apache-2.0
