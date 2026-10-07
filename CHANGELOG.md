# Changelog

## 0.1.1 - 2026-10-07

The first release of the group model.

- `Load` and `Client.Load` return a long-lived experiment handle.
- `Experiment.Assign` returns a `*Group`; read it with `Value` and `Function`.
- `Experiment.Log` records outcomes for any subject, from any process.
- Failures fall back to the application's default and record a `statespace.error` outcome.
