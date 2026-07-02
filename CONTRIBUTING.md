# Contributing to Gas

Thanks for your interest in contributing!

## Filing issues

- **Bugs:** include Go version, OS, a minimal reproducer, and what you expected vs. what happened.
- **Features:** describe the use case before the proposed API. Small, focused proposals get reviewed faster.

## Submitting changes

1. Fork the repo and create a topic branch.
2. Run tests locally: `go test -race ./...`
3. Run lint: `make lint` (or `golangci-lint run`).
4. Use **conventional commits** (`feat:`, `fix:`, `docs:`, `refactor:`, etc.).
5. **Sign off** every commit (`git commit -s`) — required by the DCO.
6. Open a PR against `main`. Keep PRs focused; one logical change per PR.

## Tests

- New features need tests. Bug fixes need a regression test.
- Prefer the table-driven `t.Run("name", func(t *testing.T) { ... })` style used throughout.
- Keep tests parallel (`t.Parallel()`) unless they touch shared state.

## Code style

- Follow the existing patterns in the package you're touching.
- Document exported APIs. Doc comments should start with the symbol name.

## Reporting security issues

See [SECURITY.md](SECURITY.md). **Do not** open public issues for security reports.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
