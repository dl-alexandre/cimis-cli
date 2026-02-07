## Summary

<!-- Describe what changed and why. -->

## Risk and Scope

- [ ] No breaking CLI command/flag changes, or clearly documented if intentional.
- [ ] CGO and pure-Go paths reviewed for regressions.
- [ ] TSDB dependency strategy (`./_deps/cimis-tsdb`) still valid.

## Required Quality Gates

- [ ] `make test`
- [ ] `make build`
- [ ] `make build-pure`
- [ ] `make lint`
- [ ] `make security`

## Validation

- [ ] Manual smoke check completed (example: `./build/cimis version`).
- [ ] New/updated tests included for changed behavior.
- [ ] Docs updated (README/help text) when user-visible behavior changed.

## Release Readiness

- [ ] Version metadata injection remains valid (`Version`, `BuildTime`).
- [ ] Checksum behavior unchanged or intentionally updated.
- [ ] CI `test/build/lint/security` expected to pass.
