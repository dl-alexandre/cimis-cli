# cimis-cli Improvement Plan

Assessment of the codebase identified six areas for improvement, organized into
three phases. Each phase builds on the previous one — testability first, then
dependency hygiene, then polish.

---

## Phase 1: Testability

The biggest gap. Most core logic has no unit tests and the monolithic `main.go`
makes isolated testing difficult.

### 1.1 Split `cmd/cimis/main.go` into per-command files

**Why:** `main.go` is 876 lines handling all eight commands. Splitting it makes
each command independently readable, testable, and reviewable.

**What:**
- [ ] Extract `fetch` command logic → `cmd/cimis/fetch.go`
- [ ] Extract `query` command logic → `cmd/cimis/query.go`
- [ ] Extract `ingest` command logic → `cmd/cimis/ingest.go`
- [ ] Extract `stats` command logic → `cmd/cimis/stats.go`
- [ ] Extract `verify` command logic → `cmd/cimis/verify.go`
- [ ] Extract `init` command logic → `cmd/cimis/init.go`
- [ ] Keep `main.go` as thin dispatcher: flag parsing, subcommand routing, version

**Scope:** `cmd/cimis/` only. No behavior changes.

### 1.2 Add unit tests for `internal/api/client.go`

**Why:** The API client has date parsing, record conversion, QC flag handling,
and response deserialization — all testable without a live API.

**What:**
- [ ] Test `parseCIMISDate` with valid dates, edge cases, empty strings
- [ ] Test `convertToRecord` with known input/output pairs
- [ ] Test QC flag extraction and mapping
- [ ] Test error handling for malformed API responses (mock HTTP)
- [ ] Test `FetchData` with a mock HTTP server (`httptest.NewServer`)

**Scope:** `internal/api/client_test.go` (new file).

### 1.3 Add unit tests for utility functions in `cmd/cimis/`

**Why:** `parseCacheSize`, metrics formatting, and retry logic are pure functions
that should have table-driven tests.

**What:**
- [ ] Test `parseCacheSize` — "100MB", "1.5GB", "512KB", "", invalid input
- [ ] Test metrics JSON output formatting
- [ ] Test retry/backoff error classification

**Scope:** `cmd/cimis/main_test.go` and `cmd/cimis/metrics_test.go` (new files).

---

## Phase 2: Dependency & Configuration

### 2.1 Resolve `cimis-tsdb` dependency strategy

**Why:** The `replace` directive in `go.mod` pointing to `../../../cimis-tsdb`
means `go install` fails for any external user, CI needs a PAT to clone the
sibling repo, and contributors must manually set up the directory structure.

**Options (pick one):**
- [ ] **Option A:** Make `cimis-tsdb` a public repository and publish tagged
      releases. Remove the `replace` directive. Simplest long-term solution.
- [ ] **Option B:** Vendor `cimis-tsdb` into this repo (`go mod vendor`).
      Self-contained but adds maintenance burden for syncing changes.
- [ ] **Option C:** Keep private but use a Go module proxy or `GOPRIVATE`
      configuration. Document the setup clearly.

**Scope:** `go.mod`, CI workflows, contributor documentation.

### 2.2 Centralize hardcoded constants

**Why:** The epoch year (1985), API base URL, buffer sizes, and pool settings
are scattered across files. Centralizing them makes tuning and testing easier.

**What:**
- [ ] Create a `const` block or small config struct with:
  - API base URL (currently hardcoded in `internal/api/client.go:19`)
  - Epoch year (used in C layer and Go layer)
  - Default buffer/pool sizes
- [ ] Add `--base-url` flag to the CLI for testing against mock servers

**Scope:** `internal/api/client.go`, `cmd/cimis/main.go`.

### 2.3 Improve error context

**Why:** Errors like "No data found" don't tell the user whether the station ID
is wrong, the date range is empty, or the API returned an error.

**What:**
- [ ] Wrap errors with `fmt.Errorf("...: %w", err)` to build context chains
- [ ] Include station ID, date range, and HTTP status in fetch errors
- [ ] Add suggestions for common failure modes (invalid key, rate limit, no data)

**Scope:** `internal/api/client.go`, `cmd/cimis/main.go`.

---

## Phase 3: Polish

### 3.1 Document and CI-test the pure-Go build path

**Why:** `make build-pure` exists but isn't tested in CI or mentioned in the
README. Users without a C compiler have no guidance.

**What:**
- [ ] Add a CI matrix entry that builds with `CGO_ENABLED=0`
- [ ] Document the tradeoffs (pure Go vs CGO) in the README
- [ ] Ensure the pure-Go path passes all tests

**Scope:** `.github/workflows/ci.yml`, `README.md`.

### 3.2 Consolidate documentation

**Why:** There are 8 markdown files beyond the README. Some overlap (e.g.,
`DEPLOYMENT_READY.md` and `DEPLOYMENT_COMPLETE.md`) and some are reference
material that could live in a `docs/` directory.

**What:**
- [ ] Move supplementary docs into `docs/` directory
- [ ] Merge `DEPLOYMENT_READY.md` and `DEPLOYMENT_COMPLETE.md`
- [ ] Link from README to docs rather than keeping everything top-level
- [ ] Evaluate whether `OPTIMIZATION_SUMMARY.md` and `PRODUCTION_HARDENING.md`
      should be combined into a single architecture/operations guide

**Scope:** Top-level markdown files, `README.md`.

---

## Summary

| Phase | Item | Priority | Effort |
|-------|------|----------|--------|
| 1.1 | Split `main.go` | High | Medium |
| 1.2 | Unit tests for API client | High | Medium |
| 1.3 | Unit tests for utilities | High | Small |
| 2.1 | Resolve `cimis-tsdb` dependency | Medium | Varies |
| 2.2 | Centralize constants | Medium | Small |
| 2.3 | Improve error context | Medium | Small |
| 3.1 | Pure-Go build in CI | Low | Small |
| 3.2 | Consolidate docs | Low | Small |
