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
- [x] Extract `fetch` command logic → `cmd/cimis/fetch.go`
- [x] Extract `query` command logic → `cmd/cimis/query.go`
- [x] Extract `ingest` command logic → `cmd/cimis/ingest.go`
- [x] Extract `stats` command logic → `cmd/cimis/stats.go`
- [x] Extract `verify` command logic → `cmd/cimis/verify.go`
- [x] Extract `init` command logic → `cmd/cimis/init.go`
- [x] Keep `main.go` as thin dispatcher: flag parsing, subcommand routing, version

**Scope:** `cmd/cimis/` only. No behavior changes.

### 1.2 Add unit tests for `internal/api/client.go`

**Why:** The API client has date parsing, record conversion, QC flag handling,
and response deserialization — all testable without a live API.

**What:**
- [x] Test `parseCIMISDate` with valid dates, edge cases, empty strings
- [x] Test `convertToRecord` with known input/output pairs
- [x] Test QC flag extraction and mapping
- [x] Test error handling for malformed API responses (mock HTTP)
- [x] Test `FetchData` with a mock HTTP server (`httptest.NewServer`)

**Scope:** `internal/api/client_test.go` (new file).

### 1.3 Add unit tests for utility functions in `cmd/cimis/`

**Why:** `parseCacheSize`, metrics formatting, and retry logic are pure functions
that should have table-driven tests.

**What:**
- [x] Test `parseCacheSize` — "100MB", "1.5GB", "512KB", "", invalid input
- [x] Test metrics JSON output formatting
- [x] Test retry/backoff error classification

**Scope:** `cmd/cimis/main_test.go` and `cmd/cimis/metrics_test.go` (new files).

---

## Phase 2: Dependency & Configuration

### 2.1 Resolve `cimis-tsdb` dependency strategy

**Why:** The `replace` directive in `go.mod` pointing to `../../../cimis-tsdb`
means `go install` fails for any external user, CI needs a PAT to clone the
sibling repo, and contributors must manually set up the directory structure.

**Selected: Option C** — Keep `cimis-tsdb` private with clear documentation.

**Decision:** The storage layer contains proprietary optimization logic. We
maintain the `replace` directive for development and use CI/CD to build releases
that users install via Homebrew or direct download.

**Implementation:**
- [x] **Decision made:** Keep `cimis-tsdb` private (protect competitive advantage)
- [x] **Document setup:** Created `CONTRIBUTING.md` with contributor setup guide
- [x] **Document limitations:** Clarified pure-Go build scope (API client only)
- [x] **CI/CD maintained:** Release workflow handles private repo access via `GITHUB_TOKEN`

**Tradeoffs accepted:**
- `go install` won't work for external users (by design)
- Contributors need manual setup with replace directive
- Binary distribution via releases is the primary distribution method

**Scope:** `go.mod` (keep replace), `CONTRIBUTING.md` (new), CI workflows (unchanged).

### 2.2 Centralize hardcoded constants

**Why:** The epoch year (1985), API base URL, buffer sizes, and pool settings
are scattered across files. Centralizing them makes tuning and testing easier.

**What:**
- [x] Create a `const` block with `BaseURL`, `DailyDataItems`, `HourlyDataItems`,
      `EpochYear` and `Epoch` var in `internal/api/client.go`
- [x] Replace duplicated API URL in `client_streaming.go` with shared `BaseURL`
- [x] Replace hardcoded `1985` epoch in `query.go` with `api.Epoch`
- [x] Add `SetBaseURL()` method to Client for testing against mock servers

**Note:** Added `SetBaseURL()` method rather than CLI flag since mock server testing
is primarily done at the API client level in tests (using `httptest.NewServer`).
The method enables test scenarios without cluttering the CLI interface.

**Scope:** `internal/api/client.go`.

### 2.3 Improve error context

**Why:** Errors like "No data found" don't tell the user whether the station ID
is wrong, the date range is empty, or the API returned an error.

**What:**
- [x] Wrap errors with `fmt.Errorf("...: %w", err)` to build context chains
- [x] Include station ID, date range, and HTTP status in fetch errors
- [x] Add suggestions for common failure modes (invalid key, rate limit, no data)
      via `apiError()` helper with hints for 401/403, 429, and 5xx

**Scope:** `internal/api/client.go`, `cmd/cimis/main.go`.

---

## Phase 3: Polish

### 3.1 Document and CI-test the pure-Go build path

**Why:** `make build-pure` exists but isn't tested in CI or mentioned in the
README. Users without a C compiler have no guidance.

**What:**
- [x] Add a CI matrix entry that builds with `CGO_ENABLED=0`
- [x] Document the pure-Go build in the README
- [x] Fix `make build-pure` (was using `CGO_ENABLED=1` by mistake)
- [x] Verify pure-Go tests pass for `internal/api` package
- [x] Document limitation: pure-Go only works for API client, not full CLI

**Clarification:** The pure-Go build only compiles `internal/api` (no CGO/SQLite
dependencies). The full CLI requires CGO because `cimis-tsdb` uses SQLite for
storage. Updated README and CONTRIBUTING.md to reflect this accurately.

**Verified:** `CGO_ENABLED=0 go test ./internal/api/...` passes all 14 test cases.

**Scope:** `.github/workflows/ci.yml`, `README.md`, `CONTRIBUTING.md`.

### 3.2 Consolidate documentation

**Why:** There are 8 markdown files beyond the README. Some overlap (e.g.,
`DEPLOYMENT_READY.md` and `DEPLOYMENT_COMPLETE.md`) and some are reference
material that could live in a `docs/` directory.

**What:**
- [x] Move supplementary docs into `docs/` directory
- [x] Merge `DEPLOYMENT_READY.md` and `DEPLOYMENT_COMPLETE.md` into `docs/deployment.md`
- [x] Update `DOCS.md` as index linking to `docs/` files
- [x] Link from README to docs

**Scope:** Top-level markdown files, `README.md`.

---

## Summary

All improvements completed!

| Phase | Item | Status | Notes |
|-------|------|--------|-------|
| 1.1 | ~~Split `main.go`~~ | ✅ Done | 6 command files extracted |
| 1.2 | ~~Unit tests for API client~~ | ✅ Done | 14 tests, 528 lines |
| 1.3 | ~~Unit tests for utilities~~ | ✅ Done | Cache size, metrics, retry tests |
| 2.1 | ~~Resolve `cimis-tsdb` dependency~~ | ✅ Done | Option C selected - keep private with docs |
| 2.2 | ~~Centralize constants~~ | ✅ Done | `BaseURL`, `Epoch`, data items |
| 2.3 | ~~Improve error context~~ | ✅ Done | Wrapped errors with hints |
| 3.1 | ~~Pure-Go build in CI~~ | ✅ Done | API tests pass with `CGO_ENABLED=0` |
| 3.2 | ~~Consolidate docs~~ | ✅ Done | Moved to `docs/` directory |

**New files created:**
- `CONTRIBUTING.md` - Contributor setup guide
- `cmd/cimis/{fetch,ingest,init,query,stats,verify}.go` - Split commands
- `cmd/cimis/{main,metrics}_test.go` - CLI tests
- `internal/api/client_test.go` - API client tests
- `docs/{benchmarks,deployment,github-pat-setup,optimization,private-dependency,production-hardening,streaming-client}.md`

**Key decisions:**
1. **Dependency:** Keep `cimis-tsdb` private (Option C) - protects proprietary storage logic
2. **Distribution:** Binary releases via GitHub + Homebrew (not `go install`)
3. **Pure-Go:** Limited to `internal/api` (full CLI requires CGO/SQLite)
