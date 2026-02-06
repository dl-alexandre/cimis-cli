# Contributing to cimis-cli

This guide covers how to set up your development environment and contribute to cimis-cli.

## Quick Start for Contributors

### Prerequisites

- Go 1.21 or later
- C compiler (for SQLite support via CGO)
- CIMIS API key (for testing fetches)

### Repository Setup

Since `cimis-tsdb` is a private dependency, you'll need to set up the local replace directive:

```bash
# 1. Clone both repositories to the same parent directory
mkdir -p ~/projects/cimis
cd ~/projects/cimis
git clone https://github.com/dl-alexandre/cimis-cli.git
git clone https://github.com/dl-alexandre/cimis-tsdb.git

# 2. The go.mod already has the replace directive:
# replace github.com/dl-alexandre/cimis-tsdb => ../../../cimis-tsdb
# This assumes both repos are in the same parent directory

# 3. Build
cd cimis-cli
make build

# 4. Test (requires CIMIS_APP_KEY)
export CIMIS_APP_KEY=your-api-key-here
make test
```

**Directory Structure:**
```
~/projects/cimis/
├── cimis-cli/          # This repository
└── cimis-tsdb/         # Private dependency (sibling directory)
```

## Understanding the Build Modes

### Standard Build (CGO Required)

The full CLI with all storage capabilities:

```bash
make build
```

**Requirements:**
- C compiler (gcc/clang)
- CGO enabled (default)
- Access to cimis-tsdb (via replace directive)

### Pure-Go Build (Limited)

Builds only the API client packages without storage:

```bash
make build-pure
# Or manually:
CGO_ENABLED=0 go build ./internal/api/...
```

**Limitations:**
- Only builds `internal/api` package
- Cannot build full CLI (requires cimis-tsdb storage which uses CGO/SQLite)
- Useful for testing API client logic without database dependencies

**Note:** The README mentions "pure-Go build retains all CLI functionality" - this refers to a future state where storage might be abstracted. Currently, the pure-Go build only applies to the API client package.

## Testing

### API Tests (Pure-Go, No Database Required)

```bash
CGO_ENABLED=0 go test ./internal/api/... -v
```

These tests:
- Use mock HTTP servers (no live API calls)
- Test date parsing, record conversion, QC flags
- Run in CI without CIMIS_APP_KEY

### Full Test Suite (Requires CGO + API Key)

```bash
export CIMIS_APP_KEY=your-api-key-here
make test
```

Includes:
- Unit tests
- Integration tests (make real API calls)
- Database operations

## Dependency Strategy

We intentionally keep `cimis-tsdb` private because:

1. **Competitive Advantage**: The storage/optimization logic is proprietary
2. **Binary Distribution**: Users get functionality via releases without source access
3. **CI/CD Security**: GitHub Actions builds releases with controlled access

### For End Users

**Install from releases (Recommended):**
```bash
# macOS/Linux
brew install dl-alexandre/tap/cimis

# Or download directly
curl -L https://github.com/dl-alexandre/cimis-cli/releases/latest/download/cimis-$(uname -s)-$(uname -m) -o cimis
chmod +x cimis
```

**Why not `go install`?**
The private dependency prevents `go install github.com/dl-alexandre/cimis-cli/cmd/cimis@latest` from working. This is by design - we want to control distribution through signed binaries rather than source.

### For Contributors

You need access to both repositories. Contact the maintainer for `cimis-tsdb` access, then follow the setup above with the replace directive.

## Release Process

Maintainers only - requires access to private `cimis-tsdb`:

```bash
# 1. Update version in code
# 2. Commit and push
# 3. Tag and push (triggers release workflow)
git tag v0.0.2
git push origin v0.0.2
```

The GitHub Actions workflow:
1. Checks out both repos (uses GITHUB_TOKEN for private access)
2. Builds for 6 platforms (darwin/linux/windows × arm64/amd64)
3. Signs macOS binaries
4. Creates GitHub release with checksums
5. Updates Homebrew tap

## Troubleshooting

### "cannot find module" errors

The replace directive expects `cimis-tsdb` at `../../../cimis-tsdb` (two levels up from `cimis-cli/go.mod`).

**Verify your directory structure:**
```bash
ls ../../../cimis-tsdb/go.mod  # Should exist
```

**If you cloned to different locations**, update the replace path:
```bash
# Example: if cimis-tsdb is at ~/work/cimis-tsdb
go mod edit -replace github.com/dl-alexandre/cimis-tsdb=~/work/cimis-tsdb
```

### "sqlite3: library not found" errors

Install a C compiler:
```bash
# macOS
xcode-select --install

# Ubuntu/Debian
sudo apt-get install gcc libsqlite3-dev

# Windows (MinGW)
choco install mingw
```

### API rate limits during testing

The test suite makes real API calls. CIMIS API limits:
- 1000 requests per day per key
- Rate limiting applies

Use `-short` flag for limited tests:
```bash
go test -short ./...
```

## Questions?

- **Issues**: [github.com/dl-alexandre/cimis-cli/issues](https://github.com/dl-alexandre/cimis-cli/issues)
- **Discussions**: Use GitHub Discussions for questions about architecture or design decisions
- **Private repo access**: Contact maintainer for `cimis-tsdb` contributor access

## License

MIT - See LICENSE file for details.
