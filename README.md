# CIMIS CLI Tool

A command-line tool for fetching, storing, and querying California Irrigation Management Information System (CIMIS) weather data.

Built on top of the [cimis-tsdb](https://github.com/dl-alexandre/cimis-tsdb) high-performance time-series storage library.

## Features

- **Fetch Data**: Download weather data from CIMIS API with streaming and retry support
- **Local Storage**: Store data in optimized compressed format for fast offline access
- **Query**: Fast queries with optional caching and performance metrics
- **Batch Operations**: Process multiple stations in parallel with worker pools
- **Monitoring**: Built-in performance profiling and cache statistics

## Installation

```bash
go install github.com/dl-alexandre/cimis-cli/cmd/cimisdb@latest
```

Or build from source:

```bash
git clone https://github.com/dl-alexandre/cimis-cli
cd cimis-cli
make build
```

## Setup

1. Get a CIMIS API key from [CIMIS Web Services](https://cimis.water.ca.gov/Default.aspx)

2. Set your API key:
   ```bash
   export CIMIS_APP_KEY=your-api-key-here
   ```

3. Initialize the database:
   ```bash
   ./build/cimisdb init
   ```

## Usage

### Basic Commands

```bash
# Show version
cimisdb version

# Fetch recent data for a station (doesn't store)
cimisdb fetch -station 2 -days 30

# Fetch and store data for a year
cimisdb ingest -station 2 -year 2023

# Query stored data
cimisdb query -station 2 -start 2023-06-01 -end 2023-06-30

# Show database statistics
cimisdb stats

# Verify chunk integrity
cimisdb verify
```

### Advanced Features

#### Streaming Fetch (Production-Ready)

Fetch multiple stations in parallel with detailed metrics:

```bash
# Fetch multiple stations for 2024
cimisdb fetch-streaming \
  -stations 2,5,10,15 \
  -year 2024 \
  -concurrency 8 \
  -perf

# Fetch station range
cimisdb fetch-streaming \
  -stations 1-20 \
  -year 2024 \
  -concurrency 10 \
  -retries 3
```

#### Query with Caching

Enable caching for repeated queries:

```bash
cimisdb query \
  -station 2 \
  -start 2023-01-01 \
  -end 2023-12-31 \
  -cache 100MB \
  -perf
```

Output includes cache statistics:
```
=== Cache Statistics ===
Total Size: 52.3 MB / 100.0 MB (52.3%)
Entries: 12
Hits: 8
Misses: 4
Hit Rate: 66.67%
Evictions: 0
```

#### Optimized Ingestion

Use the optimized ingest command for better performance:

```bash
cimisdb ingest-opt \
  -stations 1-100 \
  -year 2024 \
  -concurrency 16 \
  -compression 3
```

#### Performance Profiling

Profile CPU and memory usage:

```bash
cimisdb profile \
  -station 2 \
  -start 2023-01-01 \
  -end 2023-12-31 \
  -cpu-profile cpu.prof \
  -mem-profile mem.prof
```

## Commands Reference

| Command | Description |
|---------|-------------|
| `version` | Show version information |
| `init` | Initialize database directories and metadata |
| `fetch` | Fetch data from CIMIS API (testing only) |
| `fetch-streaming` | Production fetch with streaming and retries |
| `ingest` | Fetch and store data in compressed chunks |
| `ingest-opt` | Optimized batch ingestion |
| `query` | Query stored data with filtering |
| `stats` | Show database statistics |
| `verify` | Verify chunk integrity |
| `profile` | Performance profiling |

## Configuration

### Global Flags

- `-data-dir string` - Data directory (default: `./data`)
- `-app-key string` - CIMIS API app key (or `CIMIS_APP_KEY` env var)

### Query Flags

- `-station int` - Station ID (required)
- `-start string` - Start date `YYYY-MM-DD`
- `-end string` - End date `YYYY-MM-DD`
- `-hourly` - Query hourly data (default: daily)
- `-cache string` - Cache size (e.g., `100MB`, `1GB`)
- `-perf` - Show performance metrics

### Fetch Streaming Flags

- `-stations string` - CSV list or range (e.g., `2,5,10` or `1-10`)
- `-year int` - Year to fetch
- `-start string` - Start date `MM/DD/YYYY` (overrides year)
- `-end string` - End date `MM/DD/YYYY` (overrides year)
- `-concurrency int` - Worker pool size (default: 4)
- `-retries int` - Max retries on failure (default: 3)
- `-perf` - Print detailed metrics
- `-dry-run` - Fetch without storing

## Data Directory Structure

```
data/
├── metadata.sqlite3        # Station info and chunk index
└── stations/
    ├── 002/                # Station 002
    │   ├── 2020_daily.zst  # Compressed daily data
    │   ├── 2021_daily.zst
    │   └── 2020_hourly.zst # Compressed hourly data
    └── 005/                # Station 005
        └── ...
```

## Performance Tips

1. **Use streaming fetch for production**: `fetch-streaming` handles retries and provides detailed metrics
2. **Enable caching for repeated queries**: `-cache 100MB` significantly speeds up re-queries
3. **Batch operations**: Process multiple stations in parallel with `-concurrency`
4. **Compression levels**: Higher compression (3-5) for storage, lower (1) for speed
5. **Verify integrity**: Run `verify` periodically to check for corrupted chunks

## Storage Library

This CLI uses the [cimis-tsdb](https://github.com/dl-alexandre/cimis-tsdb) library for storage.

If you need programmatic access or want to build custom tools, use the library directly:

```go
import "github.com/dl-alexandre/cimis-tsdb/storage"
```

See the [library documentation](https://github.com/dl-alexandre/cimis-tsdb#readme) for API details.

## Examples

### Daily Workflow

```bash
# Fetch today's data for station 2
cimisdb fetch-streaming -stations 2 -start $(date -v-1d +%m/%d/%Y) -end $(date +%m/%d/%Y)

# Query last 7 days
cimisdb query -station 2 -start $(date -v-7d +%Y-%m-%d) -end $(date +%Y-%m-%d)
```

### Historical Data Ingestion

```bash
# Fetch all available stations for 2020-2024
for year in {2020..2024}; do
  cimisdb fetch-streaming \
    -stations 1-100 \
    -year $year \
    -concurrency 20 \
    -retries 5 \
    -perf
done
```

### Data Analysis

```bash
# Export to CSV for analysis
cimisdb query -station 2 -start 2023-01-01 -end 2023-12-31 > station2_2023.csv
```

## Troubleshooting

**API rate limits**: CIMIS API limits concurrent requests. Reduce `-concurrency` if you see 429 errors.

**Out of memory**: Reduce cache size or batch size for large queries.

**Corrupted chunks**: Run `cimisdb verify` to identify and remove corrupted files.

## License

MIT

## Links

- [CIMIS Website](https://cimis.water.ca.gov/)
- [CIMIS API Documentation](https://cimis.water.ca.gov/WSNReportCriteria.aspx)
- [cimis-tsdb Library](https://github.com/dl-alexandre/cimis-tsdb)
