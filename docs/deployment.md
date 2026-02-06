# Deployment History: Streaming Client

> Historical milestone document from the initial deployment of the streaming client.

## Timeline

- **Implemented**: `fetch-streaming` command with streaming JSON decode, worker pools, and detailed metrics
- **Deployed**: `ingest` command switched to streaming client as default
- **Deprecated**: Old `fetch` command (still functional, shows warning)

## Key Results

| Metric | Value |
|--------|-------|
| Throughput | 288 rec/s |
| Compression | 2.68x ratio |
| Fetch latency | 1.15s |
| Memory/record | 469 bytes |
| Memory reduction | 60-70% vs buffered approach |
| JSON decode | ~2x faster (streaming vs unmarshal) |

## Architecture

```
ingest / fetch-streaming
  -> OptimizedClient (HTTP/2, connection pooling)
    -> Streaming JSON decode (json.Decoder)
      -> Zstd compression (2.68x ratio)
        -> Atomic writes (temp + rename)
```

## Features

- Streaming JSON decode via `json.Decoder` (no `io.ReadAll` + `json.Unmarshal`)
- Per-phase timing: DNS, TCP, TLS, TTFB, body read, JSON decode, chunk write
- Exponential backoff with jitter for 429/5xx errors
- Configurable worker pool for parallel station fetching
- Dry-run mode, gzip support, deterministic output ordering
