# Simple Web Crawler

An asynchronous Python web crawler that starts from a given webpage/domain and discovers all reachable, unique HTML sub-pages.

## Features

- **Same-domain by default**: Automatically restricts crawling (including HTTP redirects) to the starting domain.
- **Optional external domain crawling**: Pass `--include-external` (`-e`) to follow external domains with configurable depth and page limits.
- **Concurrent & fast**: Uses `asyncio` and `httpx` connection pooling for concurrent page fetching.
- **Deduplication & normalization**: Resolves relative paths and `<base href>`, strips URL fragments (`#...`), normalizes ports/paths, and handles link cycles cleanly.
- **Flexible output**: Output either visited pages (`--visited-only`, default) or pages that failed to fetch (`-f` / `--failed-only`) to `stdout` or a file (`-o`).

## Requirements

- Python 3.10+
- `httpx` (`pip install -r requirements.txt`)

## Usage

Print all reachable visited pages on a domain to the screen:

```bash
python3 crawler.py https://example.com --visited-only
```

Output only broken / failed-to-fetch URLs:

```bash
python3 crawler.py https://example.com --failed-only
```

Save visited URLs to a file:

```bash
python3 crawler.py https://example.com -o visited_urls.txt
```

Include external domains (defaults to `--max-depth 2` and `--max-pages 200` unless overridden):

```bash
python3 crawler.py https://example.com --include-external --max-depth 3 --max-pages 500 -o all_urls.txt
```

### CLI Options

| Flag | Description |
| --- | --- |
| `url` | Starting webpage or domain (e.g. `https://example.com` or `example.com`) |
| `-e`, `--include-external` | Follow links to external domains (defaults to depth `2`, max `200` pages) |
| `--include-subdomains` | Treat subdomains of the starting domain as internal |
| `--visited-only` | Output only successfully visited pages (default) |
| `-f`, `--failed-only` | Output only pages that failed to fetch (HTTP errors, timeouts, connection errors) |
| `-o FILE`, `--output FILE` | Write output URLs to `FILE` instead of printing to `stdout` |
| `-d N`, `--max-depth N` | Maximum link depth from start URL (`0` = start page only, `-1` = unlimited) |
| `-m N`, `--max-pages N` | Maximum number of unique pages to visit (-1 = unlimited) |
| `-c N`, `--concurrency N` | Number of concurrent HTTP requests (default: `5`) |
| `-t SEC`, `--timeout SEC` | Per-request timeout in seconds (default: `20.0`) |
| `-r N`, `--retries N` | Retries on timeouts and transient HTTP/network errors (default: `3`) |
| `--user-agent STR` | Custom `User-Agent` header string |
| `-q`, `--quiet` | Suppress progress logs on `stderr` |
