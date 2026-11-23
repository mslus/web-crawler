#!/usr/bin/env python3
"""Simple asynchronous web crawler with domain filtering and configurable limits."""

from __future__ import annotations

import argparse
import asyncio
import posixpath
import random
import sys
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # 10 MB safety limit per HTML page
MAX_REDIRECTS = 10
MAX_RETRY_AFTER_SECONDS = 15.0
HTML_CONTENT_TYPES = {"", "text/html", "application/xhtml+xml"}
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 508}
DEFAULT_EXTERNAL_MAX_DEPTH = 2
DEFAULT_EXTERNAL_MAX_PAGES = 200
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


class LinkExtractor(HTMLParser):
    """Extracts hyperlinks from HTML documents while respecting <base href>."""

    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.base_url: str | None = None
        self.raw_links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        if tag_lower == "base" and self.base_url is None:
            for attr_name, attr_val in attrs:
                if attr_name.lower() == "href" and attr_val:
                    self.base_url = urljoin(self.page_url, attr_val.strip())
                    break
        elif tag_lower in ("a", "area"):
            for attr_name, attr_val in attrs:
                if attr_name.lower() == "href" and attr_val:
                    self.raw_links.append(attr_val.strip())
                    break

    def resolved_links(self) -> list[str]:
        effective_base = self.base_url or self.page_url
        links: list[str] = []
        for href in self.raw_links:
            resolved = normalize_url(urljoin(effective_base, href))
            if resolved is not None:
                links.append(resolved)
        return links


def normalize_seed_url(raw_url: str) -> str:
    """Normalize user-supplied starting URL or bare domain."""
    candidate = raw_url.strip()
    if not candidate:
        raise ValueError("Starting URL cannot be empty.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    normalized = normalize_url(candidate)
    if normalized is None:
        raise ValueError(f"Invalid HTTP/HTTPS URL: {raw_url!r}")
    return normalized


def normalize_url(raw_url: str) -> str | None:
    """Canonicalize an HTTP/HTTPS URL and strip fragments; return None for non-HTTP URLs."""
    candidate = raw_url.strip()
    if not candidate:
        return None

    try:
        parts = urlsplit(candidate)
    except ValueError:
        return None

    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return None

    hostname = parts.hostname
    if not hostname:
        return None
    hostname = hostname.lower()

    try:
        port = parts.port
    except ValueError:
        return None

    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None

    if ":" in hostname and not hostname.startswith("["):
        host_part = f"[{hostname}]"
    else:
        host_part = hostname
    netloc = f"{host_part}:{port}" if port is not None else host_part

    raw_path = parts.path or "/"
    preserve_trailing_slash = (
        raw_path.endswith("/")
        or raw_path.endswith("/.")
        or raw_path.endswith("/..")
        or raw_path in (".", "..")
    )
    clean_input_path = "/" + raw_path.lstrip("/")
    norm_path = posixpath.normpath(clean_input_path)
    if preserve_trailing_slash and not norm_path.endswith("/"):
        norm_path += "/"

    return urlunsplit((scheme, netloc, norm_path, parts.query, ""))


def _extract_hostname(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def _strip_www(hostname: str) -> str:
    return hostname[4:] if hostname.startswith("www.") else hostname


@dataclass(slots=True)
class CrawlConfig:
    """Configuration options for WebCrawler."""

    start_url: str
    include_external: bool = False
    include_subdomains: bool = False
    max_depth: int | None = None
    max_pages: int | None = None
    concurrency: int = 5
    timeout: float = 20.0
    max_retries: int = 3
    retry_backoff: float = 0.75
    user_agent: str = DEFAULT_USER_AGENT
    quiet: bool = False


class WebCrawler:
    """Asynchronous web crawler supporting internal-only and external domain traversal."""

    def __init__(
        self,
        config: CrawlConfig,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self._transport = transport
        self.seed_url = normalize_seed_url(config.start_url)
        self.seed_host = _extract_hostname(self.seed_url)
        base_host = _strip_www(self.seed_host)
        self.allowed_hosts: set[str] = {
            self.seed_host,
            base_host,
            f"www.{base_host}",
        }

        self._best_depth: dict[str, int] = {}
        self._fetched_urls: set[str] = set()
        self._page_links: dict[str, list[str]] = {}
        self.visited_urls: list[str] = []
        self._visited_set: set[str] = set()
        self.failed_urls: list[str] = []
        self._failed_set: set[str] = set()
        self.failed_reasons: dict[str, str] = {}
        self._stop_event = asyncio.Event()

    def is_url_allowed(self, url: str) -> bool:
        """Return True if the URL's domain is permitted under the active configuration."""
        if self.config.include_external:
            return True

        host = _extract_hostname(url)
        if not host:
            return False
        if host in self.allowed_hosts:
            return True
        if self.config.include_subdomains:
            base = _strip_www(self.seed_host)
            if host == base or host.endswith(f".{base}"):
                return True
        return False

    def _log(self, message: str) -> None:
        if not self.config.quiet:
            print(message, file=sys.stderr)

    def _record_failure(self, url: str, depth: int, reason: str) -> None:
        if self._stop_event.is_set() or url in self._failed_set:
            return
        self._failed_set.add(url)
        self.failed_urls.append(url)
        self.failed_reasons[url] = reason
        self._log(f"[fail] Failed (depth {depth}): {url} ({reason})")

    async def _sleep_backoff(self, attempt: int, retry_after: str | None = None) -> None:
        delay: float | None = None
        if retry_after:
            try:
                parsed = float(retry_after.strip())
                if parsed >= 0:
                    delay = min(parsed, MAX_RETRY_AFTER_SECONDS)
            except ValueError:
                pass
        if delay is None:
            base = self.config.retry_backoff * (2**attempt)
            delay = base + random.uniform(0.0, max(0.05, base * 0.25))
        if delay <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except TimeoutError:
            pass

    def _should_enqueue(self, url: str, depth: int) -> bool:
        prev_depth = self._best_depth.get(url)
        if prev_depth is not None:
            if self.config.max_depth is None or prev_depth <= depth:
                return False
        self._best_depth[url] = depth
        return True

    async def crawl(self) -> list[str]:
        """Run the crawl starting from seed_url and return all unique visited page URLs."""
        queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        self._best_depth[self.seed_url] = 0
        queue.put_nowait((self.seed_url, 0))

        limits = httpx.Limits(
            max_connections=max(self.config.concurrency * 2, 20),
            max_keepalive_connections=self.config.concurrency,
        )
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        timeout = httpx.Timeout(self.config.timeout)

        async with httpx.AsyncClient(
            limits=limits,
            headers=headers,
            timeout=timeout,
            follow_redirects=False,
            transport=self._transport,
        ) as client:
            workers = [
                asyncio.create_task(self._worker(client, queue))
                for _ in range(max(1, self.config.concurrency))
            ]
            await queue.join()
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        return list(self.visited_urls)

    async def _worker(
        self,
        client: httpx.AsyncClient,
        queue: asyncio.Queue[tuple[str, int]],
    ) -> None:
        while True:
            url, depth = await queue.get()
            try:
                if self._stop_event.is_set():
                    continue
                if (
                    self.config.max_pages is not None
                    and len(self.visited_urls) >= self.config.max_pages
                ):
                    self._stop_event.set()
                    continue

                if url in self._page_links:
                    final_url = url
                    links = self._page_links[url]
                else:
                    if url in self._fetched_urls:
                        continue
                    self._fetched_urls.add(url)
                    result = await self._fetch_html(client, url, depth)
                    if result is None:
                        continue

                    final_url, html_text = result
                    if self._stop_event.is_set():
                        continue

                    if final_url not in self._visited_set:
                        if (
                            self.config.max_pages is not None
                            and len(self.visited_urls) >= self.config.max_pages
                        ):
                            self._stop_event.set()
                            continue

                        self._visited_set.add(final_url)
                        self.visited_urls.append(final_url)
                        self._log(
                            f"[{len(self.visited_urls)}] Visited (depth {depth}): {final_url}"
                        )

                        if (
                            self.config.max_pages is not None
                            and len(self.visited_urls) >= self.config.max_pages
                        ):
                            self._stop_event.set()
                            continue

                    links = self._extract_links(final_url, html_text)
                    if self.config.max_depth is not None:
                        self._page_links[final_url] = links
                        if url != final_url:
                            self._page_links[url] = links

                if self.config.max_depth is not None and depth >= self.config.max_depth:
                    continue

                next_depth = depth + 1
                for link in links:
                    if self._stop_event.is_set():
                        break
                    if not self.is_url_allowed(link):
                        continue
                    if self._should_enqueue(link, next_depth):
                        queue.put_nowait((link, next_depth))
            finally:
                queue.task_done()

    async def _fetch_html(
        self,
        client: httpx.AsyncClient,
        url: str,
        depth: int,
    ) -> tuple[str, str] | None:
        """Fetch URL following allowed redirects; return (final_url, html) if HTML page."""
        current_url = url
        redirect_chain: set[str] = {url}
        max_attempts = max(1, self.config.max_retries + 1)

        for _ in range(MAX_REDIRECTS + 1):
            if self._stop_event.is_set():
                return None

            redirected = False
            for attempt in range(max_attempts):
                if self._stop_event.is_set():
                    return None

                attempt_timeout = httpx.Timeout(
                    self.config.timeout * (1.0 + 0.5 * attempt)
                )
                try:
                    async with client.stream(
                        "GET", current_url, timeout=attempt_timeout
                    ) as response:
                        if response.is_redirect and "location" in response.headers:
                            raw_target = urljoin(
                                current_url, response.headers["location"]
                            )
                            next_url = normalize_url(raw_target)
                            if next_url is None:
                                try:
                                    scheme = urlsplit(raw_target.strip()).scheme.lower()
                                except ValueError:
                                    scheme = "http"
                                if scheme in ("", "http", "https"):
                                    self._record_failure(
                                        current_url, depth, "Invalid redirect location"
                                    )
                                return None

                            if depth == 0:
                                next_host = _extract_hostname(next_url)
                                if _strip_www(next_host) == _strip_www(self.seed_host):
                                    self.allowed_hosts.add(next_host)

                            if not self.is_url_allowed(next_url):
                                return None
                            if next_url in redirect_chain:
                                self._record_failure(url, depth, "Redirect loop")
                                return None
                            redirect_chain.add(next_url)
                            if not self._should_enqueue(next_url, depth):
                                return None
                            if next_url in self._fetched_urls:
                                return None
                            self._fetched_urls.add(next_url)
                            current_url = next_url
                            redirected = True
                            break

                        if response.status_code in RETRYABLE_STATUS_CODES:
                            if attempt + 1 < max_attempts:
                                retry_after = response.headers.get("retry-after")
                                await self._sleep_backoff(attempt, retry_after)
                                continue
                            self._record_failure(
                                current_url, depth, f"HTTP {response.status_code}"
                            )
                            return None

                        if response.status_code < 200 or response.status_code >= 300:
                            self._record_failure(
                                current_url, depth, f"HTTP {response.status_code}"
                            )
                            return None

                        content_type = (
                            response.headers.get("content-type", "")
                            .split(";")[0]
                            .strip()
                            .lower()
                        )
                        if content_type not in HTML_CONTENT_TYPES:
                            return None

                        chunks: list[bytes] = []
                        total_bytes = 0
                        async for chunk in response.aiter_bytes():
                            chunks.append(chunk)
                            total_bytes += len(chunk)
                            if total_bytes >= MAX_RESPONSE_BYTES:
                                break

                        raw_bytes = b"".join(chunks)
                        encoding = response.encoding or "utf-8"
                        try:
                            html_text = raw_bytes.decode(encoding, errors="replace")
                        except LookupError:
                            html_text = raw_bytes.decode("utf-8", errors="replace")
                        return current_url, html_text
                except httpx.HTTPError as exc:
                    if self._stop_event.is_set():
                        return None
                    if attempt + 1 < max_attempts:
                        await self._sleep_backoff(attempt)
                        continue
                    self._record_failure(
                        current_url, depth, str(exc) or type(exc).__name__
                    )
                    return None

            if not redirected:
                return None

        self._record_failure(url, depth, "Too many redirects")
        return None

    @staticmethod
    def _extract_links(page_url: str, html_text: str) -> list[str]:
        extractor = LinkExtractor(page_url)
        try:
            extractor.feed(html_text)
        except Exception:
            pass
        return extractor.resolved_links()


def build_arg_parser() -> argparse.ArgumentParser:
    examples = """\
Usage Examples:
  # Crawl a single domain and print all visited URLs to stdout
  python3 crawler.py https://example.com

  # Save visited pages to a file
  python3 crawler.py https://example.com -o visited.txt

  # Output only pages that failed to fetch (broken links / 4xx / 5xx / timeouts)
  python3 crawler.py https://example.com --failed-only
  python3 crawler.py https://example.com -f -o broken_links.txt

  # Explicitly output only visited pages and suppress stderr progress logs
  python3 crawler.py https://example.com --visited-only -q

  # Include subdomains of the target domain (e.g. blog.example.com)
  python3 crawler.py https://example.com --include-subdomains

  # Include external domains with custom depth and page limits
  python3 crawler.py https://example.com --include-external -d 2 -m 300 -o all_urls.txt

  # Tune concurrency, request timeout, and retry count
  python3 crawler.py https://example.com -c 8 -t 30 -r 4
"""
    parser = argparse.ArgumentParser(
        prog="crawler.py",
        description=(
            "Crawl a webpage/domain and discover all reachable unique HTML sub-pages.\n"
            "Excludes external domains by default."
        ),
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    positional_group = parser.add_argument_group("Positional Arguments")
    positional_group.add_argument(
        "url",
        help="Starting webpage or domain (e.g. https://example.com or example.com)",
    )

    scope_group = parser.add_argument_group("Scope & Limit Options")
    scope_group.add_argument(
        "-e",
        "--include-external",
        action="store_true",
        help=(
            "Include and crawl pages on external domains "
            f"(defaults to --max-depth {DEFAULT_EXTERNAL_MAX_DEPTH} and "
            f"--max-pages {DEFAULT_EXTERNAL_MAX_PAGES} unless overridden)"
        ),
    )
    scope_group.add_argument(
        "--include-subdomains",
        action="store_true",
        help="Allow subdomains of the starting domain when external domains are excluded",
    )
    scope_group.add_argument(
        "-d",
        "--max-depth",
        type=int,
        default=None,
        metavar="DEPTH",
        help="Maximum link depth from start URL (0 = start page only, -1 = unlimited)",
    )
    scope_group.add_argument(
        "-m",
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of unique pages to visit (-1 = unlimited)",
    )

    output_group = parser.add_argument_group("Output Options")
    mode_group = output_group.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--visited-only",
        action="store_true",
        help="Output only successfully visited pages (default)",
    )
    mode_group.add_argument(
        "-f",
        "--failed-only",
        action="store_true",
        help="Output only pages that failed to fetch (HTTP errors, timeouts, connection errors)",
    )
    output_group.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        metavar="FILE",
        help="Write output URLs to FILE instead of printing to stdout",
    )
    output_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and summary logs on stderr",
    )

    network_group = parser.add_argument_group("Network & Retry Options")
    network_group.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=5,
        metavar="N",
        help="Number of concurrent HTTP requests (default: 5)",
    )
    network_group.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=20.0,
        metavar="SEC",
        help="Per-request timeout in seconds (default: 20.0)",
    )
    network_group.add_argument(
        "-r",
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="Number of retries on timeouts and transient HTTP/network errors (default: 3)",
    )
    network_group.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        metavar="STR",
        help="Custom User-Agent header string",
    )

    general_group = parser.add_argument_group("General Options")
    general_group.add_argument(
        "-h",
        "--help",
        action="help",
        help="Show this help message with usage examples and exit",
    )

    return parser


def resolve_limits(
    include_external: bool,
    max_depth_arg: int | None,
    max_pages_arg: int | None,
) -> tuple[int | None, int | None]:
    """Resolve effective max_depth and max_pages values from CLI arguments."""
    if max_depth_arg is None:
        max_depth = DEFAULT_EXTERNAL_MAX_DEPTH if include_external else None
    elif max_depth_arg < 0:
        max_depth = None
    else:
        max_depth = max_depth_arg

    if max_pages_arg is None:
        max_pages = DEFAULT_EXTERNAL_MAX_PAGES if include_external else None
    elif max_pages_arg < 0:
        max_pages = None
    else:
        max_pages = max_pages_arg

    return max_depth, max_pages


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.max_pages == 0:
        parser.error("--max-pages must be at least 1 (or -1 for unlimited)")
    if args.retries < 0:
        parser.error("--retries cannot be negative")

    max_depth, max_pages = resolve_limits(
        include_external=args.include_external,
        max_depth_arg=args.max_depth,
        max_pages_arg=args.max_pages,
    )

    try:
        config = CrawlConfig(
            start_url=args.url,
            include_external=args.include_external,
            include_subdomains=args.include_subdomains,
            max_depth=max_depth,
            max_pages=max_pages,
            concurrency=args.concurrency,
            timeout=args.timeout,
            max_retries=args.retries,
            user_agent=args.user_agent,
            quiet=args.quiet,
        )
        crawler = WebCrawler(config)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if not args.quiet:
        scope = "all domains (external enabled)" if args.include_external else "same domain only"
        depth_desc = "unlimited" if max_depth is None else str(max_depth)
        pages_desc = "unlimited" if max_pages is None else str(max_pages)
        print(
            f"Starting crawl at {crawler.seed_url} "
            f"[scope: {scope}, max_depth: {depth_desc}, max_pages: {pages_desc}]",
            file=sys.stderr,
        )

    visited = asyncio.run(crawler.crawl())
    failed = crawler.failed_urls
    selected_urls = failed if args.failed_only else visited
    label = "failed" if args.failed_only else "visited"

    output_content = "\n".join(selected_urls) + ("\n" if selected_urls else "")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_content, encoding="utf-8")
        if not args.quiet:
            print(
                f"Done. Wrote {len(selected_urls)} unique {label} page(s) to {args.output} "
                f"(visited: {len(visited)}, failed: {len(failed)}).",
                file=sys.stderr,
            )
    else:
        sys.stdout.write(output_content)
        if not args.quiet:
            print(
                f"Done. Output {len(selected_urls)} unique {label} page(s) "
                f"(visited: {len(visited)}, failed: {len(failed)}).",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
