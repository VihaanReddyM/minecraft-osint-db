from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse


@dataclass(frozen=True)
class ProxyConfig:
    """Immutable proxy endpoint configuration.

    Use `from_url()` to parse a URL string like 'socks5://user:pass@host:port'.
    `httpx_url()` returns the string that httpx.Client(proxy=...) accepts.
    """

    host: str
    port: int
    protocol: str = "socks5"
    username: str | None = None
    password: str | None = None

    def httpx_url(self) -> str:
        auth = f"{self.username}:{self.password}@" if self.username else ""
        return f"{self.protocol}://{auth}{self.host}:{self.port}"

    @classmethod
    def from_url(cls, url: str) -> ProxyConfig:
        """Parse a proxy URL such as 'socks5://user:pass@host:1080'."""
        parsed = urlparse(url.strip())
        protocol = parsed.scheme or "socks5"
        host = parsed.hostname
        port = parsed.port
        if not host:
            raise ValueError(f"Cannot parse proxy host from URL: {url!r}")
        if not port:
            raise ValueError(f"Cannot parse proxy port from URL: {url!r}")
        return cls(
            host=host,
            port=port,
            protocol=protocol,
            username=parsed.username or None,
            password=parsed.password or None,
        )


class ProxyProvider(Protocol):
    """Protocol for objects that vend ProxyConfig instances to workers.

    `get_proxy(worker_id)` is called once per worker at startup.
    Implementations may use round-robin, VPN mapping, or any other strategy.
    """

    def get_proxy(self, worker_id: int) -> ProxyConfig | None: ...
    def all_proxies(self) -> list[ProxyConfig]: ...


@dataclass
class StaticProxyPool:
    """Round-robin assignment from a fixed list of proxy endpoints.

    Worker 0 → proxies[0], worker 1 → proxies[1], ..., worker N → proxies[N % len].
    When `proxies` is empty, `get_proxy()` returns None (no proxy routing).
    """

    proxies: list[ProxyConfig]

    def get_proxy(self, worker_id: int) -> ProxyConfig | None:
        if not self.proxies:
            return None
        return self.proxies[worker_id % len(self.proxies)]

    def all_proxies(self) -> list[ProxyConfig]:
        return list(self.proxies)

    @classmethod
    def from_urls(cls, urls: list[str]) -> StaticProxyPool:
        return cls(proxies=[ProxyConfig.from_url(u) for u in urls])

    @classmethod
    def from_file(cls, path: Path) -> StaticProxyPool:
        """Load proxy URLs from a text file (one per line, # comments supported)."""
        lines = path.read_text(encoding="utf-8").splitlines()
        urls = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
        return cls.from_urls(urls)


class NoProxyPool:
    """Sentinel pool that returns None for all workers (direct outbound connections)."""

    def get_proxy(self, worker_id: int) -> ProxyConfig | None:
        return None

    def all_proxies(self) -> list[ProxyConfig]:
        return []
