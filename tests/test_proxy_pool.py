from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from mcosint.proxy.pool import NoProxyPool, ProxyConfig, StaticProxyPool


class TestProxyConfig:
    def test_httpx_url_no_auth(self) -> None:
        p = ProxyConfig(host="127.0.0.1", port=1080)
        assert p.httpx_url() == "socks5://127.0.0.1:1080"

    def test_httpx_url_with_auth(self) -> None:
        p = ProxyConfig(host="10.0.0.1", port=1081, username="alice", password="secret")
        assert p.httpx_url() == "socks5://alice:secret@10.0.0.1:1081"

    def test_httpx_url_http_protocol(self) -> None:
        p = ProxyConfig(host="proxy.example.com", port=3128, protocol="http")
        assert p.httpx_url() == "http://proxy.example.com:3128"

    def test_from_url_socks5_no_auth(self) -> None:
        p = ProxyConfig.from_url("socks5://127.0.0.1:1080")
        assert p.host == "127.0.0.1"
        assert p.port == 1080
        assert p.protocol == "socks5"
        assert p.username is None
        assert p.password is None

    def test_from_url_socks5_with_auth(self) -> None:
        p = ProxyConfig.from_url("socks5://user:pass@10.0.0.1:9050")
        assert p.host == "10.0.0.1"
        assert p.port == 9050
        assert p.username == "user"
        assert p.password == "pass"

    def test_from_url_http(self) -> None:
        p = ProxyConfig.from_url("http://proxy.local:8080")
        assert p.protocol == "http"
        assert p.host == "proxy.local"
        assert p.port == 8080

    def test_from_url_roundtrip(self) -> None:
        url = "socks5://alice:secret@127.0.0.1:1080"
        p = ProxyConfig.from_url(url)
        assert p.httpx_url() == url

    def test_from_url_missing_host_raises(self) -> None:
        with pytest.raises(ValueError, match="proxy host"):
            ProxyConfig.from_url("socks5://:1080")

    def test_from_url_missing_port_raises(self) -> None:
        with pytest.raises(ValueError, match="proxy port"):
            ProxyConfig.from_url("socks5://127.0.0.1")

    def test_from_url_strips_whitespace(self) -> None:
        p = ProxyConfig.from_url("  socks5://127.0.0.1:1080  ")
        assert p.host == "127.0.0.1"


class TestStaticProxyPool:
    def _pool(self, n: int) -> StaticProxyPool:
        return StaticProxyPool(
            proxies=[ProxyConfig(host="127.0.0.1", port=1080 + i) for i in range(n)]
        )

    def test_empty_pool_returns_none(self) -> None:
        pool = StaticProxyPool(proxies=[])
        assert pool.get_proxy(0) is None
        assert pool.get_proxy(99) is None

    def test_single_proxy_all_workers_get_same(self) -> None:
        pool = self._pool(1)
        assert pool.get_proxy(0) == pool.get_proxy(1) == pool.get_proxy(100)
        assert pool.get_proxy(0).port == 1080  # type: ignore[union-attr]

    def test_round_robin_two_proxies(self) -> None:
        pool = self._pool(2)
        assert pool.get_proxy(0).port == 1080  # type: ignore[union-attr]
        assert pool.get_proxy(1).port == 1081  # type: ignore[union-attr]
        assert pool.get_proxy(2).port == 1080  # wraps
        assert pool.get_proxy(3).port == 1081

    def test_round_robin_five_proxies(self) -> None:
        pool = self._pool(5)
        for i in range(15):
            assert pool.get_proxy(i).port == 1080 + (i % 5)  # type: ignore[union-attr]

    def test_all_proxies_returns_copy(self) -> None:
        pool = self._pool(3)
        proxies = pool.all_proxies()
        assert len(proxies) == 3
        proxies.clear()
        assert len(pool.all_proxies()) == 3  # original unchanged

    def test_from_urls(self) -> None:
        pool = StaticProxyPool.from_urls(["socks5://127.0.0.1:1080", "socks5://127.0.0.1:1081"])
        assert len(pool.all_proxies()) == 2

    def test_from_file(self, tmp_path: Path) -> None:
        content = textwrap.dedent("""\
            # comment line
            socks5://127.0.0.1:1080
            socks5://127.0.0.1:1081

            # another comment
            socks5://127.0.0.1:1082
        """)
        f = tmp_path / "proxies.txt"
        f.write_text(content)
        pool = StaticProxyPool.from_file(f)
        assert len(pool.all_proxies()) == 3
        assert pool.get_proxy(0).port == 1080  # type: ignore[union-attr]
        assert pool.get_proxy(1).port == 1081
        assert pool.get_proxy(2).port == 1082

    def test_from_file_empty(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.txt"
        f.write_text("# just comments\n")
        pool = StaticProxyPool.from_file(f)
        assert pool.get_proxy(0) is None


class TestNoProxyPool:
    def test_always_returns_none(self) -> None:
        pool = NoProxyPool()
        assert pool.get_proxy(0) is None
        assert pool.get_proxy(999) is None

    def test_all_proxies_empty(self) -> None:
        assert NoProxyPool().all_proxies() == []
