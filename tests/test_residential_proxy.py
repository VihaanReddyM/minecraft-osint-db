from __future__ import annotations

from mcosint.proxy.residential import ResidentialProxyProvider
from mcosint.proxy.pool import ProxyConfig


class TestResidentialProxyProvider:
    def test_empty_returns_none(self) -> None:
        p = ResidentialProxyProvider()
        assert p.get_proxy(0) is None

    def test_single_proxy_all_workers(self) -> None:
        p = ResidentialProxyProvider.from_urls(["socks5://127.0.0.1:1080"])
        assert p.get_proxy(0).port == 1080
        assert p.get_proxy(99).port == 1080

    def test_round_robin(self) -> None:
        p = ResidentialProxyProvider.from_urls([
            "socks5://127.0.0.1:1080",
            "socks5://127.0.0.1:1081",
        ])
        assert p.get_proxy(0).port == 1080
        assert p.get_proxy(1).port == 1081
        assert p.get_proxy(2).port == 1080

    def test_all_proxies_copy(self) -> None:
        p = ResidentialProxyProvider.from_urls(["socks5://127.0.0.1:1080"])
        proxies = p.all_proxies()
        proxies.clear()
        assert len(p.all_proxies()) == 1

    def test_from_urls_creates_proxy_configs(self) -> None:
        p = ResidentialProxyProvider.from_urls([
            "socks5://10.0.0.1:9050",
            "socks5://10.0.0.2:9050",
        ])
        assert len(p.all_proxies()) == 2
        assert all(isinstance(px, ProxyConfig) for px in p.all_proxies())

    def test_round_robin_wraps_correctly(self) -> None:
        urls = [f"socks5://127.0.0.1:{1080 + i}" for i in range(3)]
        p = ResidentialProxyProvider.from_urls(urls)
        for i in range(9):
            assert p.get_proxy(i).port == 1080 + (i % 3)

    def test_default_factory_is_empty_list(self) -> None:
        p = ResidentialProxyProvider()
        assert p.proxies == []

    def test_refresh_proxies_is_noop_by_default(self) -> None:
        p = ResidentialProxyProvider.from_urls(["socks5://127.0.0.1:1080"])
        # Should not raise; is a no-op stub.
        p._refresh_proxies()
        assert len(p.all_proxies()) == 1
