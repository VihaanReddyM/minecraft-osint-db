from __future__ import annotations

import logging
from dataclasses import dataclass, field

from mcosint.proxy.pool import ProxyConfig

log = logging.getLogger(__name__)


@dataclass
class ResidentialProxyProvider:
    """Stub proxy provider for residential/datacenter proxy services.

    Implements the ProxyProvider protocol via round-robin assignment.
    Extend by overriding `_refresh_proxies()` to call a provider API.
    """

    proxies: list[ProxyConfig] = field(default_factory=list)
    _rotation_counter: int = field(default=0, init=False, repr=False)

    @classmethod
    def from_urls(cls, urls: list[str]) -> ResidentialProxyProvider:
        """Build a provider from a list of proxy URL strings."""
        return cls(proxies=[ProxyConfig.from_url(u) for u in urls])

    def get_proxy(self, worker_id: int) -> ProxyConfig | None:
        """Return a proxy for the given worker using round-robin assignment."""
        active = self._active_proxies()
        if not active:
            return None
        return active[worker_id % len(active)]

    def all_proxies(self) -> list[ProxyConfig]:
        """Return a copy of all active proxy configs."""
        return list(self._active_proxies())

    def _active_proxies(self) -> list[ProxyConfig]:
        return list(self.proxies)

    def _refresh_proxies(self) -> None:
        """Override to fetch fresh proxy list from a provider API."""
        log.debug("ResidentialProxyProvider: no refresh configured")
