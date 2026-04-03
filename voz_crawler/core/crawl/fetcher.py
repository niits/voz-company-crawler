import requests

_CF_BLOCK_MARKERS = ("Just a moment", "cf-browser-verification")
_CF_BLOCK_STATUSES = (403, 429, 503)


class Fetcher:
    """HTTP client for FlareSolverr. Reuses a requests.Session for keep-alive."""

    def __init__(self, flaresolverr_url: str, timeout: int = 60) -> None:
        self._flaresolverr_url = flaresolverr_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    def fetch(self, url: str) -> tuple[int, str]:
        """Fetch a URL through FlareSolverr. Returns (status_code, html)."""
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": self._timeout * 1000,
        }
        r = self._session.post(
            f"{self._flaresolverr_url}/v1",
            json=payload,
            timeout=self._timeout + 10,
        )
        r.raise_for_status()
        data = r.json()
        if data["status"] != "ok":
            raise RuntimeError(f"FlareSolverr error: {data.get('message', data)}")
        solution = data["solution"]
        return solution["status"], solution["response"]

    def is_cf_block(self, status_code: int, html: str) -> bool:
        if status_code in _CF_BLOCK_STATUSES:
            snippet = html[:2000]
            return any(m in snippet for m in _CF_BLOCK_MARKERS)
        return False
