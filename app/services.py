import httpx


class LROClient:
    """Создаёт по экземпляру httpx.AsyncClient на каждого воркера."""

    def __init__(self):
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient()
        return self._client

    async def method_one_async(self, payload: dict) -> dict:
        client = self._ensure_client()
        resp = await client.post('https://httpbin.org/post', json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()

    async def method_two_async(self, payload: dict) -> dict:
        client = self._ensure_client()
        resp = await client.post('https://httpbin.org/post', json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
