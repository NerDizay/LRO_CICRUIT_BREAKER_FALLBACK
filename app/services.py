import time

import httpx

CB_CLOSED = True
CB_OPEN = False


class CircuitBreaker:
    def __init__(self, failure_threshold: int, open_timeout: int) -> None:
        self.failure_threshold = failure_threshold
        self.open_timeout = open_timeout
        self._failures = 0
        self._opened_at: float | None = None
        self._state: bool = CB_CLOSED

    @property
    def state(self) -> bool:
        if not self._state and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self.open_timeout:
                self._state = CB_CLOSED
                self._failures = 0
                self._opened_at = None
        return self._state

    def record_success(self) -> None:
        self._failures = 0
        self._state = CB_CLOSED
        self._opened_at = None

    def record_failure(self) -> bool:
        """Записывает ошибку. Возвращает True если CB перешёл в OPEN."""
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state = CB_OPEN
            self._opened_at = time.monotonic()
            return True
        return False


class CircuitBreakerOpen(Exception):
    def __init__(self) -> None:
        super().__init__('Circuit breaker is OPEN')


class LROClient:
    """Создаёт по экземпляру httpx.AsyncClient на каждого воркера."""

    def __init__(self, circuit_breaker: CircuitBreaker) -> None:
        self._client: httpx.AsyncClient | None = None
        self.circuit_breaker = circuit_breaker

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient()
        return self._client

    async def _request(self, url: str, json: dict, timeout: int = 15) -> dict:
        if self.circuit_breaker.state == CB_OPEN:
            raise CircuitBreakerOpen()
        try:
            client = self._ensure_client()
            resp = await client.post(url, json=json, timeout=timeout)
            resp.raise_for_status()
        except Exception:
            self.circuit_breaker.record_failure()
            raise
        self.circuit_breaker.record_success()
        return resp.json()

    async def method_one_async(self, payload: dict) -> dict:
        return await self._request('https://httpbin.org/post', payload)

    async def method_two_async(self, payload: dict) -> dict:
        return await self._request('https://httpbin.org/post', payload)

    async def method_fallback_async(self, payload: dict) -> dict:
        return {'fallback': 'fallback'}

    async def close(self):
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
