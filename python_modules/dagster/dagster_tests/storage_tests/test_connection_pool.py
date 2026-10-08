from dagster._core.storage.runs.in_memory import InMemoryRunStorage


class _WebserverHookOnlyRunStorage(InMemoryRunStorage):
    """A storage that only implements the older `optimize_for_webserver` hook."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[int, int, int]] = []

    def optimize_for_webserver(
        self, statement_timeout: int, pool_recycle: int, max_overflow: int
    ) -> None:
        self.calls.append((statement_timeout, pool_recycle, max_overflow))


def test_enable_connection_pool_falls_back_to_webserver_hook() -> None:
    storage = _WebserverHookOnlyRunStorage()

    # the webserver passes a statement timeout and keeps the old behavior
    storage.enable_connection_pool(pool_recycle=60, max_overflow=5, statement_timeout=1000)
    assert storage.calls == [(1000, 60, 5)]

    # without a timeout (the daemon), such a storage keeps opening a connection per call
    storage.enable_connection_pool(pool_recycle=60, max_overflow=5)
    assert storage.calls == [(1000, 60, 5)]
