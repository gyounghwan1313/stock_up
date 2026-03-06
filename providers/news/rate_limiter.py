import json
import os
import time
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, state_file: str | None = None):
        self._last_call: dict[str, float] = {}
        self._state_file = state_file
        if state_file:
            self._load_state()

    def _load_state(self) -> None:
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, "r") as f:
                    self._last_call = json.load(f)
                logger.info("Rate limiter state loaded from %s", self._state_file)
        except Exception as e:
            logger.warning("Failed to load rate limiter state: %s", e)

    def _save_state(self) -> None:
        if not self._state_file:
            return
        try:
            dir_path = os.path.dirname(os.path.abspath(self._state_file))
            os.makedirs(dir_path, exist_ok=True)
            with open(self._state_file, "w") as f:
                json.dump(self._last_call, f)
        except Exception as e:
            logger.warning("Failed to save rate limiter state: %s", e)

    def wait_if_needed(self, source: str, interval_seconds: int) -> None:
        """마지막 호출 기록 시각으로부터 interval_seconds가 경과할 때까지 대기.

        호출 시각 갱신은 mark_called()로 별도 수행해야 한다.
        """
        now = time.time()
        last = self._last_call.get(source, 0)
        elapsed = now - last
        if elapsed < interval_seconds:
            wait_time = interval_seconds - elapsed
            logger.info("Rate limit: waiting %.1fs for %s", wait_time, source)
            time.sleep(wait_time)

    def mark_called(self, source: str) -> None:
        """RSS 호출 완료 시점을 기록한다."""
        self._last_call[source] = time.time()
        self._save_state()

    def can_call(self, source: str, interval_seconds: int) -> bool:
        now = time.time()
        last = self._last_call.get(source, 0)
        return (now - last) >= interval_seconds
