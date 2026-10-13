import asyncio
import math
from collections import deque
from dataclasses import dataclass
from types import TracebackType


@dataclass(frozen=True, slots=True)
class LoopLagSample:
    scheduled_at: float
    observed_at: float
    lag_ms: float


class EventLoopLagMonitor:
    def __init__(
        self,
        *,
        interval_ms: float = 10.0,
        max_samples: int = 1000,
    ) -> None:
        if not math.isfinite(interval_ms) or interval_ms <= 0:
            raise ValueError("interval_ms must be finite and greater than zero")

        if max_samples <= 0:
            raise ValueError("max_samples must be greater than zero")

        self._interval_seconds = interval_ms / 1000.0
        self._samples: deque[LoopLagSample] = deque(maxlen=max_samples)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._handle: asyncio.TimerHandle | None = None

    @property
    def samples(self) -> tuple[LoopLagSample, ...]:
        return tuple(self._samples)

    async def __aenter__(self) -> "EventLoopLagMonitor":
        if self._loop is not None:
            raise RuntimeError("Monitor is already running")

        self._samples.clear()

        loop = asyncio.get_running_loop()
        self._loop = loop

        scheduled_at = loop.time() + self._interval_seconds
        self._handle = loop.call_at(
            scheduled_at,
            self._sample,
            scheduled_at,
        )

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None

        self._loop = None

    def _sample(self, scheduled_at: float) -> None:
        loop = self._loop

        if loop is None:
            return

        observed_at = loop.time()

        sample = LoopLagSample(
            scheduled_at=scheduled_at,
            observed_at=observed_at,
            lag_ms=max(
                0.0,
                (observed_at - scheduled_at) * 1000.0,
            ),
        )

        self._samples.append(sample)

        next_at = observed_at + self._interval_seconds
        self._handle = loop.call_at(
            next_at,
            self._sample,
            next_at,
        )
