import asyncio
import time

import pytest

from await_lens.core.event_loop import EventLoopLagMonitor


def _block_for(seconds: float) -> None:
    time.sleep(seconds)


@pytest.mark.parametrize(
    "interval_ms",
    [0.0, -1.0, float("inf"), float("nan")],
)
def test_rejects_invalid_interval(interval_ms: float) -> None:
    with pytest.raises(
        ValueError,
        match="interval_ms must be finite and greater than zero",
    ):
        EventLoopLagMonitor(interval_ms=interval_ms)


def test_rejects_invalid_capacity() -> None:
    with pytest.raises(
        ValueError,
        match="max_samples must be greater than zero",
    ):
        EventLoopLagMonitor(max_samples=0)


@pytest.mark.asyncio
async def test_collects_samples() -> None:
    async with EventLoopLagMonitor(interval_ms=5) as monitor:
        await asyncio.sleep(0.04)

    assert monitor.samples
    assert all(sample.lag_ms >= 0 for sample in monitor.samples)


@pytest.mark.asyncio
async def test_detects_blocking_call() -> None:
    async with EventLoopLagMonitor(interval_ms=5) as monitor:
        await asyncio.sleep(0.02)
        _block_for(0.08)
        await asyncio.sleep(0.02)

    assert monitor.samples
    assert max(sample.lag_ms for sample in monitor.samples) >= 40


@pytest.mark.asyncio
async def test_samples_are_bounded() -> None:
    async with EventLoopLagMonitor(
        interval_ms=1,
        max_samples=3,
    ) as monitor:
        await asyncio.sleep(0.04)

    assert len(monitor.samples) == 3


@pytest.mark.asyncio
async def test_monitor_stops_after_context_manager() -> None:
    monitor = EventLoopLagMonitor(interval_ms=1)

    async with monitor:
        await asyncio.sleep(0.03)

    sample_count = len(monitor.samples)
    await asyncio.sleep(0.03)

    assert len(monitor.samples) == sample_count


@pytest.mark.asyncio
async def test_rejects_nested_start() -> None:
    monitor = EventLoopLagMonitor()

    async with monitor:
        with pytest.raises(RuntimeError, match="already running"):
            async with monitor:
                pass
