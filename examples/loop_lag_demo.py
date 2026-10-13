import asyncio
import time

from await_lens.core.event_loop import EventLoopLagMonitor


def blocking_operation() -> None:
    time.sleep(0.15)


async def main() -> None:
    async with EventLoopLagMonitor(interval_ms=10) as monitor:
        await asyncio.sleep(0.03)

        # Intentionally block the event loop.
        blocking_operation()

        await asyncio.sleep(0.03)

    print(f"Collected samples: {len(monitor.samples)}")

    if monitor.samples:
        peak = max(sample.lag_ms for sample in monitor.samples)
        print(f"Peak event loop lag: {peak:.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())
