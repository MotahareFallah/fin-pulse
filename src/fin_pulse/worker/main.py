"""Worker entry point: `python -m fin_pulse.worker.main`."""

import asyncio
import logging
import signal

from fin_pulse.worker.consumer import run


async def _main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await run(stop)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    asyncio.run(_main())


if __name__ == "__main__":
    main()
