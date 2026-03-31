"""
Synchronized wall-clock tick for all scrapers.
================================================
Every scraper calls ``sleep_until_next_tick(interval)`` at the end of its
loop.  The function sleeps until the next wall-clock boundary that is a
multiple of *interval* seconds since the Unix epoch.

With a 2-second interval the ticks land on even UTC seconds:
    … 14:22:02.000, 14:22:04.000, 14:22:06.000, …

Because Docker containers inherit the host kernel clock, all containers
will fire at the same wall-clock instants (sub-ms skew) — no Redis,
no message queue, no extra infrastructure.

If a scrape cycle takes longer than *interval* the next tick has already
passed; the function returns immediately (sleep ≈ 0) and the scraper
self-corrects on the following cycle.
"""

import math
import time

__all__ = ["sleep_until_next_tick"]


def sleep_until_next_tick(interval: float = 2.0) -> float:
    """Sleep until the next wall-clock tick aligned to *interval*.

    Returns the tick time (Unix timestamp) that was waited for, so the
    caller can record it (e.g. as ``tick_time`` in CSVs for easy cross-
    bookmaker alignment).
    """
    now = time.time()
    next_tick = math.ceil(now / interval) * interval
    delay = next_tick - now
    if delay > 0:
        time.sleep(delay)
    return next_tick

