"""
Shared buffered CSV writer for all v2 scrapers.

Instead of open/write/close per row (3 syscalls × 50 matches = 150 syscalls
per poll cycle), this keeps file handles open and flushes them in batch.

Usage in scrapers::

    from scrapers.shared.csv_writer import MatchCSVWriter

    writer = MatchCSVWriter()
    writer.write_row("/path/to/file.csv", header, row)
    # ... on shutdown or periodically:
    writer.flush()
    writer.close_all()
"""

from __future__ import annotations

import csv
import os
import threading
from typing import Dict, List, Optional


class MatchCSVWriter:
    """Buffered CSV writer that keeps file handles open across writes.

    Each unique path gets one persistent file handle + csv.writer.
    Rows are written immediately but flushed in batch via flush().
    Handles are created lazily on first write and reused thereafter.

    Thread-safe via per-path locking.
    """

    def __init__(self) -> None:
        self._files: Dict[str, object] = {}   # path -> file handle
        self._writers: Dict[str, csv.writer] = {}  # path -> csv.writer
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def _get_lock(self, path: str) -> threading.Lock:
        with self._global_lock:
            if path not in self._locks:
                self._locks[path] = threading.Lock()
            return self._locks[path]

    def _open_if_needed(self, path: str, header: list) -> csv.writer:
        if path in self._writers:
            return self._writers[path]
        # Create directory if needed
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Write header only on first creation
        exists = os.path.exists(path)
        f = open(path, "a", newline="", buffering=1)  # line-buffered
        self._files[path] = f
        w = csv.writer(f)
        self._writers[path] = w
        if not exists or os.path.getsize(path) == 0:
            w.writerow(header)
            f.flush()
        return w

    def write_row(self, path: str, header: list, row: List[str]) -> None:
        """Write a row to the given CSV path. Creates file + header if needed."""
        lock = self._get_lock(path)
        with lock:
            w = self._open_if_needed(path, header)
            w.writerow(row)

    def flush(self) -> None:
        """Flush all open file handles to disk."""
        with self._global_lock:
            for f in self._files.values():
                try:
                    f.flush()
                except Exception:
                    pass

    def close_all(self) -> None:
        """Close all open file handles."""
        with self._global_lock:
            for f in self._files.values():
                try:
                    f.flush()
                    f.close()
                except Exception:
                    pass
            self._files.clear()
            self._writers.clear()
