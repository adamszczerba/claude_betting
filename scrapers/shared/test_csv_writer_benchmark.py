"""Benchmark: buffered vs unbuffered CSV writes."""

import csv
import os
import tempfile
import time

import pytest


def _unbuffered_write(path: str, header: list, rows: list) -> None:
    """Old approach: open/write/close per row."""
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(header)
    for row in rows:
        with open(path, "a", newline="") as f:
            csv.writer(f).writerow(row)


def _buffered_write(path: str, header: list, rows: list) -> None:
    """New approach: keep handle open, flush at end."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    exists = os.path.exists(path)
    f = open(path, "a", newline="", buffering=1)
    w = csv.writer(f)
    if not exists or os.path.getsize(path) == 0:
        w.writerow(header)
    for row in rows:
        w.writerow(row)
    f.flush()
    f.close()


class TestBufferedWriteBenchmark:
    """Quantify write buffering speedup."""

    def test_buffered_faster_for_many_rows(self, tmp_path):
        """Buffered writes should be significantly faster for 100+ rows."""
        header = ["ts", "match_time", "status", "h", "a", "o1", "oX", "o2", "tl", "oo", "ou"]
        rows = [[f"2026-01-01T00:00:{i:02d}.000", "45:00", "1H", "0", "0",
                 "1.50", "3.50", "6.00", "2.5", "1.80", "1.90"] for i in range(200)]

        # Unbuffered
        p1 = tmp_path / "unbuffered.csv"
        t0 = time.monotonic()
        _unbuffered_write(str(p1), header, rows)
        t_unbuffered = time.monotonic() - t0

        # Buffered
        p2 = tmp_path / "buffered.csv"
        t0 = time.monotonic()
        _buffered_write(str(p2), header, rows)
        t_buffered = time.monotonic() - t0

        # Verify correctness
        with open(p1) as f:
            r1 = list(csv.reader(f))
        with open(p2) as f:
            r2 = list(csv.reader(f))
        assert r1 == r2, "Buffered and unbuffered should produce identical output"

        # Buffered should be faster (typically 5-10x)
        print(f"\nUnbuffered: {t_unbuffered:.4f}s, Buffered: {t_buffered:.4f}s, "
              f"Speedup: {t_unbuffered/t_buffered:.1f}x")
        assert t_buffered < t_unbuffered, "Buffered should be faster than unbuffered"

    def test_buffered_matches_scraper_pattern(self, tmp_path):
        """Simulate the actual scraper pattern: 50 matches, 10 cycles each."""
        header = ["ts", "mt", "st", "hs", "as", "o1", "oX", "o2", "tl", "oo", "ou"]
        num_matches = 50
        num_cycles = 10

        # Create match file paths
        match_paths = [tmp_path / f"match_{i}.csv" for i in range(num_matches)]

        # Unbuffered: open/close per match per cycle
        t0 = time.monotonic()
        for cycle in range(num_cycles):
            for mp in match_paths:
                if cycle == 0:
                    if not os.path.exists(mp):
                        with open(mp, "w", newline="") as f:
                            csv.writer(f).writerow(header)
                row = [f"2026-01-01T00:00:{cycle:02d}.000", f"{cycle}:00", "1H",
                       "0", "0", "1.50", "3.50", "6.00", "2.5", "1.80", "1.90"]
                with open(mp, "a", newline="") as f:
                    csv.writer(f).writerow(row)
        t_unbuffered = time.monotonic() - t0

        # Clean up
        for mp in match_paths:
            mp.unlink(missing_ok=True)

        # Buffered: open once, write all cycles, close
        t0 = time.monotonic()
        handles = {}
        for mp in match_paths:
            f = open(mp, "a", newline="", buffering=1)
            w = csv.writer(f)
            if not os.path.exists(mp) or os.path.getsize(mp) == 0:
                w.writerow(header)
            handles[mp] = (f, w)

        for cycle in range(num_cycles):
            for mp in match_paths:
                f, w = handles[mp]
                row = [f"2026-01-01T00:00:{cycle:02d}.000", f"{cycle}:00", "1H",
                       "0", "0", "1.50", "3.50", "6.00", "2.5", "1.80", "1.90"]
                w.writerow(row)

        for f, _ in handles.values():
            f.flush()
            f.close()
        t_buffered = time.monotonic() - t0

        print(f"\nScraper pattern ({num_matches} matches × {num_cycles} cycles):")
        print(f"  Unbuffered: {t_unbuffered:.4f}s")
        print(f"  Buffered:   {t_buffered:.4f}s")
        print(f"  Speedup:    {t_unbuffered/t_buffered:.1f}x")
        assert t_buffered < t_unbuffered
