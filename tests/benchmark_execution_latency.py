"""Offline read-path benchmark. No config, credentials, DB, or exchange calls.

Run from the repository: python -m tests.benchmark_execution_latency
The comparator retains the pre-cache sorting/top-of-book implementation;
both variants use identical updates and return detached lists.
"""
import json
import statistics
import time

from entropy_arb.book import OrderBook


class UncachedBook(OrderBook):
    def sorted_bids(self):
        return sorted(self.bids.items(), key=lambda kv: -kv[0])

    def sorted_asks(self):
        return sorted(self.asks.items())

    def mid(self):
        return (max(self.bids) + min(self.asks)) / 2 if self.bids and self.asks else None


def benchmark(cls):
    samples = []
    levels = {"bids": [{"price": str(99 - i / 100), "size": "1"} for i in range(1000)],
              "asks": [{"price": str(100 + i / 100), "size": "1"} for i in range(1000)]}
    for _ in range(7):
        book = cls()
        book.apply_lighter(levels, snapshot=True)
        start = time.perf_counter()
        for i in range(100):
            book.apply_lighter({"bids": [{"price": "99", "size": str(i + 1)}]}, False)
            for _ in range(10):
                assert book.sorted_bids()[0] == (99, i + 1)
                assert book.sorted_asks()[0] == (100, 1)
                assert book.mid() == 99.5
        samples.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(samples), 3)


if __name__ == "__main__":
    old, new = benchmark(UncachedBook), benchmark(OrderBook)
    print(json.dumps({"workload": "100 updates, 1000 levels/side, 10 read passes/update",
                      "uncached_median_ms": old, "cached_median_ms": new,
                      "speedup": round(old / new, 2)}, indent=2))
