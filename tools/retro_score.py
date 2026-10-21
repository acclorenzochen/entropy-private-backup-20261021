#!/usr/bin/env python3
"""Print a transparent retrospective-farming contribution projection."""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.retro_farming import (
    RetroAnalysisConfig,
    load_project_contribution,
    plan_retro_canary,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze project contribution under hypothetical score models")
    parser.add_argument("--db", default="logs/entropy_arb.sqlite3")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--since", type=float, default=None)
    parser.add_argument("--until", type=float, default=None)
    parser.add_argument("--growth-volume-multiplier", type=float, default=0.10)
    parser.add_argument("--expected-canary-loss-usd", type=float, default=0.005)
    parser.add_argument("--expected-canary-raw-volume-usd", type=float,
                        default=30.0)
    args = parser.parse_args()
    if not 0.0 <= args.growth_volume_multiplier <= 1.0:
        parser.error("--growth-volume-multiplier must be between 0 and 1")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    result = load_project_contribution(
        args.db,
        since=args.since,
        until=args.until,
        limit=args.limit,
        config=RetroAnalysisConfig(
            growth_volume_multiplier=args.growth_volume_multiplier),
    )
    result["shadow_canary_plan"] = plan_retro_canary(
        result,
        expected_loss_usd=args.expected_canary_loss_usd,
        expected_raw_volume_usd=args.expected_canary_raw_volume_usd,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
