#!/usr/bin/env python3
"""演示运行器。

用法:
  python3 -m lease_sim.demo all                 跑全部场景
  python3 -m lease_sim.demo run pause-expiry    跑单个场景（带完整时间线）
  python3 -m lease_sim.demo list                列出场景
  python3 -m lease_sim.demo verify              只输出断言结论（CI 友好）
"""
from __future__ import annotations

import argparse
import sys

from .report import full_report, render_timeline, render_verification
from .scenarios import SCENARIOS


def _run_one(key: str, verbose: bool) -> tuple[bool, str]:
    if key not in SCENARIOS:
        return False, f"未知场景 {key}，可选: {', '.join(SCENARIOS)}"
    result = SCENARIOS[key].run()
    parts = [result.render()]
    if verbose:
        parts.extend(["", full_report(result.world)])
    return result.ok, "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lease_sim.demo", description="分布式租约栅栏模拟器"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="列出预置场景")

    p_run = sub.add_parser("run", help="运行单个场景")
    p_run.add_argument("scenario", choices=sorted(SCENARIOS))
    p_run.add_argument("-q", "--quiet", action="store_true",
                       help="只看断言，不打印完整审计时间线")

    p_all = sub.add_parser("all", help="运行全部场景")
    p_all.add_argument("-q", "--quiet", action="store_true")

    sub.add_parser("verify", help="CI 模式：只输出通过/失败汇总")

    args = parser.parse_args(argv)

    if args.cmd == "list":
        for key, mod in SCENARIOS.items():
            print(f"  {key:<14} {mod.TITLE}")
        return 0

    if args.cmd == "run":
        ok, text = _run_one(args.scenario, verbose=not args.quiet)
        print(text)
        return 0 if ok else 1

    if args.cmd == "all":
        all_ok = True
        for key in SCENARIOS:
            ok, text = _run_one(key, verbose=not args.quiet)
            print(text)
            print()
            all_ok = all_ok and ok
        print("=" * 78)
        print(f"全部场景: {'通过' if all_ok else '失败'}")
        return 0 if all_ok else 1

    if args.cmd == "verify":
        all_ok = True
        for key, mod in SCENARIOS.items():
            result = mod.run()
            mark = "PASS" if result.ok else "FAIL"
            print(f"[{mark}] {key:<14} "
                  f"{sum(a.passed for a in result.assertions)}/"
                  f"{len(result.assertions)} 断言通过")
            all_ok = all_ok and result.ok
        return 0 if all_ok else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
