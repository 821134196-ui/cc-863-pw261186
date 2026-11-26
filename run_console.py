#!/usr/bin/env python3
"""交互式操作台入口。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lease_sim.repl import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
